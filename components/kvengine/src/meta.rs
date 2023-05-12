// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::max, collections::HashMap, iter::Iterator};

use api_version::api_v2::{is_whole_keyspace_range, KEYSPACE_PREFIX_LEN};
use bytes::{Buf, Bytes};
use kvenginepb as pb;
use protobuf::Message;
use slog_global::*;

use super::*;

#[derive(Default, Clone)]
pub struct ShardMeta {
    pub engine_id: u64,
    pub id: u64,
    pub ver: u64,
    pub range: ShardRange,
    // sequence is the raft log index of the applied change set.
    pub seq: u64,
    pub(crate) files: HashMap<u64, FileMeta>,

    pub(crate) properties: Properties,
    pub base_version: u64,
    // data_sequence is the raft log index of data included in the latest L0 file.
    pub data_sequence: u64,
    // max_ts is the max ts in all sst files.
    pub max_ts: u64,
    pub parent: Option<Box<ShardMeta>>,
}

impl ShardMeta {
    pub fn new(engine_id: u64, cs: &pb::ChangeSet) -> Self {
        assert!(cs.has_snapshot() || cs.has_initial_flush() || cs.has_restore_shard());
        let snap = if cs.has_snapshot() {
            cs.get_snapshot()
        } else if cs.has_initial_flush() {
            cs.get_initial_flush()
        } else if cs.has_restore_shard() {
            cs.get_restore_shard()
        } else {
            unreachable!();
        };
        let mut meta = Self {
            engine_id,
            id: cs.shard_id,
            ver: cs.shard_ver,
            range: ShardRange::from_snap(snap),
            seq: cs.sequence,
            properties: Properties::new().apply_pb(snap.get_properties()),
            base_version: snap.base_version,
            data_sequence: snap.data_sequence,
            max_ts: snap.max_ts,
            ..Default::default()
        };
        for l0 in snap.get_l0_creates() {
            meta.add_file(l0.id, -1, 0, l0.get_smallest(), l0.get_biggest());
        }
        for blob in snap.get_blob_creates() {
            meta.add_file(
                blob.id,
                -1,
                BLOB_LEVEL,
                blob.get_smallest(),
                blob.get_biggest(),
            );
        }
        for tbl in snap.get_table_creates() {
            meta.add_file(
                tbl.id,
                tbl.cf,
                tbl.level,
                tbl.get_smallest(),
                tbl.get_biggest(),
            );
        }
        if cs.has_parent() {
            let parent_meta = Box::new(Self::new(engine_id, cs.get_parent()));
            meta.parent = Some(parent_meta);
        }
        meta
    }

    pub fn tag(&self) -> ShardTag {
        ShardTag::new(self.engine_id, IdVer::new(self.id, self.ver))
    }

    pub fn new_split(
        id: u64,
        ver: u64,
        start: &[u8],
        end: &[u8],
        props: &pb::Properties,
        parent: Box<ShardMeta>,
        enable_inner_key_offset: bool,
    ) -> Self {
        let range = if enable_inner_key_offset && is_whole_keyspace_range(start, end) {
            ShardRange::new(start, end, KEYSPACE_PREFIX_LEN)
        } else {
            debug!(
                "engine new_split";
                "shard_id" => id,
                "start_key" => format!("{:?}", start),
                "end_key" => format!("{:?}", end),
                "inner_key_off" => parent.range.inner_key_off
            );
            ShardRange::new(start, end, parent.range.inner_key_off)
        };
        Self {
            id,
            ver,
            range,
            parent: Some(parent),
            properties: Properties::new().apply_pb(props),
            ..Default::default()
        }
    }

    fn move_down_file(&mut self, id: u64, cf: i32, level: u32) {
        let mut fm = self.files.get_mut(&id).unwrap();
        assert!(
            fm.get_level() + 1 == level,
            "fm.level {} level {}",
            fm.get_level(),
            level
        );
        assert!(fm.cf == cf as i8);
        fm.level = level as u8;
    }

    pub fn add_file(&mut self, id: u64, cf: i32, level: u32, smallest: &[u8], biggest: &[u8]) {
        self.files
            .insert(id, FileMeta::new(cf, level, smallest, biggest));
    }

    fn delete_file(&mut self, id: u64, level: u32) {
        if self.has_file_at_level(id, level) {
            self.files.remove(&id);
        }
    }

    fn has_file_at_level(&self, id: u64, level: u32) -> bool {
        self.file_level(id) == Some(level)
    }

    fn file_level(&self, id: u64) -> Option<u32> {
        self.files.get(&id).map(|fm| fm.get_level())
    }

    pub fn get_property(&self, key: &str) -> Option<Bytes> {
        self.properties.get(key)
    }

    pub fn set_property(&mut self, key: &str, value: &[u8]) {
        self.properties.set(key, value);
    }

    pub fn apply_change_set(&mut self, cs: &pb::ChangeSet) {
        if cs.sequence > 0 {
            self.seq = cs.sequence;
        }
        if cs.has_initial_flush() {
            self.apply_initial_flush(cs);
            return;
        }
        if cs.has_flush() {
            self.apply_flush(cs);
            return;
        }
        if cs.has_compaction() {
            self.apply_compaction(cs.get_compaction());
            return;
        }
        if cs.has_destroy_range() {
            self.apply_destroy_range(cs);
            return;
        }
        if cs.has_truncate_ts() {
            self.apply_truncate_ts(cs);
            return;
        }
        if cs.has_trim_over_bound() {
            self.apply_trim_over_bound(cs);
            return;
        }
        if cs.has_ingest_files() {
            self.apply_ingest_files(cs.get_ingest_files());
            return;
        }
        if cs.has_restore_shard() {
            self.apply_restore_shard(cs);
            return;
        }
        if cs.has_major_compaction() {
            self.apply_major_compaction(cs.get_major_compaction());
            return;
        }
        if !cs.get_property_key().is_empty() {
            if cs.get_property_merge() {
                // Now only DEL_PREFIXES_KEY is mergeable.
                assert_eq!(cs.get_property_key(), DEL_PREFIXES_KEY);
                let prefix = cs.get_property_value();
                let inner_key_off = self.range.inner_key_off;
                self.properties.set(
                    DEL_PREFIXES_KEY,
                    &self
                        .properties
                        .get(DEL_PREFIXES_KEY)
                        .map(|b| DeletePrefixes::unmarshal(b.chunk(), inner_key_off))
                        .unwrap_or_else(|| DeletePrefixes::new_with_inner_key_off(inner_key_off))
                        .merge(prefix)
                        .marshal(),
                );
            } else if cs.get_property_key() == TRUNCATE_TS_KEY {
                let truncate_ts = TruncateTs::unmarshal(cs.get_property_value());
                match self.get_property(TRUNCATE_TS_KEY) {
                    Some(v) => {
                        let cur_truncate_ts = if v.is_empty() {
                            None
                        } else {
                            Some(TruncateTs::unmarshal(v.chunk()))
                        };
                        if need_update_truncate_ts(cur_truncate_ts, truncate_ts) {
                            self.properties
                                .set(cs.get_property_key(), cs.get_property_value());
                        }
                    }
                    None => self
                        .properties
                        .set(cs.get_property_key(), cs.get_property_value()),
                };
            } else {
                self.properties
                    .set(cs.get_property_key(), cs.get_property_value());
            }
            return;
        }
        panic!("unexpected change set {:?}", cs)
    }

    fn is_duplicated_compaction(&self, comp: &mut pb::Compaction) -> bool {
        if is_move_down(comp) {
            if let Some(level) = self.file_level(comp.get_top_deletes()[0]) {
                if level == comp.level {
                    return false;
                }
            }
            info!(
                "{} skip duplicated move_down compaction level:{}",
                self.tag(),
                comp.level
            );
            return true;
        }
        for i in 0..comp.get_top_deletes().len() {
            let id = comp.get_top_deletes()[i];
            if self.is_compaction_file_deleted(id, comp.level, comp) {
                return true;
            }
        }
        for i in 0..comp.get_bottom_deletes().len() {
            let id = comp.get_bottom_deletes()[i];
            if self.is_compaction_file_deleted(id, comp.level + 1, comp) {
                return true;
            }
        }
        false
    }

    pub fn is_duplicated_major_compaction(&self, comp: &mut pb::MajorCompaction) -> bool {
        let ssts_already_deleted = comp
            .get_sstable_change()
            .get_table_deletes()
            .iter()
            .any(|sst_delete| !self.has_file_at_level(sst_delete.get_id(), sst_delete.get_level()));
        let blobs_already_deleted = comp
            .get_old_blob_tables()
            .iter()
            .any(|blob_tbl_delete| !self.has_file_at_level(*blob_tbl_delete, BLOB_LEVEL));
        if ssts_already_deleted || blobs_already_deleted {
            info!("{} skip duplicated major compaction {:?}", self.tag(), comp);
            // set compaction to be conflicted, so that newly created files will be GCed.
            comp.conflicted = true;
            return true;
        }
        false
    }

    pub fn is_duplicated_change_set(&self, cs: &mut pb::ChangeSet) -> bool {
        if cs.sequence > 0 && self.seq >= cs.sequence {
            info!("{} skip duplicated change {:?}", self.tag(), cs);
            return true;
        }
        if cs.has_initial_flush() && self.parent.is_none() {
            info!(
                "{} skip duplicated initial flush, already flushed",
                self.tag()
            );
            return true;
        }
        if cs.has_flush() {
            let flush = cs.get_flush();
            if flush.get_version() <= self.data_version() {
                info!(
                    "{} skip duplicated flush {:?} for old version",
                    self.tag(),
                    cs
                );
                return true;
            }
        }
        if cs.has_compaction() {
            let comp = cs.mut_compaction();
            if self.is_duplicated_compaction(comp) {
                return true;
            }
        }
        if cs.has_major_compaction() {
            let comp = cs.mut_major_compaction();
            if self.is_duplicated_major_compaction(comp) {
                return true;
            }
        }
        if cs.has_destroy_range() && self.is_duplicated_table_change(cs.get_destroy_range()) {
            info!("{} skip duplicated destroy range {:?}", self.tag(), cs);
            return true;
        }
        if cs.has_truncate_ts() && self.is_duplicated_table_change(cs.get_truncate_ts()) {
            info!("{} skip duplicated truncate ts {:?}", self.tag(), cs);
            return true;
        }
        if cs.has_trim_over_bound() && self.is_duplicated_table_change(cs.get_trim_over_bound()) {
            info!("{} skip duplicated trim_over_bound {:?}", self.tag(), cs);
            return true;
        }
        if cs.has_ingest_files() {
            let ingest_files = cs.get_ingest_files();
            let ingest_id =
                get_shard_property(INGEST_ID_KEY, ingest_files.get_properties()).unwrap();
            if let Some(old_ingest_id) = self.get_property(INGEST_ID_KEY) {
                if ingest_id.eq(&old_ingest_id) {
                    info!(
                        "{} skip duplicated ingest files, ingest_id:{:?}",
                        self.tag(),
                        ingest_id,
                    );
                    return true;
                }
            }
        }
        if cs.has_restore_shard() {
            // TODO: skip duplicated
        }
        false
    }

    fn is_compaction_file_deleted(&self, id: u64, level: u32, comp: &mut pb::Compaction) -> bool {
        if !self.has_file_at_level(id, level) {
            info!(
                "{} skip duplicated compaction file {} at level {}, already deleted.",
                self.tag(),
                id,
                level,
            );
            comp.conflicted = true;
            return true;
        }
        false
    }

    fn apply_flush(&mut self, cs: &pb::ChangeSet) {
        let flush = cs.get_flush();
        self.apply_properties(flush.get_properties());
        if flush.has_l0_create() {
            let l0 = flush.get_l0_create();
            self.add_file(l0.id, -1, 0, l0.get_smallest(), l0.get_biggest());
        }
        if flush.has_blob_create() {
            let blob = flush.get_blob_create();
            self.add_file(
                blob.id,
                -1,
                BLOB_LEVEL,
                blob.get_smallest(),
                blob.get_biggest(),
            );
        }
        let new_data_seq = flush.get_version() - self.base_version;
        if self.data_sequence < new_data_seq {
            debug!(
                "{} apply flush update data sequence from {} to {}, flush ver {} base {}",
                self.tag(),
                self.data_sequence,
                new_data_seq,
                flush.get_version(),
                self.base_version
            );
            self.data_sequence = new_data_seq;
        }
        self.max_ts = std::cmp::max(self.max_ts, flush.max_ts);
    }

    pub fn apply_initial_flush(&mut self, cs: &pb::ChangeSet) {
        let props = self.properties.clone();
        let mut new_meta = Self::new(self.engine_id, cs);
        new_meta.range = self.range.clone();
        new_meta.properties = props;
        // self.data_sequence may be advanced on raft log gc tick.
        new_meta.data_sequence = std::cmp::max(new_meta.data_sequence, self.data_sequence);
        *self = new_meta;
    }

    fn apply_compaction(&mut self, comp: &pb::Compaction) {
        if is_move_down(comp) {
            for tbl in comp.get_table_creates() {
                self.move_down_file(tbl.id, tbl.cf, tbl.level);
            }
            return;
        }
        for id in comp.get_top_deletes() {
            self.delete_file(*id, comp.level);
        }
        for id in comp.get_bottom_deletes() {
            self.delete_file(*id, comp.level + 1);
        }
        for tbl in comp.get_table_creates() {
            self.add_file(
                tbl.id,
                tbl.cf,
                tbl.level,
                tbl.get_smallest(),
                tbl.get_biggest(),
            )
        }
    }

    fn apply_major_compaction(&mut self, comp: &pb::MajorCompaction) {
        for delete in comp.get_sstable_change().get_table_deletes() {
            self.delete_file(delete.get_id(), delete.get_level());
        }
        for create in comp.get_sstable_change().get_table_creates() {
            self.add_file(
                create.id,
                create.cf,
                create.level,
                create.get_smallest(),
                create.get_biggest(),
            );
        }
        for delete in comp.get_old_blob_tables() {
            self.delete_file(*delete, BLOB_LEVEL);
        }
        for create in comp.get_new_blob_tables() {
            self.add_file(
                create.get_id(),
                -1,
                BLOB_LEVEL,
                create.get_smallest(),
                create.get_biggest(),
            );
        }
    }

    fn is_duplicated_table_change(&self, tc: &pb::TableChange) -> bool {
        tc.get_table_deletes()
            .iter()
            .any(|deleted| !self.has_file_at_level(deleted.get_id(), deleted.get_level()))
    }

    fn apply_table_change(&mut self, tc: &pb::TableChange) {
        for deleted in tc.get_table_deletes() {
            self.delete_file(deleted.get_id(), deleted.get_level());
        }
        for created in tc.get_table_creates() {
            self.add_file(
                created.id,
                created.cf,
                created.level,
                created.get_smallest(),
                created.get_biggest(),
            );
        }
    }

    fn apply_destroy_range(&mut self, cs: &pb::ChangeSet) {
        assert!(cs.has_destroy_range());
        self.apply_table_change(cs.get_destroy_range());
        // ChangeSet of DestroyRange contains the corresponding delete-prefixes which
        // should be cleaned up.
        assert_eq!(cs.get_property_key(), DEL_PREFIXES_KEY);
        if let Some(data) = self.properties.get(DEL_PREFIXES_KEY) {
            let old = DeletePrefixes::unmarshal(data.chunk(), self.range.inner_key_off);
            let done = DeletePrefixes::unmarshal(cs.get_property_value(), self.range.inner_key_off);
            self.properties
                .set(DEL_PREFIXES_KEY, &old.split(&done).marshal());
            info!("Destroy range in meta changed from {:?} to {:?}", old, done);
        }
    }

    fn apply_truncate_ts(&mut self, cs: &pb::ChangeSet) {
        debug!("apply truncate ts in meta {:?}", self.id);
        assert!(cs.has_truncate_ts());
        self.apply_table_change(cs.get_truncate_ts());

        // ChangeSet of TruncateTs contains the corresponding ts which should be cleaned
        // up.
        assert_eq!(cs.get_property_key(), TRUNCATE_TS_KEY);
        if self.get_property(TRUNCATE_TS_KEY).is_some() {
            let truncated_ts = TruncateTs::unmarshal(cs.get_property_value());
            let v = self.get_property(TRUNCATE_TS_KEY).unwrap();
            let cur_truncate_ts = if v.is_empty() {
                None
            } else {
                Some(TruncateTs::unmarshal(v.chunk()))
            };
            // if applied truncate_ts is smaller than truncate ts in Meta, remove it.
            if need_update_truncate_ts(cur_truncate_ts, truncated_ts) {
                self.set_property(TRUNCATE_TS_KEY, b"");
            }
            info!(
                "Request change truncate ts in meta from {:?} to {:?}",
                cur_truncate_ts, truncated_ts
            );
        }
    }

    fn apply_trim_over_bound(&mut self, cs: &pb::ChangeSet) {
        debug!("apply changeset.trim_over_bound in meta {:?}", self.id);
        assert!(cs.has_trim_over_bound());
        self.apply_table_change(cs.get_trim_over_bound());
        self.set_property(TRIM_OVER_BOUND, TRIM_OVER_BOUND_DISABLE);
    }

    fn apply_ingest_files(&mut self, ingest_files: &pb::IngestFiles) {
        self.apply_properties(ingest_files.get_properties());
        for tbl in ingest_files.get_table_creates() {
            self.add_file(tbl.id, 0, tbl.level, tbl.get_smallest(), tbl.get_biggest());
        }
        for l0_tbl in ingest_files.get_l0_creates() {
            self.add_file(
                l0_tbl.id,
                -1,
                0,
                l0_tbl.get_smallest(),
                l0_tbl.get_biggest(),
            );
        }
        for blob_tbl in ingest_files.get_blob_creates() {
            self.add_file(
                blob_tbl.id,
                -1,
                BLOB_LEVEL,
                blob_tbl.get_smallest(),
                blob_tbl.get_biggest(),
            );
        }
    }

    fn apply_properties(&mut self, props: &pb::Properties) {
        for i in 0..props.get_keys().len() {
            let key = &props.get_keys()[i];
            let val = &props.get_values()[i];
            self.properties.set(key, val.as_slice());
        }
    }

    fn apply_restore_shard(&mut self, cs: &pb::ChangeSet) {
        assert!(cs.has_restore_shard());
        assert_eq!(self.range.outer_start, cs.get_restore_shard().outer_start);
        assert_eq!(self.range.outer_end, cs.get_restore_shard().outer_end);
        info!(
            "{} apply_restore_shard in meta: current ver:{}, seq:{}, base_ver:{}, data_seq:{}",
            self.tag(),
            self.ver,
            self.seq,
            self.base_version,
            self.data_sequence
        );

        // Increase shard version to make change sets generated before restore shard
        // stale.
        let mut new_meta = Self::new(self.engine_id, cs);
        new_meta.data_sequence = cs.sequence;
        new_meta.ver = cs.shard_ver + 1;
        *self = new_meta;
        info!(
            "{} apply_restore_shard in meta: new ver:{}, seq:{}, base_ver:{}, data_seq:{}",
            self.tag(),
            self.ver,
            self.seq,
            self.base_version,
            self.data_sequence
        );
    }

    pub fn apply_split(
        &self,
        split: &kvenginepb::Split,
        sequence: u64,
        initial_seq: u64,
        enable_inner_key_offset: bool,
    ) -> Vec<ShardMeta> {
        let old = self;
        let new_shards_len = split.get_new_shards().len();
        let mut new_shards = Vec::with_capacity(new_shards_len);
        let new_ver = old.ver + new_shards_len as u64 - 1;
        for i in 0..new_shards_len {
            let (start_key, end_key) = get_splitting_start_end(
                &old.range.outer_start,
                &old.range.outer_end,
                split.get_keys(),
                i,
            );
            let new_shard = &split.get_new_shards()[i];
            let id = new_shard.get_shard_id();
            let mut meta = ShardMeta::new_split(
                id,
                new_ver,
                start_key,
                end_key,
                new_shard,
                Box::new(old.clone()),
                enable_inner_key_offset,
            );
            meta.engine_id = self.engine_id;
            if id == old.id {
                meta.base_version = old.base_version;
                meta.data_sequence = sequence;
                meta.seq = sequence;
            } else {
                debug!(
                    "new base for {}, {} {}",
                    meta.tag(),
                    old.base_version,
                    sequence
                );
                meta.base_version = old.base_version + sequence;
                meta.data_sequence = initial_seq;
                meta.seq = initial_seq;
            }
            new_shards.push(meta);
        }
        for new_shard in &mut new_shards {
            for (fid, fm) in &old.files {
                if new_shard.overlap_table(&fm.smallest, &fm.biggest) {
                    new_shard.files.insert(*fid, fm.clone());
                }
            }
        }
        new_shards
    }

    pub fn to_change_set(&self) -> pb::ChangeSet {
        let mut cs = new_change_set(self.id, self.ver);
        cs.set_sequence(self.seq);
        let mut snap = pb::Snapshot::new();
        snap.set_outer_start(self.range.outer_start.to_vec());
        snap.set_outer_end(self.range.outer_end.to_vec());
        snap.set_inner_key_off(self.range.inner_key_off as u32);
        snap.set_properties(self.properties.to_pb(self.id));
        snap.set_base_version(self.base_version);
        snap.set_data_sequence(self.data_sequence);
        snap.set_max_ts(self.max_ts);
        for (k, v) in self.files.iter() {
            if v.is_blob_file() {
                let mut blob = pb::BlobCreate::new();
                blob.set_id(*k);
                blob.set_smallest(v.smallest.to_vec());
                blob.set_biggest(v.biggest.to_vec());
                snap.mut_blob_creates().push(blob);
            } else if v.get_level() == 0 {
                let mut l0 = pb::L0Create::new();
                l0.set_id(*k);
                l0.set_smallest(v.smallest.to_vec());
                l0.set_biggest(v.biggest.to_vec());
                snap.mut_l0_creates().push(l0);
            } else {
                let mut tbl = pb::TableCreate::new();
                tbl.set_id(*k);
                tbl.set_cf(v.cf as i32);
                tbl.set_level(v.get_level());
                tbl.set_smallest(v.smallest.to_vec());
                tbl.set_biggest(v.biggest.to_vec());
                snap.mut_table_creates().push(tbl);
            }
        }
        cs.set_snapshot(snap);
        if let Some(parent) = &self.parent {
            cs.set_parent(parent.to_change_set());
        }
        cs
    }

    pub fn marshal(&self) -> Vec<u8> {
        let cs = self.to_change_set();
        cs.write_to_bytes().unwrap()
    }

    pub fn all_file_keys(&self) -> Vec<u64> {
        let mut ids = Vec::with_capacity(self.files.len());
        for k in self.files.keys() {
            ids.push(*k);
        }
        ids
    }

    pub fn all_files(&self) -> &HashMap<u64, FileMeta> {
        &self.files
    }

    pub fn overlap_table(&self, smallest: &[u8], biggest: &[u8]) -> bool {
        // [start-----smallest-----biggest-----end)
        // smallest-----[start-----biggest-----end)
        // [start-----smallest-----end)-----biggest
        // smallest-----[start-----end)-----biggest
        self.range.inner_start() <= biggest && smallest < self.range.inner_end()
    }

    pub(crate) fn get_blob_files(&self) -> Vec<(u64, Vec<u8>, Vec<u8>)> {
        let mut blob_files = vec![];
        for (id, file) in &self.files {
            if !file.is_blob_file() {
                continue;
            }
            blob_files.push((*id, file.smallest.to_vec(), file.biggest.to_vec()));
        }
        blob_files
    }

    pub(crate) fn entirely_over_bound_table(&self, smallest: &[u8], biggest: &[u8]) -> bool {
        // smallest-----biggest-----[start----------end)
        // [start----------end)-----smallest-----biggest
        !self.overlap_table(smallest, biggest)
    }

    pub(crate) fn partially_over_bound_table(&self, smallest: &[u8], biggest: &[u8]) -> bool {
        // smallest-----[start-----end)-----biggest
        // smallest-----[start-----biggest-----end)
        // [start-----smallest-----end)-----biggest
        self.overlap_table(smallest, biggest)
            && (smallest < self.range.inner_start() || self.range.inner_end() <= biggest)
    }

    // the ingest level never skip to lower level if upper level file exists, this
    // may not be optimal but prevent compaction generate conflicting files.
    // It find the top most existing file's level as ingest level, if there is
    // overlap with existing files, it will use the one level upper.
    pub(crate) fn get_ingest_level(&self, smallest: &[u8], biggest: &[u8]) -> u32 {
        // find the top most level as ingest level.
        let mut ingest_level = self
            .files
            .values()
            .filter(|f| !f.is_blob_file())
            .map(|f| f.level)
            .min()
            .unwrap_or(3);
        if ingest_level == 0 {
            return 0;
        }
        let overlap = self
            .files
            .values()
            .filter(|f| f.level == ingest_level && f.cf == 0)
            .any(|file| file.smallest.chunk() <= biggest && smallest <= file.biggest.chunk());
        if overlap {
            ingest_level -= 1;
        }
        ingest_level as u32
    }

    pub(crate) fn data_version(&self) -> u64 {
        self.base_version + self.data_sequence
    }

    pub fn prepare_merge(&mut self, sequence: u64) {
        let parent = self.clone();
        self.ver += 1;
        self.seq = sequence;
        self.parent = Some(Box::new(parent));
    }

    pub fn rollback_merge(&mut self, sequence: u64) {
        self.ver += 1;
        self.seq = sequence;
    }

    pub fn commit_merge(&mut self, source: &ShardMeta, sequence: u64) {
        let mut parent = self.clone();
        // Include the source files in the parent for future initial flush.
        for (&id, source_file) in &source.files {
            parent.files.insert(id, source_file.clone());
        }
        for (&id, source_file) in &source.files {
            self.files.insert(id, source_file.clone());
        }

        debug_assert_eq!(self.range.inner_key_off, source.range.inner_key_off);
        if self.range.outer_end == source.range.outer_start {
            self.range.outer_end = source.range.outer_end.clone();
        } else {
            self.range.outer_start = source.range.outer_start.clone();
        }
        self.ver = max(self.ver, source.ver) + 1;
        let source_mem_tbl_version = source.base_version + source.seq;
        let target_mem_tbl_version = self.base_version + sequence;
        self.base_version = max(source_mem_tbl_version, target_mem_tbl_version) - sequence;
        self.parent = Some(Box::new(parent));
        self.seq = sequence;
    }
}

#[derive(Clone, Debug)]
pub struct FileMeta {
    pub cf: i8,
    pub level: u8,
    pub smallest: Bytes,
    pub biggest: Bytes,
}

impl FileMeta {
    fn new(cf: i32, level: u32, smallest: &[u8], biggest: &[u8]) -> Self {
        let meta_level = if is_blob_file(level) {
            1u8 << 7
        } else {
            level as u8
        };
        Self {
            cf: cf as i8,
            level: meta_level,
            smallest: Bytes::copy_from_slice(smallest),
            biggest: Bytes::copy_from_slice(biggest),
        }
    }

    fn is_blob_file(&self) -> bool {
        (self.level & (1u8 << 7)) > 0
    }

    pub(crate) fn get_level(&self) -> u32 {
        if self.is_blob_file() {
            BLOB_LEVEL
        } else {
            self.level as u32
        }
    }
}

pub fn is_move_down(comp: &pb::Compaction) -> bool {
    comp.top_deletes.len() == comp.table_creates.len()
        && comp.top_deletes[0] == comp.table_creates[0].id
}

pub fn new_change_set(shard_id: u64, shard_ver: u64) -> pb::ChangeSet {
    let mut cs = pb::ChangeSet::new();
    cs.set_shard_id(shard_id);
    cs.set_shard_ver(shard_ver);
    cs
}

pub const GLOBAL_SHARD_END_KEY: &[u8] = &[255, 255, 255, 255, 255, 255, 255, 255];

trait MetaReader {
    fn iterate_meta<F>(&self, f: F) -> Result<()>
    where
        F: Fn(&pb::ChangeSet) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use std::iter::{FromIterator, Iterator};

    use super::*;

    #[test]
    fn test_ingest_level() {
        for level in 0..=3 {
            let mut cs = new_change_set(1, 1);
            let snap = cs.mut_snapshot();
            snap.set_outer_end(GLOBAL_SHARD_END_KEY.to_vec());
            snap.set_max_ts(100);
            let mut id = 0;
            let mut make_table = |level: u32, smallest: &str, biggest: &str| {
                id += 1;
                let mut tbl = pb::TableCreate::new();
                tbl.id = id;
                tbl.level = level;
                tbl.smallest = smallest.as_bytes().to_vec();
                tbl.biggest = biggest.as_bytes().to_vec();
                tbl
            };
            let tables = snap.mut_table_creates();
            tables.push(make_table(level, "1000", "2000"));
            let meta = ShardMeta::new(1, &cs);
            assert_eq!(meta.max_ts, 100);
            let assert_get_ingest_level = |smallest: &str, biggest: &str, level| {
                assert_eq!(
                    meta.get_ingest_level(smallest.as_bytes(), biggest.as_bytes()),
                    level
                );
            };
            let overlap_level = level.saturating_sub(1);
            assert_get_ingest_level("0000", "0999", level);
            assert_get_ingest_level("1000", "1500", overlap_level);
            assert_get_ingest_level("1000", "2000", overlap_level);
            assert_get_ingest_level("1500", "2000", overlap_level);
            assert_get_ingest_level("1500", "2999", overlap_level);
            assert_get_ingest_level("2000", "2999", overlap_level);
            assert_get_ingest_level("2500", "2999", level);
        }
    }

    // See issue #572
    // https://github.com/tidbcloud/cloud-storage-engine/issues/572
    #[test]
    fn test_delete_file_with_level() {
        let files = vec![
            // L0:
            (1, FileMeta::new(0, 0, b"", b"")),
            // L1:
            (101, FileMeta::new(0, 1, b"", b"")),
            (102, FileMeta::new(0, 1, b"", b"")),
            // L2:
            (201, FileMeta::new(0, 2, b"", b"")),
            (202, FileMeta::new(0, 2, b"", b"")),
        ];

        // comp_level, top_deletes, bottom_deletes, is_duplicated, result_files
        struct Case(u32, &'static [u64], &'static [u64], bool, &'static [u64]);
        let cases: Vec<Case> = vec![
            // delete top
            Case(0, &[1], &[], false, &[101, 102, 201, 202]),
            Case(1, &[101], &[], false, &[1, 102, 201, 202]),
            // delete bottom
            Case(0, &[], &[102], false, &[1, 101, 201, 202]),
            Case(1, &[], &[201, 202], false, &[1, 101, 102]),
            // delete both top & bottom
            Case(1, &[102], &[202], false, &[1, 101, 201]),
            // top deleted
            Case(0, &[888], &[], true, &[1, 101, 102, 201, 202]),
            Case(1, &[888], &[], true, &[1, 101, 102, 201, 202]),
            // bottom deleted
            Case(1, &[], &[888], true, &[1, 101, 102, 201, 202]),
            // bottom level not match
            Case(1, &[], &[101], true, &[1, 101, 102, 201, 202]),
            // top level not match
            Case(1, &[201], &[202], true, &[1, 101, 102, 201]),
        ];

        for (idx, Case(comp_level, top_deletes, bottom_deletes, is_duplicated, result_files)) in
            cases.into_iter().enumerate()
        {
            let meta = ShardMeta {
                files: HashMap::from_iter(files.clone().into_iter()),
                ..Default::default()
            };

            // Test compaction.
            {
                let mut meta = meta.clone();
                let mut comp = pb::Compaction::default();
                comp.set_level(comp_level);
                comp.mut_top_deletes().extend_from_slice(top_deletes);
                comp.mut_bottom_deletes().extend_from_slice(bottom_deletes);
                comp.mut_table_creates().push(pb::TableCreate {
                    id: 100000,
                    level: comp_level + 1,
                    ..Default::default()
                });

                assert_eq!(
                    meta.is_duplicated_compaction(&mut comp),
                    is_duplicated,
                    "case {}",
                    idx
                );

                meta.apply_compaction(&comp);
                assert_eq!(result_files.len() + 1, meta.files.len());
                for id in result_files {
                    assert!(meta.files.contains_key(id), "case {} file id {}", idx, id);
                }
            }

            // Test other table changes.
            {
                let mut table_change = pb::TableChange::default();
                let table_deletes = table_change.mut_table_deletes();
                for &id in top_deletes {
                    let deleted = pb::TableDelete {
                        id,
                        level: comp_level,
                        ..Default::default()
                    };
                    table_deletes.push(deleted);
                }
                for &id in bottom_deletes {
                    let deleted = pb::TableDelete {
                        id,
                        level: comp_level + 1,
                        ..Default::default()
                    };
                    table_deletes.push(deleted);
                }

                let changesets = vec![
                    {
                        // destroy_range
                        let mut cs = pb::ChangeSet::default();
                        cs.set_destroy_range(table_change.clone());
                        cs.set_property_key(DEL_PREFIXES_KEY.to_string());
                        cs
                    },
                    {
                        // truncate_ts
                        let mut cs = pb::ChangeSet::default();
                        cs.set_truncate_ts(table_change.clone());
                        cs.set_property_key(TRUNCATE_TS_KEY.to_string());
                        cs
                    },
                    {
                        // trim_over_bound
                        let mut cs = pb::ChangeSet::default();
                        cs.set_trim_over_bound(table_change.clone());
                        cs
                    },
                ];

                for mut cs in changesets {
                    let mut meta = meta.clone();
                    assert_eq!(
                        meta.is_duplicated_change_set(&mut cs),
                        is_duplicated,
                        "case {} cs {:?}",
                        idx,
                        cs,
                    );

                    meta.apply_change_set(&cs);
                    assert_eq!(result_files.len(), meta.files.len());
                    for id in result_files {
                        assert!(
                            meta.files.contains_key(id),
                            "case {} cs {:?} file id {}",
                            idx,
                            cs,
                            id
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_table_overlap() {
        test_table_overlap_helper(false);
        test_table_overlap_helper(true);
    }

    fn test_table_overlap_helper(enable_inner_key_off: bool) {
        let range = if enable_inner_key_off {
            ShardRange::new(&[1, 2, 3, 4, 10], &[1, 2, 3, 4, 20], 4)
        } else {
            ShardRange::new(&[10], &[20], 0)
        };
        let meta = ShardMeta {
            range,
            ..Default::default()
        };

        let cases: Vec<(u8, u8, bool, bool, bool)> = vec![
            // smallest, biggest, overlap, entirely_over_bound, partially_over_bound
            (3, 5, false, true, false),
            (3, 10, true, false, true),
            (3, 15, true, false, true),
            (3, 20, true, false, true),
            (3, 25, true, false, true),
            (10, 15, true, false, false),
            (10, 20, true, false, true),
            (10, 25, true, false, true),
            (13, 15, true, false, false),
            (13, 20, true, false, true),
            (13, 25, true, false, true),
            (20, 25, false, true, false),
            (23, 25, false, true, false),
        ];

        for (idx, (smallest, biggest, overlap, entirely_over_bound, partially_over_bound)) in
            cases.into_iter().enumerate()
        {
            let smallest = [smallest];
            let biggest = [biggest];

            assert_eq!(
                meta.overlap_table(&smallest, &biggest),
                overlap,
                "case {}",
                idx
            );
            assert_eq!(
                meta.entirely_over_bound_table(&smallest, &biggest),
                entirely_over_bound,
                "case {}",
                idx
            );
            assert_eq!(
                meta.partially_over_bound_table(&smallest, &biggest),
                partially_over_bound,
                "case {}",
                idx
            );
        }
    }
}
