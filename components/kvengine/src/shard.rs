// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    iter::Iterator,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::*, *},
        Arc, RwLock,
    },
};

use bytes::{Buf, BufMut, Bytes};
use dashmap::DashMap;
use kvenginepb as pb;
use slog_global::*;
use tikv_util::codec::number::U64_SIZE;

use crate::{
    table::{
        self,
        memtable::{self, CFTable},
        search,
        sstable::{L0Table, SSTable},
    },
    Iterator as TableIterator, *,
};

pub struct Shard {
    pub engine_id: u64,
    pub id: u64,
    pub ver: u64,
    pub start: Bytes,
    pub end: Bytes,
    pub parent_id: u64,
    pub(crate) data: RwLock<ShardData>,
    pub(crate) opt: Arc<Options>,

    // If the shard is not active, flush mem table and do compaction will ignore this shard.
    pub(crate) active: AtomicBool,

    pub(crate) properties: Properties,
    pub(crate) compacting: AtomicBool,
    pub(crate) initial_flushed: AtomicBool,

    pub(crate) base_version: u64,

    pub(crate) estimated_size: AtomicU64,
    pub(crate) estimated_entries: AtomicU64,
    pub(crate) max_ts: AtomicU64,
    pub(crate) estimated_kv_size: AtomicU64,

    // meta_seq is the raft log index of the applied change set.
    // Because change set are applied in the worker thread, the value is usually smaller
    // than write_sequence.
    pub(crate) meta_seq: AtomicU64,

    // write_sequence is the raft log index of the applied write batch.
    pub(crate) write_sequence: AtomicU64,

    pub(crate) compaction_priority: RwLock<Option<CompactionPriority>>,

    pub(crate) parent_snap: RwLock<Option<pb::Snapshot>>,
}

pub const INGEST_ID_KEY: &str = "_ingest_id";
pub const DEL_PREFIXES_KEY: &str = "_del_prefixes";
pub const TRUNCATE_TS_KEY: &str = "_truncate_ts";

pub const TRIM_OVER_BOUND: &str = "_trim_over_bound";
pub const TRIM_OVER_BOUND_ENABLE: &[u8] = &[1];
pub const TRIM_OVER_BOUND_DISABLE: &[u8] = b"";

impl Shard {
    pub fn new(
        engine_id: u64,
        props: &pb::Properties,
        ver: u64,
        start: &[u8],
        end: &[u8],
        opt: Arc<Options>,
    ) -> Self {
        let start = Bytes::copy_from_slice(start);
        let end = Bytes::copy_from_slice(end);
        let shard = Self {
            engine_id,
            id: props.shard_id,
            ver,
            start: start.clone(),
            end: end.clone(),
            parent_id: 0,
            data: RwLock::new(ShardData::new_empty(start, end)),
            opt,
            active: Default::default(),
            properties: Properties::new().apply_pb(props),
            compacting: Default::default(),
            initial_flushed: Default::default(),
            base_version: Default::default(),
            estimated_size: Default::default(),
            estimated_entries: Default::default(),
            max_ts: Default::default(),
            estimated_kv_size: Default::default(),
            meta_seq: Default::default(),
            write_sequence: Default::default(),
            compaction_priority: RwLock::new(None),
            parent_snap: RwLock::new(None),
        };
        if let Some(val) = get_shard_property(DEL_PREFIXES_KEY, props) {
            shard.set_del_prefix(&val);
        }
        if let Some(val) = get_shard_property(TRUNCATE_TS_KEY, props) {
            // when load shard from Meta, the value maybe empty.
            if !val.is_empty() {
                shard.set_truncate_ts(&val);
            }
        }
        if let Some(val) = get_shard_property(TRIM_OVER_BOUND, props) {
            if !val.is_empty() {
                shard.set_trim_over_bound(&val);
            }
        }
        shard
    }

    pub fn new_for_ingest(engine_id: u64, cs: &pb::ChangeSet, opt: Arc<Options>) -> Self {
        let snap = cs.get_snapshot();
        let mut shard = Self::new(
            engine_id,
            snap.get_properties(),
            cs.shard_ver,
            snap.start.as_slice(),
            snap.end.as_slice(),
            opt,
        );
        if !cs.has_parent() {
            store_bool(&shard.initial_flushed, true);
        } else {
            shard.parent_id = cs.get_parent().shard_id;
            let mut guard = shard.parent_snap.write().unwrap();
            *guard = Some(cs.get_parent().get_snapshot().clone());
        }
        shard.base_version = snap.base_version;
        shard.meta_seq.store(cs.sequence, Release);
        shard.write_sequence.store(snap.data_sequence, Release);
        info!(
            "ingest shard {} mem_table_version {}, change {:?}",
            shard.tag(),
            shard.load_mem_table_version(),
            &cs,
        );
        shard
    }

    pub(crate) fn set_active(&self, active: bool) {
        self.active.store(active, Release);
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Acquire)
    }

    pub(crate) fn refresh_states(&self) {
        self.refresh_estimated_size_and_entries();
        self.refresh_compaction_priority();
    }

    fn refresh_estimated_size_and_entries(&self) {
        let data = self.get_data();
        let mut max_ts = data.get_mem_table_max_ts();

        let (mut size, mut entries, mut kv_size, l0_max_ts) = data.get_l0_stats();
        max_ts = cmp::max(max_ts, l0_max_ts);

        data.for_each_level(|cf, l| {
            let (lv_size, lv_entries, lv_kv_size, lv_max_ts) = data.get_level_stats(l);
            size += lv_size;
            max_ts = max_ts_by_cf(max_ts, cf, lv_max_ts);
            entries += lv_entries;
            if cf == WRITE_CF {
                kv_size += lv_kv_size;
            }
            false
        });
        store_u64(&self.estimated_size, size);
        store_u64(&self.estimated_entries, entries);
        store_u64(&self.max_ts, max_ts);
        store_u64(&self.estimated_kv_size, kv_size);
    }

    pub(crate) fn set_del_prefix(&self, val: &[u8]) {
        let data = self.get_data();
        let new_data = ShardData::new(
            data.start.clone(),
            data.end.clone(),
            DeletePrefixes::unmarshal(val),
            data.truncate_ts,
            data.trim_over_bound,
            data.mem_tbls.clone(),
            data.l0_tbls.clone(),
            data.cfs.clone(),
        );
        self.set_data(new_data);
    }

    pub(crate) fn merge_del_prefix(&self, prefix: &[u8]) {
        let data = self.get_data();
        let new_data = ShardData::new(
            data.start.clone(),
            data.end.clone(),
            data.del_prefixes.merge(prefix),
            data.truncate_ts,
            data.trim_over_bound,
            data.mem_tbls.clone(),
            data.l0_tbls.clone(),
            data.cfs.clone(),
        );
        self.set_data(new_data);
    }

    pub(crate) fn set_truncate_ts(&self, val: &[u8]) -> bool {
        let truncate_ts = TruncateTs::unmarshal(val);
        let data = self.get_data();
        if let Some(curr_truncate_ts) = data.truncate_ts {
            if curr_truncate_ts <= truncate_ts {
                warn!("ignore another PiTR before the current one has completed";
                    "current truncated_ts" => ?curr_truncate_ts, "incoming truncated_ts" => ?truncate_ts,
                    "shard" => ?self.tag());
                return false;
            } else {
                warn!("overwrite truncate_ts";
                    "current truncated_ts" => ?curr_truncate_ts, "incoming truncated_ts" => ?truncate_ts,
                    "shard" => ?self.tag());
            }
        }

        let new_data = ShardData::new(
            data.start.clone(),
            data.end.clone(),
            data.del_prefixes.clone(),
            Some(truncate_ts),
            data.trim_over_bound,
            data.mem_tbls.clone(),
            data.l0_tbls.clone(),
            data.cfs.clone(),
        );
        self.set_data(new_data);

        info!("ready to truncate ts"; "truncate_ts" => ?truncate_ts, "shard" => ?self.tag());
        true
    }

    pub(crate) fn set_trim_over_bound(&self, val: &[u8]) {
        let trim_over_bound = !val.is_empty();

        let data = self.get_data();
        let new_data = ShardData::new(
            data.start.clone(),
            data.end.clone(),
            data.del_prefixes.clone(),
            data.truncate_ts,
            trim_over_bound,
            data.mem_tbls.clone(),
            data.l0_tbls.clone(),
            data.cfs.clone(),
        );
        self.set_data(new_data);

        if trim_over_bound {
            info!("{} ready to trim_over_bound", self.tag());
        }
    }

    pub fn get_suggest_split_key(&self) -> Option<Bytes> {
        let data = self.get_data();
        let max_level = data
            .get_cf(0)
            .levels
            .iter()
            .max_by_key(|level| level.tables.len())?;
        if max_level.tables.len() == 0 {
            return None;
        }
        if max_level.tables.len() == 1 {
            return max_level.tables[0].get_suggest_split_key();
        }
        let tbl_idx = max_level.tables.len() * 2 / 3;
        Some(Bytes::copy_from_slice(max_level.tables[tbl_idx].smallest()))
    }

    pub fn get_evenly_split_keys(&self, count: usize) -> Option<Vec<Bytes>> {
        if count <= 1 {
            return None;
        }
        let data = self.get_data();
        let max_level = data
            .get_cf(0)
            .levels
            .iter()
            .max_by_key(|level| level.tables.len())?;
        if max_level.tables.len() <= 1 {
            return None;
        }
        let step = (max_level.tables.len() / count).max(1);
        Some(
            max_level
                .tables
                .iter()
                .step_by(step)
                .skip(1)
                .filter_map(|tbl| {
                    (self.overlap_key(tbl.smallest()))
                        .then(|| Bytes::copy_from_slice(tbl.smallest()))
                })
                .collect(),
        )
    }

    pub fn overlap_table(&self, smallest: &[u8], biggest: &[u8]) -> bool {
        self.start <= biggest && smallest < self.end
    }

    pub fn cover_full_table(&self, smallest: &[u8], biggest: &[u8]) -> bool {
        self.start <= smallest && biggest < self.end
    }

    pub fn overlap_key(&self, key: &[u8]) -> bool {
        self.start <= key && key < self.end
    }

    pub fn get_property(&self, key: &str) -> Option<Bytes> {
        self.properties.get(key)
    }

    pub fn set_property(&self, key: &str, val: &[u8]) {
        self.properties.set(key, val);
    }

    pub(crate) fn load_mem_table_version(&self) -> u64 {
        self.base_version + self.write_sequence.load(Acquire)
    }

    pub fn get_all_files(&self) -> Vec<u64> {
        let data = self.get_data();
        data.get_all_files()
    }

    pub fn split_mem_tables(&self, parent_mem_tbls: &[CFTable]) -> Vec<CFTable> {
        let mut new_mem_tbls = vec![CFTable::new()];
        for old_mem_tbl in parent_mem_tbls {
            if old_mem_tbl.has_data_in_range(self.start.chunk(), self.end.chunk()) {
                new_mem_tbls.push(old_mem_tbl.new_split());
            }
        }
        new_mem_tbls
    }

    pub fn add_mem_table(&self, mem_tbl: CFTable) {
        let old_data = self.get_data();
        let mut new_mem_tbls = Vec::with_capacity(old_data.mem_tbls.len());
        new_mem_tbls.push(mem_tbl);
        new_mem_tbls.extend_from_slice(old_data.mem_tbls.as_slice());
        let new_data = ShardData::new(
            self.start.clone(),
            self.end.clone(),
            old_data.del_prefixes.clone(),
            old_data.truncate_ts,
            old_data.trim_over_bound,
            new_mem_tbls,
            old_data.l0_tbls.clone(),
            old_data.cfs.clone(),
        );
        self.set_data(new_data);
    }

    pub fn get_write_sequence(&self) -> u64 {
        self.write_sequence.load(Ordering::Acquire)
    }

    pub fn get_meta_sequence(&self) -> u64 {
        self.meta_seq.load(Ordering::Acquire)
    }

    pub fn get_estimated_size(&self) -> u64 {
        self.estimated_size.load(Ordering::Relaxed)
    }

    pub fn get_estimated_entries(&self) -> u64 {
        self.estimated_entries.load(Ordering::Relaxed)
    }

    // Note: `get_max_ts` would be not up-to-date.
    // Invoke `Shard::refresh_states` to refresh it.
    pub fn get_max_ts(&self) -> u64 {
        cmp::max(
            self.max_ts.load(Ordering::Relaxed),
            self.get_data().get_mem_table_max_ts(),
        )
    }

    pub fn get_estimated_kv_size(&self) -> u64 {
        self.estimated_kv_size.load(Ordering::Relaxed)
    }

    pub fn get_initial_flushed(&self) -> bool {
        self.initial_flushed.load(Acquire)
    }

    fn refresh_compaction_priority(&self) {
        let mut max_pri = CompactionPriority::default();
        max_pri.shard_id = self.id;
        max_pri.shard_ver = self.ver;
        max_pri.cf = -1;
        let data = self.get_data();
        if data.l0_tbls.len() > 2 {
            let size_score = data.get_l0_total_size() as f64 / self.opt.base_size as f64;
            let num_tbl_score = data.l0_tbls.len() as f64 / 4.0;
            max_pri.score = size_score * 0.6 + num_tbl_score * 0.4;
        }
        for l0 in &data.l0_tbls {
            if !data.cover_full_table(l0.smallest(), l0.biggest()) {
                // set highest priority for newly split L0.
                max_pri.score += 2.0;
                let mut lock = self.compaction_priority.write().unwrap();
                *lock = Some(max_pri);
                return;
            }
        }
        for cf in 0..NUM_CFS {
            let scf = data.get_cf(cf);
            for lh in &scf.levels[..scf.levels.len() - 1] {
                let level_total_size = data.get_level_total_size(lh);
                let score = level_total_size as f64
                    / ((self.opt.base_size as f64) * 10f64.powf((lh.level - 1) as f64));
                if max_pri.score < score {
                    max_pri.score = score;
                    max_pri.level = lh.level;
                    max_pri.cf = cf as isize;
                }
            }
        }
        let mut lock = self.compaction_priority.write().unwrap();
        *lock = (max_pri.score > 1.0).then(|| max_pri);
    }

    pub(crate) fn get_compaction_priority(&self) -> Option<CompactionPriority> {
        self.compaction_priority.read().unwrap().clone()
    }

    pub(crate) fn get_data(&self) -> ShardData {
        self.data.read().unwrap().clone()
    }

    pub(crate) fn set_data(&self, data: ShardData) {
        let mut guard = self.data.write().unwrap();
        *guard = data
    }

    pub fn new_snap_access(&self) -> SnapAccess {
        SnapAccess::new(self)
    }

    pub fn get_writable_mem_table_size(&self) -> u64 {
        let guard = self.data.read().unwrap();
        guard.mem_tbls[0].size()
    }

    pub fn has_over_bound_data(&self) -> bool {
        self.get_data().has_over_bound_data()
    }

    pub fn get_trim_over_bound(&self) -> bool {
        self.get_data().trim_over_bound
    }

    pub fn data_all_persisted(&self) -> bool {
        self.data.read().unwrap().all_presisted()
    }

    pub fn tag(&self) -> ShardTag {
        ShardTag::new(self.engine_id, IDVer::new(self.id, self.ver))
    }

    pub(crate) fn ready_to_compact(&self) -> bool {
        self.is_active()
            && self.get_initial_flushed()
            && (self.get_compaction_priority().is_some()
                || self.get_data().ready_to_destroy_range()
                || self.get_data().ready_to_truncate_ts()
                || self.get_data().ready_to_trim_over_bound())
    }

    pub(crate) fn add_parent_mem_tbls(&self, parent: Arc<Shard>) {
        let shard_data = self.get_data();
        let parent_data = parent.get_data();
        let mem_tbls = self.split_mem_tables(&parent_data.mem_tbls);
        let mem_tbl_vers: Vec<u64> = mem_tbls.iter().map(|tbl| tbl.get_version()).collect();
        info!("{} add parent mem-tables {:?}", self.tag(), mem_tbl_vers);
        let new_data = ShardData::new(
            shard_data.start.clone(),
            shard_data.end.clone(),
            shard_data.del_prefixes.clone(),
            shard_data.truncate_ts,
            shard_data.trim_over_bound,
            mem_tbls,
            shard_data.l0_tbls.clone(),
            shard_data.cfs.clone(),
        );
        self.set_data(new_data);
    }
}

#[derive(Clone)]
pub(crate) struct ShardData {
    pub(crate) core: Arc<ShardDataCore>,
}

impl Deref for ShardData {
    type Target = ShardDataCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl ShardData {
    pub fn new_empty(start: Bytes, end: Bytes) -> Self {
        Self::new(
            start,
            end,
            DeletePrefixes::default(),
            None,
            false,
            vec![CFTable::new()],
            vec![],
            [ShardCF::new(0), ShardCF::new(1), ShardCF::new(2)],
        )
    }

    pub fn new(
        start: Bytes,
        end: Bytes,
        del_prefixes: DeletePrefixes,
        truncate_ts: Option<TruncateTs>,
        trim_over_bound: bool,
        mem_tbls: Vec<memtable::CFTable>,
        l0_tbls: Vec<L0Table>,
        cfs: [ShardCF; 3],
    ) -> Self {
        assert!(!mem_tbls.is_empty());
        Self {
            core: Arc::new(ShardDataCore {
                start,
                end,
                del_prefixes,
                truncate_ts,
                trim_over_bound,
                mem_tbls,
                l0_tbls,
                cfs,
            }),
        }
    }
}

pub(crate) struct ShardDataCore {
    pub(crate) start: Bytes,
    pub(crate) end: Bytes,
    pub(crate) del_prefixes: DeletePrefixes,
    pub(crate) truncate_ts: Option<TruncateTs>,
    pub(crate) trim_over_bound: bool,
    pub(crate) mem_tbls: Vec<memtable::CFTable>,
    pub(crate) l0_tbls: Vec<L0Table>,
    pub(crate) cfs: [ShardCF; 3],
}

impl ShardDataCore {
    pub fn get_writable_mem_table(&self) -> &memtable::CFTable {
        self.mem_tbls.first().unwrap()
    }

    pub(crate) fn get_cf(&self, cf: usize) -> &ShardCF {
        &self.cfs[cf]
    }

    pub(crate) fn get_all_files(&self) -> Vec<u64> {
        let mut files = Vec::new();
        for l0 in &self.l0_tbls {
            files.push(l0.id());
        }
        self.for_each_level(|_cf, lh| {
            for tbl in lh.tables.iter() {
                files.push(tbl.id())
            }
            false
        });
        files.sort_unstable();
        files
    }

    pub(crate) fn for_each_level<F>(&self, mut f: F)
    where
        F: FnMut(usize /*cf*/, &LevelHandler) -> bool, /*stopped*/
    {
        for cf in 0..NUM_CFS {
            let scf = self.get_cf(cf);
            for lh in scf.levels.as_slice() {
                if f(cf, lh) {
                    return;
                }
            }
        }
    }

    // Return (max_ts).
    pub(crate) fn get_mem_table_max_ts(&self) -> u64 {
        let mut max_ts = 0;
        self.mem_tbls.iter().for_each(|t| {
            max_ts = cmp::max(max_ts, t.data_max_ts());
        });
        max_ts
    }

    pub(crate) fn get_l0_total_size(&self) -> u64 {
        let (total_size, ..) = self.get_l0_stats();
        total_size
    }

    // Return (total_size, total_entries, total_kv_size, max_ts).
    pub(crate) fn get_l0_stats(&self) -> (u64, u64, u64, u64) {
        let (mut total_size, mut total_entries, mut total_kv_size, mut max_ts) = (0, 0, 0, 0);
        self.l0_tbls.iter().for_each(|l0| {
            if self.cover_full_table(l0.smallest(), l0.biggest()) {
                total_size += l0.size();
                total_entries += l0.entries();
                total_kv_size += l0.kv_size();
            } else {
                total_size += l0.size() / 2;
                total_entries += l0.entries() / 2;
                total_kv_size += l0.kv_size() / 2;
            }
            max_ts = cmp::max(max_ts, l0.max_ts());
        });
        (total_size, total_entries, total_kv_size, max_ts)
    }

    pub(crate) fn get_level_total_size(&self, level: &LevelHandler) -> u64 {
        let (total_size, ..) = self.get_level_stats(level);
        total_size
    }

    // Return (total_size, total_entries, total_kv_size, max_ts).
    pub(crate) fn get_level_stats(&self, level: &LevelHandler) -> (u64, u64, u64, u64) {
        let (mut total_size, mut total_entries, mut total_kv_size, mut max_ts) = (0, 0, 0, 0);
        level.tables.iter().enumerate().for_each(|(i, tbl)| {
            if self.is_over_bound_table(level, i, tbl) {
                total_size += tbl.size() / 2;
                total_entries += tbl.entries as u64 / 2;
                total_kv_size += tbl.kv_size / 2;
            } else {
                total_size += tbl.size();
                total_entries += tbl.entries as u64;
                total_kv_size += tbl.kv_size;
            }
            max_ts = cmp::max(max_ts, tbl.max_ts);
        });
        (total_size, total_entries, total_kv_size, max_ts)
    }

    fn is_over_bound_table(&self, level: &LevelHandler, i: usize, tbl: &SSTable) -> bool {
        let is_bound = i == 0 || i == level.tables.len() - 1;
        is_bound && !self.cover_full_table(tbl.smallest(), tbl.biggest())
    }

    pub fn cover_full_table(&self, smallest: &[u8], biggest: &[u8]) -> bool {
        self.start <= smallest && biggest < self.end
    }

    pub fn all_presisted(&self) -> bool {
        self.mem_tbls.len() == 1 && self.mem_tbls[0].size() == 0
    }

    pub fn writable_mem_table_has_data_in_deleted_prefix(&self) -> bool {
        let mem_tbl = self.get_writable_mem_table();
        self.del_prefixes
            .delete_ranges()
            .any(|(start, end)| mem_tbl.has_data_in_range(start, end))
    }

    pub fn ready_to_destroy_range(&self) -> bool {
        !self.del_prefixes.is_empty()
            // No memtable contains data covered by deleted prefixes.
            && !self.mem_tbls.iter().any(|mem_tbl| {
                self.del_prefixes
                    .delete_ranges()
                    .any(|(start, end)| mem_tbl.has_data_in_range(start, end))
            })
    }

    pub(crate) fn has_mem_over_bound_data(&self) -> bool {
        for mem_tbl in &self.mem_tbls {
            for cf in 0..NUM_CFS {
                let skl = mem_tbl.get_cf(cf);
                let mut iter = skl.new_iterator(false);
                iter.rewind();
                if iter.valid() && iter.key() < &self.start {
                    return true;
                }
                let mut rev_iter = skl.new_iterator(true);
                rev_iter.rewind();
                if iter.valid() && iter.key() >= &self.end {
                    return true;
                }
            }
        }
        false
    }

    pub(crate) fn has_file_over_bound_data(&self) -> bool {
        for l0 in &self.l0_tbls {
            if l0.smallest() < &self.start || l0.biggest() >= &self.end {
                return true;
            }
        }
        for cf in 0..NUM_CFS {
            let scf = &self.cfs[cf];
            for level in &scf.levels {
                if level.has_over_bound_data(self.start.chunk(), self.end.chunk()) {
                    return true;
                }
            }
        }
        false
    }

    pub fn has_over_bound_data(&self) -> bool {
        self.has_mem_over_bound_data() || self.has_file_over_bound_data()
    }

    pub fn writable_mem_table_need_truncate_ts(&self) -> bool {
        let mem_tbl = self.get_writable_mem_table();
        mem_tbl.data_max_ts() > self.truncate_ts.unwrap().inner()
    }

    pub fn ready_to_truncate_ts(&self) -> bool {
        self.truncate_ts.is_some()
        // No memtable contains data with version > truncate_ts.
        && !self.mem_tbls.iter().any(|mem_tbl| {
            mem_tbl.data_max_ts() > self.truncate_ts.unwrap().inner()
        })
    }

    pub fn ready_to_trim_over_bound(&self) -> bool {
        self.trim_over_bound && !self.has_mem_over_bound_data()
    }
}

pub fn store_u64(ptr: &AtomicU64, val: u64) {
    ptr.store(val, Release);
}

pub fn load_u64(ptr: &AtomicU64) -> u64 {
    ptr.load(Acquire)
}

pub fn store_bool(ptr: &AtomicBool, val: bool) {
    ptr.store(val, Release)
}

pub fn load_bool(ptr: &AtomicBool) -> bool {
    ptr.load(Acquire)
}

pub(crate) struct ShardCFBuilder {
    levels: Vec<LevelHandlerBuilder>,
}

impl ShardCFBuilder {
    pub(crate) fn new(cf: usize) -> Self {
        Self {
            levels: vec![LevelHandlerBuilder::new(); CF_LEVELS[cf]],
        }
    }

    pub(crate) fn build(&mut self) -> ShardCF {
        let mut levels = Vec::with_capacity(self.levels.len());
        for i in 0..self.levels.len() {
            levels.push(self.levels[i].build(i + 1))
        }
        ShardCF { levels }
    }

    pub(crate) fn add_table(&mut self, tbl: SSTable, level: usize) {
        self.levels[level - 1].add_table(tbl)
    }
}

#[derive(Clone)]
struct LevelHandlerBuilder {
    tables: Option<Vec<SSTable>>,
}

impl LevelHandlerBuilder {
    fn new() -> Self {
        Self {
            tables: Some(vec![]),
        }
    }

    fn build(&mut self, level: usize) -> LevelHandler {
        let mut tables = self.tables.take().unwrap();
        tables.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        LevelHandler::new(level, tables)
    }

    fn add_table(&mut self, tbl: SSTable) {
        if self.tables.is_none() {
            self.tables = Some(vec![])
        }
        self.tables.as_mut().unwrap().push(tbl)
    }
}

#[derive(Clone)]
pub(crate) struct ShardCF {
    pub(crate) levels: Vec<LevelHandler>,
}

impl ShardCF {
    pub(crate) fn new(cf: usize) -> Self {
        let mut levels = vec![];
        for j in 1..=CF_LEVELS[cf] {
            levels.push(LevelHandler::new(j, vec![]));
        }
        Self { levels }
    }

    pub(crate) fn set_has_overlapping(&self, cd: &mut CompactDef) {
        if cd.move_down() {
            return;
        }
        let kr = get_key_range(&cd.top);
        for lvl_idx in (cd.level + 1)..self.levels.len() {
            let lh = &self.levels[lvl_idx];
            let (left, right) = lh.overlapping_tables(&kr);
            if left < right {
                cd.has_overlap = true;
                return;
            }
        }
    }

    pub(crate) fn get_level(&self, level: usize) -> &LevelHandler {
        &self.levels[level - 1]
    }

    pub(crate) fn set_level(&mut self, level_handler: LevelHandler) {
        let level_idx = level_handler.level - 1;
        self.levels[level_idx] = level_handler
    }
}

#[derive(Default, Clone)]
pub struct LevelHandler {
    pub(crate) tables: Arc<Vec<SSTable>>,
    pub(crate) level: usize,
    pub(crate) max_ts: u64,
}

impl LevelHandler {
    pub fn new(level: usize, tables: Vec<SSTable>) -> Self {
        let mut max_ts = 0;
        for tbl in &tables {
            if max_ts < tbl.max_ts {
                max_ts = tbl.max_ts;
            }
        }
        Self {
            tables: Arc::new(tables),
            level,
            max_ts,
        }
    }

    pub(crate) fn overlapping_tables(&self, key_range: &KeyRange) -> (usize, usize) {
        get_tables_in_range(
            &self.tables,
            key_range.left.chunk(),
            key_range.right.chunk(),
        )
    }

    pub fn get(&self, key: &[u8], version: u64, key_hash: u64) -> table::Value {
        self.get_in_table(key, version, key_hash, self.get_table(key))
    }

    fn get_in_table(
        &self,
        key: &[u8],
        version: u64,
        key_hash: u64,
        tbl: Option<&SSTable>,
    ) -> table::Value {
        if tbl.is_none() {
            return table::Value::new();
        }
        tbl.unwrap().get(key, version, key_hash)
    }

    pub(crate) fn get_table(&self, key: &[u8]) -> Option<&SSTable> {
        let idx = search(self.tables.len(), |i| self.tables[i].biggest() >= key);
        if idx >= self.tables.len() {
            return None;
        }
        Some(&self.tables[idx])
    }

    pub(crate) fn get_table_by_id(&self, id: u64) -> Option<SSTable> {
        for tbl in self.tables.as_slice() {
            if tbl.id() == id {
                return Some(tbl.clone());
            }
        }
        None
    }

    pub(crate) fn check_order(&self, cf: usize, tag: ShardTag) {
        let tables = &self.tables;
        if tables.len() <= 1 {
            return;
        }
        for i in 0..(tables.len() - 1) {
            let ti = &tables[i];
            let tj = &tables[i + 1];
            if ti.smallest() > ti.biggest()
                || ti.smallest() >= tj.smallest()
                || ti.biggest() >= tj.biggest()
            {
                panic!(
                    "[{}] check table fail table ti {} smallest {:?}, biggest {:?}, tj {} smallest {:?}, biggest {:?}, cf: {}, level: {}",
                    tag,
                    ti.id(),
                    ti.smallest(),
                    ti.biggest(),
                    tj.id(),
                    tj.smallest(),
                    tj.biggest(),
                    cf,
                    self.level,
                );
            }
        }
    }

    pub(crate) fn get_newer(&self, key: &[u8], version: u64, key_hash: u64) -> table::Value {
        if self.max_ts < version {
            return table::Value::new();
        }
        if let Some(tbl) = self.get_table(key) {
            return tbl.get_newer(key, version, key_hash);
        }
        table::Value::new()
    }

    pub(crate) fn has_over_bound_data(&self, start: &[u8], end: &[u8]) -> bool {
        if self.tables.is_empty() {
            return false;
        }
        let first = self.tables.first().unwrap();
        let last = self.tables.last().unwrap();
        first.smallest() < start || last.biggest() >= end
    }
}

#[derive(Default, Clone)]
pub struct Properties {
    m: DashMap<String, Bytes>,
}

impl Properties {
    pub fn new() -> Self {
        Self {
            m: dashmap::DashMap::new(),
        }
    }

    pub fn set(&self, key: &str, val: &[u8]) {
        self.m.insert(key.to_string(), Bytes::copy_from_slice(val));
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let bin = self.m.get(key)?;
        Some(bin.value().clone())
    }

    pub fn to_pb(&self, shard_id: u64) -> kvenginepb::Properties {
        let mut props = kvenginepb::Properties::new();
        props.shard_id = shard_id;
        self.m.iter().for_each(|r| {
            props.keys.push(r.key().clone());
            props.values.push(r.value().to_vec());
        });
        props
    }

    pub fn apply_pb(self, props: &kvenginepb::Properties) -> Self {
        let keys = props.get_keys();
        let vals = props.get_values();
        for i in 0..keys.len() {
            let key = &keys[i];
            let val = &vals[i];
            self.set(key, val.as_slice());
        }
        self
    }
}

pub fn get_shard_property(key: &str, props: &kvenginepb::Properties) -> Option<Vec<u8>> {
    let keys = props.get_keys();
    for i in 0..keys.len() {
        if key == keys[i] {
            return Some(props.get_values()[i].clone());
        }
    }
    None
}

pub fn get_splitting_start_end<'a: 'b, 'b>(
    start: &'a [u8],
    end: &'a [u8],
    split_keys: &'b [Vec<u8>],
    i: usize,
) -> (&'b [u8], &'b [u8]) {
    let start_key = if i != 0 {
        split_keys[i - 1].as_slice()
    } else {
        start as &'b [u8]
    };
    let end_key = if i == split_keys.len() {
        end
    } else {
        split_keys[i].as_slice()
    };
    (start_key, end_key)
}

#[derive(Clone, Default)]
pub struct DeletePrefixes {
    pub(crate) prefixes: Vec<Vec<u8>>,
    // prefixes_nexts are prefix-next keys of prefixes, i.e., [prefixes[i], prefixes_nexts[i]) is
    // the range that should be destroyed.
    prefixes_nexts: Vec<Vec<u8>>,
}

impl std::fmt::Debug for DeletePrefixes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeletePrefixes")
            .field(
                "prefixes",
                &self
                    .prefixes
                    .iter()
                    .map(|p| log_wrappers::Value::key(p))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl DeletePrefixes {
    fn gen_prefixes_nexts(prefixes: &[Vec<u8>]) -> Vec<Vec<u8>> {
        prefixes
            .iter()
            .map(|p| {
                let mut p_n = p.to_vec();
                tidb_query_common::util::convert_to_prefix_next(&mut p_n);
                p_n
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.prefixes.is_empty()
    }

    pub fn merge(&self, prefix: &[u8]) -> Self {
        let mut new_prefixes = vec![];
        for old_prefix in &self.prefixes {
            if prefix.starts_with(old_prefix) {
                // old prefix covers new prefix, do not need to change.
                return self.clone();
            }
            if old_prefix.starts_with(prefix) {
                // new prefix covers old prefix. old prefix can be dropped.
                continue;
            }
            new_prefixes.push(old_prefix.clone());
        }
        new_prefixes.push(prefix.to_vec());
        new_prefixes.sort_unstable();
        let new_prefixes_nexts = DeletePrefixes::gen_prefixes_nexts(&new_prefixes);
        Self {
            prefixes: new_prefixes,
            prefixes_nexts: new_prefixes_nexts,
        }
    }

    /// Removes all prefixes in the `other` one.
    pub fn split(&self, other: &DeletePrefixes) -> Self {
        let mut new = self.clone();
        new.prefixes.retain(|p| !other.prefixes.contains(p));
        new.prefixes_nexts = DeletePrefixes::gen_prefixes_nexts(&new.prefixes);
        new
    }

    pub fn marshal(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for prefix in &self.prefixes {
            buf.put_u16_le(prefix.len() as u16);
            buf.extend_from_slice(prefix);
        }
        buf
    }

    pub fn unmarshal(mut data: &[u8]) -> Self {
        let mut prefixes = vec![];
        while !data.is_empty() {
            let len = data.get_u16_le() as usize;
            prefixes.push(data[..len].to_vec());
            data.advance(len);
        }
        let prefixes_nexts = DeletePrefixes::gen_prefixes_nexts(&prefixes);
        Self {
            prefixes,
            prefixes_nexts,
        }
    }

    pub fn build_split(&self, start: &[u8], end: &[u8]) -> Self {
        let prefixes: Vec<_> = self
            .prefixes
            .iter()
            .filter(|prefix| {
                (start <= prefix.as_slice() && prefix.as_slice() < end) || start.starts_with(prefix)
            })
            .cloned()
            .collect();
        let prefixes_nexts = DeletePrefixes::gen_prefixes_nexts(&prefixes);
        Self {
            prefixes,
            prefixes_nexts,
        }
    }

    pub fn cover_prefix(&self, prefix: &[u8]) -> bool {
        self.prefixes.iter().any(|old| prefix.starts_with(old))
    }

    pub fn cover_range(&self, start: &[u8], end: &[u8]) -> bool {
        self.prefixes
            .iter()
            .any(|p| start.starts_with(p) && end.starts_with(p))
    }

    pub fn delete_ranges(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.prefixes
            .iter()
            .map(|p| p.as_slice())
            .zip(self.prefixes_nexts.iter().map(|p| p.as_slice()))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct TruncateTs(u64);

impl TruncateTs {
    pub fn marshal(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    pub fn unmarshal(mut data: &[u8]) -> Self {
        assert_eq!(data.len(), U64_SIZE);
        Self(data.get_u64_le())
    }

    #[inline]
    pub fn inner(&self) -> u64 {
        self.0
    }
}

impl From<u64> for TruncateTs {
    fn from(ts: u64) -> Self {
        Self(ts)
    }
}

pub(crate) fn need_update_truncate_ts(cur: Option<TruncateTs>, new: TruncateTs) -> bool {
    if cur.is_none() {
        return true;
    }
    new <= cur.unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete_prefix() {
        let assert_prefix_invariant = |del_prefix: &DeletePrefixes| {
            assert_eq!(del_prefix.prefixes.len(), del_prefix.prefixes_nexts.len());
            assert_eq!(
                del_prefix.prefixes.len(),
                del_prefix.delete_ranges().count()
            );
            assert!(del_prefix.delete_ranges().all(|(p, p_n)| {
                let mut p_c = p.to_vec();
                tidb_query_common::util::convert_to_prefix_next(&mut p_c);
                p_c == p_n
            }));
        };

        let mut del_prefix = DeletePrefixes::default();
        assert_prefix_invariant(&del_prefix);
        assert!(del_prefix.is_empty());
        del_prefix = del_prefix.merge("101".as_bytes());
        assert!(!del_prefix.is_empty());
        assert_eq!(del_prefix.prefixes.len(), 1);
        assert_prefix_invariant(&del_prefix);
        for prefix in ["1010", "101"] {
            assert!(del_prefix.cover_prefix(prefix.as_bytes()));
        }
        for prefix in ["10", "103"] {
            assert!(!del_prefix.cover_prefix(prefix.as_bytes()));
        }

        del_prefix = del_prefix.merge("105".as_bytes());
        assert_prefix_invariant(&del_prefix);
        let bin = del_prefix.marshal();
        del_prefix = DeletePrefixes::unmarshal(&bin);
        assert_prefix_invariant(&del_prefix);
        assert_eq!(del_prefix.prefixes.len(), 2);
        for prefix in ["1010", "101", "1050", "105"] {
            assert!(del_prefix.cover_prefix(prefix.as_bytes()));
        }
        for prefix in ["10", "103"] {
            assert!(!del_prefix.cover_prefix(prefix.as_bytes()));
        }

        del_prefix = del_prefix.merge("10".as_bytes());
        assert_prefix_invariant(&del_prefix);
        assert_eq!(del_prefix.prefixes.len(), 1);
        for prefix in ["10", "101", "103", "104"] {
            assert!(del_prefix.cover_prefix(prefix.as_bytes()));
        }
        for prefix in ["1", "11"] {
            assert!(!del_prefix.cover_prefix(prefix.as_bytes()));
        }

        del_prefix = DeletePrefixes::default();
        del_prefix = del_prefix.merge("101".as_bytes());
        del_prefix = del_prefix.merge("102".as_bytes());
        assert_prefix_invariant(&del_prefix);
        for (start, end) in [("101", "1011"), ("102", "1022")] {
            assert!(del_prefix.cover_range(start.as_bytes(), end.as_bytes()));
        }
        for (start, end) in [("99", "100"), ("101", "102"), ("102", "103")] {
            assert!(!del_prefix.cover_range(start.as_bytes(), end.as_bytes()));
        }

        del_prefix = DeletePrefixes::default();
        del_prefix = del_prefix.merge("101".as_bytes());
        del_prefix = del_prefix.merge("1033".as_bytes());
        del_prefix = del_prefix.merge("1055".as_bytes());
        del_prefix = del_prefix.merge("107".as_bytes());
        assert_prefix_invariant(&del_prefix);

        let split_del_range = del_prefix.build_split("1033".as_bytes(), "1055".as_bytes());
        assert_prefix_invariant(&split_del_range);
        assert_eq!(split_del_range.prefixes.len(), 1);
        assert_eq!(&split_del_range.prefixes[0], "1033".as_bytes());

        let split_del_range = del_prefix.build_split("1034".as_bytes(), "1055".as_bytes());
        assert_prefix_invariant(&split_del_range);
        assert_eq!(split_del_range.prefixes.len(), 0);

        let split_del_range = del_prefix.build_split("10334".as_bytes(), "10555".as_bytes());
        assert_prefix_invariant(&split_del_range);
        assert_eq!(split_del_range.prefixes.len(), 2);
        assert_eq!(&split_del_range.prefixes[0], "1033".as_bytes());
        assert_eq!(&split_del_range.prefixes[1], "1055".as_bytes());

        del_prefix = DeletePrefixes::default()
            .merge("100".as_bytes())
            .merge("200".as_bytes())
            .merge("300".as_bytes());
        assert_prefix_invariant(&del_prefix);
        del_prefix = del_prefix.split(&DeletePrefixes::default().merge("100".as_bytes()));
        assert_prefix_invariant(&del_prefix);
        assert_eq!(del_prefix.prefixes.len(), 2);
        assert!(!del_prefix.cover_prefix("100".as_bytes()));
        assert!(del_prefix.cover_prefix("200".as_bytes()));
        assert!(del_prefix.cover_prefix("300".as_bytes()));
        del_prefix = del_prefix.split(
            &DeletePrefixes::default()
                .merge("200".as_bytes())
                .merge("300".as_bytes()),
        );
        assert_prefix_invariant(&del_prefix);
        assert_eq!(del_prefix.prefixes.len(), 0);
        assert!(!del_prefix.cover_prefix("100".as_bytes()));
        assert!(!del_prefix.cover_prefix("200".as_bytes()));
        assert!(!del_prefix.cover_prefix("300".as_bytes()));
    }

    #[test]
    fn test_truncate_ts() {
        let ts = 437598164238729283;
        let truncate_ts = TruncateTs(ts);
        assert_eq!(
            TruncateTs::unmarshal(truncate_ts.marshal().as_slice()),
            truncate_ts
        );
        assert_eq!(truncate_ts.inner(), ts);
    }
}
