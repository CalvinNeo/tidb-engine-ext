// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    collections::HashMap,
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
use rand::Rng;
use slog_global::*;
use tikv_util::codec::number::U64_SIZE;

use crate::{
    table::{
        self,
        blobtable::blobtable::BlobTable,
        memtable::{self, CfTable},
        search,
        sstable::{L0Table, SsTable},
    },
    Iterator as TableIterator, *,
};

pub struct Shard {
    pub engine_id: u64,
    pub id: u64,
    pub ver: u64,
    pub range: ShardRange,
    pub parent_id: u64,
    pub(crate) data: RwLock<ShardData>,
    pub(crate) opt: Arc<Options>,

    // If the shard is not active, flush mem table and do compaction will ignore this shard.
    pub(crate) active: AtomicBool,

    pub(crate) properties: Properties,
    pub(crate) compacting: AtomicBool,
    pub(crate) initial_flushed: AtomicBool,

    pub(crate) base_version: AtomicU64,

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

impl Deref for Shard {
    type Target = ShardRange;

    fn deref(&self) -> &Self::Target {
        &self.range
    }
}

impl Shard {
    pub fn new(
        engine_id: u64,
        props: &pb::Properties,
        ver: u64,
        range: ShardRange,
        opt: Arc<Options>,
    ) -> Self {
        let shard = Self {
            engine_id,
            id: props.shard_id,
            ver,
            range: range.clone(),
            parent_id: 0,
            data: RwLock::new(ShardData::new_empty(range)),
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
        let range = ShardRange::from_snap(snap);
        let mut shard = Self::new(engine_id, snap.get_properties(), cs.shard_ver, range, opt);
        if !cs.has_parent() {
            store_bool(&shard.initial_flushed, true);
        } else {
            shard.parent_id = cs.get_parent().shard_id;
            let mut guard = shard.parent_snap.write().unwrap();
            *guard = Some(cs.get_parent().get_snapshot().clone());
        }
        shard.base_version.store(snap.base_version, Release);
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

    pub(crate) fn id_ver(&self) -> IdVer {
        IdVer::new(self.id, self.ver)
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

    pub(crate) fn gen_rand_schedule_del_range_time(&self) -> u64 {
        let now = time::precise_time_ns();
        let delay = rand::thread_rng().gen_range(0..self.opt.max_del_range_delay.as_nanos() as u64);
        now + delay
    }

    pub(crate) fn set_del_prefix(&self, val: &[u8]) {
        let mut del_prefixes = DeletePrefixes::unmarshal(val, self.inner_key_off);
        del_prefixes.schedule_at = self.gen_rand_schedule_del_range_time();
        let data = self.get_data();
        let new_data = ShardData::new(
            data.range.clone(),
            del_prefixes,
            data.truncate_ts,
            data.trim_over_bound,
            data.mem_tbls.clone(),
            data.l0_tbls.clone(),
            data.blob_tbl_map.clone(),
            data.cfs.clone(),
        );
        self.set_data(new_data);
    }

    pub(crate) fn merge_del_prefix(&self, prefix: &[u8]) {
        let data = self.get_data();
        let mut del_prefixes = data.del_prefixes.merge(prefix);
        del_prefixes.schedule_at = self.gen_rand_schedule_del_range_time();
        let new_data = ShardData::new(
            data.range.clone(),
            del_prefixes,
            data.truncate_ts,
            data.trim_over_bound,
            data.mem_tbls.clone(),
            data.l0_tbls.clone(),
            data.blob_tbl_map.clone(),
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
            data.range.clone(),
            data.del_prefixes.clone(),
            Some(truncate_ts),
            data.trim_over_bound,
            data.mem_tbls.clone(),
            data.l0_tbls.clone(),
            data.blob_tbl_map.clone(),
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
            data.range.clone(),
            data.del_prefixes.clone(),
            data.truncate_ts,
            trim_over_bound,
            data.mem_tbls.clone(),
            data.l0_tbls.clone(),
            data.blob_tbl_map.clone(),
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
            if let Some(key) = max_level.tables[0].get_suggest_split_key() {
                return Some([self.key_prefix(), key.to_vec().as_slice()].concat().into());
            }
            return None;
        }
        let tbl_idx = max_level.tables.len() * 2 / 3;
        Some(
            [self.key_prefix(), max_level.tables[tbl_idx].smallest()]
                .concat()
                .into(),
        )
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
                        .then(|| [self.key_prefix(), tbl.smallest()].concat().into())
                })
                .collect(),
        )
    }

    pub(crate) fn overlap_table(&self, smallest: &[u8], biggest: &[u8]) -> bool {
        self.inner_start() <= biggest && smallest < self.inner_end()
    }

    pub(crate) fn cover_full_table(&self, smallest: &[u8], biggest: &[u8]) -> bool {
        self.inner_start() <= smallest && biggest < self.inner_end()
    }

    pub(crate) fn overlap_key(&self, key: &[u8]) -> bool {
        self.inner_start() <= key && key < self.inner_end()
    }

    pub fn get_property(&self, key: &str) -> Option<Bytes> {
        self.properties.get(key)
    }

    pub fn set_property(&self, key: &str, val: &[u8]) {
        self.properties.set(key, val);
    }

    pub(crate) fn load_mem_table_version(&self) -> u64 {
        self.get_base_version() + self.write_sequence.load(Acquire)
    }

    pub fn get_all_files(&self) -> Vec<u64> {
        let data = self.get_data();
        data.get_all_files()
    }

    pub(crate) fn split_mem_tables(&self, parent_mem_tbls: &[CfTable]) -> Vec<CfTable> {
        let mut new_mem_tbls = vec![CfTable::new()];
        for old_mem_tbl in parent_mem_tbls {
            if old_mem_tbl.has_data_in_range(self.inner_start(), self.inner_end()) {
                new_mem_tbls.push(old_mem_tbl.new_split());
            }
        }
        new_mem_tbls
    }

    pub fn get_base_version(&self) -> u64 {
        self.base_version.load(Ordering::Acquire)
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
        let data = self.get_data();
        if !data.blob_tbl_map.is_empty() {
            let blob_table_utilization = {
                let mut in_use_blob_size = 0;
                let mut total_blob_size = 0;
                for l0 in &data.l0_tbls {
                    in_use_blob_size += l0.total_blob_size();
                }
                for cf in &data.cfs {
                    for lh in &cf.levels {
                        for sst in &(*lh.tables) {
                            in_use_blob_size += sst.total_blob_size();
                        }
                    }
                }

                for blob_table in data.blob_tbl_map.values() {
                    total_blob_size += blob_table.total_blob_size();
                }
                if total_blob_size == 0 {
                    1.0
                } else {
                    in_use_blob_size as f64 / total_blob_size as f64
                }
            };
            if blob_table_utilization < self.opt.blob_table_gc_ratio {
                let mut lock = self.compaction_priority.write().unwrap();
                *lock = Some(CompactionPriority::Major);
                return;
            }
        }
        let mut score = 0.0;
        let mut cf_with_highest_score = -1;
        let mut level_with_highest_score = 0;
        if data.l0_tbls.len() > 2 {
            let size_score = data.get_l0_total_size() as f64 / self.opt.base_size as f64;
            let num_tbl_score = data.l0_tbls.len() as f64 / 4.0;
            score = size_score * 0.6 + num_tbl_score * 0.4;
        }
        for l0 in &data.l0_tbls {
            if !data.cover_full_table(l0.smallest(), l0.biggest()) {
                // set highest priority for newly split L0.
                score += 2.0;
                let mut lock = self.compaction_priority.write().unwrap();
                *lock = Some(CompactionPriority::L0 { score });
                return;
            }
        }
        for cf in 0..NUM_CFS {
            let scf = data.get_cf(cf);
            for lh in &scf.levels[..scf.levels.len() - 1] {
                let level_total_size = data.get_level_total_size(lh);
                let lvl_score = level_total_size as f64
                    / ((self.opt.base_size as f64) * 10f64.powf((lh.level - 1) as f64));
                if score < lvl_score {
                    score = lvl_score;
                    level_with_highest_score = lh.level;
                    cf_with_highest_score = cf as isize;
                }
            }
        }
        let priority = if score > 1.0 {
            let max_pri = if level_with_highest_score == 0 {
                CompactionPriority::L0 { score }
            } else {
                CompactionPriority::L1Plus {
                    cf: cf_with_highest_score,
                    level: level_with_highest_score,
                    score,
                }
            };
            Some(max_pri)
        } else {
            None
        };
        let mut lock = self.compaction_priority.write().unwrap();
        *lock = priority;
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
        ShardTag::new(self.engine_id, IdVer::new(self.id, self.ver))
    }

    pub(crate) fn ready_to_compact(&self) -> bool {
        let data = self.get_data();
        self.is_active()
            && self.get_initial_flushed()
            && (self.get_compaction_priority().is_some()
                || data.ready_to_destroy_range()
                || data.ready_to_truncate_ts()
                || data.ready_to_trim_over_bound())
    }

    pub(crate) fn add_parent_mem_tbls(&self, parent: Arc<Shard>) {
        let shard_data = self.get_data();
        let parent_data = parent.get_data();
        let mem_tbls = self.split_mem_tables(&parent_data.mem_tbls);
        let mem_tbl_vers: Vec<u64> = mem_tbls.iter().map(|tbl| tbl.get_version()).collect();
        info!("{} add parent mem-tables {:?}", self.tag(), mem_tbl_vers);
        let new_data = ShardData::new(
            shard_data.range.clone(),
            shard_data.del_prefixes.clone(),
            shard_data.truncate_ts,
            shard_data.trim_over_bound,
            mem_tbls,
            shard_data.l0_tbls.clone(),
            shard_data.blob_tbl_map.clone(),
            shard_data.cfs.clone(),
        );
        self.set_data(new_data);
    }

    pub(crate) fn inner_start(&self) -> &[u8] {
        &self.outer_start[self.inner_key_off..]
    }

    pub(crate) fn inner_end(&self) -> &[u8] {
        if self.inner_key_off == self.outer_end.len() {
            return GLOBAL_SHARD_END_KEY;
        }
        &self.outer_end[self.inner_key_off..]
    }

    pub(crate) fn key_prefix(&self) -> &[u8] {
        if self.inner_key_off > 0 {
            return &self.outer_start[0..self.inner_key_off];
        }
        &[]
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
    pub(crate) fn new_empty(shard_range: ShardRange) -> Self {
        let inner_key_off = shard_range.inner_key_off;
        Self::new(
            shard_range,
            DeletePrefixes::new_with_inner_key_off(inner_key_off),
            None,
            false,
            vec![CfTable::new()],
            vec![],
            Arc::new(HashMap::new()),
            [ShardCf::new(0), ShardCf::new(1), ShardCf::new(2)],
        )
    }

    pub(crate) fn new(
        range: ShardRange,
        del_prefixes: DeletePrefixes,
        truncate_ts: Option<TruncateTs>,
        trim_over_bound: bool,
        mem_tbls: Vec<memtable::CfTable>,
        l0_tbls: Vec<L0Table>,
        blob_tbl_map: Arc<HashMap<u64, BlobTable>>,
        cfs: [ShardCf; 3],
    ) -> Self {
        assert!(!mem_tbls.is_empty());

        Self {
            core: Arc::new(ShardDataCore {
                range,
                del_prefixes,
                truncate_ts,
                trim_over_bound,
                mem_tbls,
                l0_tbls,
                blob_tbl_map,
                cfs,
            }),
        }
    }
}

pub(crate) struct ShardDataCore {
    pub(crate) range: ShardRange,
    pub(crate) del_prefixes: DeletePrefixes,
    pub(crate) truncate_ts: Option<TruncateTs>,
    pub(crate) trim_over_bound: bool,
    pub(crate) mem_tbls: Vec<memtable::CfTable>,
    pub(crate) l0_tbls: Vec<L0Table>,
    pub(crate) blob_tbl_map: Arc<HashMap<u64, BlobTable>>,
    pub(crate) cfs: [ShardCf; 3],
}

impl Deref for ShardDataCore {
    type Target = ShardRange;

    fn deref(&self) -> &Self::Target {
        &self.range
    }
}

impl ShardDataCore {
    pub fn get_writable_mem_table(&self) -> &memtable::CfTable {
        self.mem_tbls.first().unwrap()
    }

    pub(crate) fn get_cf(&self, cf: usize) -> &ShardCf {
        &self.cfs[cf]
    }

    pub(crate) fn get_all_files(&self) -> Vec<u64> {
        let mut files = Vec::new();
        for l0 in &self.l0_tbls {
            files.push(l0.id());
        }
        for blob_tbl_id in self.blob_tbl_map.keys() {
            files.push(*blob_tbl_id);
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
        F: FnMut(usize /* cf */, &LevelHandler) -> bool, // stopped
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

    // Return (total_size, total_blob_size).
    // pub(crate) fn get_blob_total_size(&self) -> u64 {
    // let mut total_size = 0u64;
    //
    // self.blob_tbls.iter().for_each(|blob_tbl| {
    // if self.cover_full_table(blob_tbl.smallest_key(), blob_tbl.biggest_key()) {
    // total_size += blob_tbl.size();
    // } else {
    // total_size += blob_tbl.size() / 2;
    // };
    // });
    // total_size
    // }

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

    fn is_over_bound_table(&self, level: &LevelHandler, i: usize, tbl: &SsTable) -> bool {
        let is_bound = i == 0 || i == level.tables.len() - 1;
        is_bound && !self.cover_full_table(tbl.smallest(), tbl.biggest())
    }

    pub(crate) fn cover_full_table(&self, smallest: &[u8], biggest: &[u8]) -> bool {
        self.inner_start() <= smallest && biggest < self.inner_end()
    }

    pub fn all_presisted(&self) -> bool {
        self.mem_tbls.len() == 1 && self.mem_tbls[0].size() == 0
    }

    pub fn writable_mem_table_has_data_in_deleted_prefix(&self) -> bool {
        let mem_tbl = self.get_writable_mem_table();
        self.del_prefixes
            .inner_delete_ranges()
            .any(|(start, end)| mem_tbl.has_data_in_range(start, end))
    }

    pub fn ready_to_destroy_range(&self) -> bool {
        !self.del_prefixes.is_empty() && self.del_prefixes.after_scheduled_time()
            // No memtable contains data covered by deleted prefixes.
            && !self.mem_tbls.iter().any(|mem_tbl| {
                self.del_prefixes
                    .inner_delete_ranges()
                    .any(|(start, end)| mem_tbl.has_data_in_range(start, end))
            })
    }

    pub(crate) fn has_mem_over_bound_data(&self) -> bool {
        for mem_tbl in &self.mem_tbls {
            for cf in 0..NUM_CFS {
                let skl = mem_tbl.get_cf(cf);
                let mut iter = skl.new_iterator(false);
                iter.rewind();
                if iter.valid() && iter.key() < self.inner_start() {
                    return true;
                }
                let mut rev_iter = skl.new_iterator(true);
                rev_iter.rewind();
                if iter.valid() && iter.key() >= self.inner_end() {
                    return true;
                }
            }
        }
        false
    }

    pub(crate) fn has_file_over_bound_data(&self) -> bool {
        for l0 in &self.l0_tbls {
            if l0.smallest() < self.inner_start() || l0.biggest() >= self.inner_end() {
                return true;
            }
        }
        for cf in 0..NUM_CFS {
            let scf = &self.cfs[cf];
            for level in &scf.levels {
                if level.has_over_bound_data(self.inner_start().chunk(), self.inner_end().chunk()) {
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

pub(crate) struct ShardCfBuilder {
    levels: Vec<LevelHandlerBuilder>,
}

impl ShardCfBuilder {
    pub(crate) fn new(cf: usize) -> Self {
        Self {
            levels: vec![LevelHandlerBuilder::new(); CF_LEVELS[cf]],
        }
    }

    pub(crate) fn build(&mut self) -> ShardCf {
        let mut levels = Vec::with_capacity(self.levels.len());
        for i in 0..self.levels.len() {
            levels.push(self.levels[i].build(i + 1))
        }
        ShardCf { levels }
    }

    pub(crate) fn add_table(&mut self, tbl: SsTable, level: usize) {
        self.levels[level - 1].add_table(tbl)
    }
}

#[derive(Clone)]
struct LevelHandlerBuilder {
    tables: Option<Vec<SsTable>>,
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

    fn add_table(&mut self, tbl: SsTable) {
        if self.tables.is_none() {
            self.tables = Some(vec![])
        }
        self.tables.as_mut().unwrap().push(tbl)
    }
}

#[derive(Clone)]
pub(crate) struct ShardCf {
    pub(crate) levels: Vec<LevelHandler>,
}

impl ShardCf {
    pub(crate) fn new(cf: usize) -> Self {
        let mut levels = vec![];
        for j in 1..=CF_LEVELS[cf] {
            levels.push(LevelHandler::new(j, vec![]));
        }
        Self { levels }
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
    pub(crate) tables: Arc<Vec<SsTable>>,
    pub(crate) level: usize,
    pub(crate) max_ts: u64,
}

impl LevelHandler {
    pub fn new(level: usize, tables: Vec<SsTable>) -> Self {
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

    pub fn get(
        &self,
        key: &[u8],
        version: u64,
        key_hash: u64,
        out_val_owner: &mut Vec<u8>,
    ) -> table::Value {
        self.get_in_table(key, version, key_hash, self.get_table(key), out_val_owner)
    }

    fn get_in_table(
        &self,
        key: &[u8],
        version: u64,
        key_hash: u64,
        tbl: Option<&SsTable>,
        out_val_owner: &mut Vec<u8>,
    ) -> table::Value {
        if tbl.is_none() {
            return table::Value::new();
        }
        tbl.unwrap()
            .get(key, version, key_hash, out_val_owner, self.level)
    }

    pub(crate) fn get_table(&self, key: &[u8]) -> Option<&SsTable> {
        let idx = search(self.tables.len(), |i| self.tables[i].biggest() >= key);
        if idx >= self.tables.len() {
            return None;
        }
        Some(&self.tables[idx])
    }

    pub(crate) fn get_table_by_id(&self, id: u64) -> Option<SsTable> {
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

    pub(crate) fn get_newer(
        &self,
        key: &[u8],
        version: u64,
        key_hash: u64,
        out_val_owner: &mut Vec<u8>,
    ) -> table::Value {
        if self.max_ts < version {
            return table::Value::new();
        }
        if let Some(tbl) = self.get_table(key) {
            return tbl.get_newer(key, version, key_hash, out_val_owner, self.level);
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

#[derive(Clone)]
pub struct DeletePrefixes {
    pub(crate) prefixes: Vec<Vec<u8>>,
    // prefixes_nexts are prefix-next keys of prefixes, i.e., [prefixes[i], prefixes_nexts[i]) is
    // the range that should be destroyed.
    prefixes_nexts: Vec<Vec<u8>>,
    pub(crate) schedule_at: u64,
    pub(crate) inner_key_off: usize,
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
    pub fn new_with_inner_key_off(inner_key_off: usize) -> Self {
        Self {
            prefixes: vec![],
            prefixes_nexts: vec![],
            schedule_at: 0,
            inner_key_off,
        }
    }
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

    pub fn after_scheduled_time(&self) -> bool {
        time::precise_time_ns() > self.schedule_at
    }

    pub fn merge(&self, prefix: &[u8]) -> Self {
        assert!(prefix.len() >= self.inner_key_off);

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
            schedule_at: 0,
            inner_key_off: self.inner_key_off,
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

    pub fn unmarshal(mut data: &[u8], inner_key_off: usize) -> Self {
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
            schedule_at: 0,
            inner_key_off,
        }
    }

    pub fn build_split(&self, start: &[u8], end: &[u8], inner_key_off: usize) -> Self {
        let prefixes: Vec<_> = self
            .prefixes
            .iter()
            .filter(|prefix| {
                (start <= prefix.as_slice() && prefix.as_slice() < end) || start.starts_with(prefix)
            })
            .cloned()
            .collect();
        let prefixes_nexts = DeletePrefixes::gen_prefixes_nexts(&prefixes);
        let scheduled_at = self.schedule_at;
        Self {
            prefixes,
            prefixes_nexts,
            schedule_at: scheduled_at,
            inner_key_off,
        }
    }

    pub(crate) fn cover_prefix(&self, inner_prefix: &[u8]) -> bool {
        let inner_key_off = self.inner_key_off;
        self.prefixes.iter().any(|p| {
            if inner_key_off >= p.len() {
                return true;
            }
            let inner_p = &p[inner_key_off..];
            inner_prefix.starts_with(inner_p)
        })
    }

    pub(crate) fn cover_range(&self, inner_start: &[u8], inner_end: &[u8]) -> bool {
        let inner_key_off = self.inner_key_off;
        self.prefixes.iter().any(|p| {
            if inner_key_off >= p.len() {
                return true;
            }
            let inner_p = &p[inner_key_off..];
            inner_start.starts_with(inner_p) && inner_end.starts_with(inner_p)
        })
    }

    pub(crate) fn inner_delete_ranges(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let inner_key_off = self.inner_key_off;
        self.prefixes
            .iter()
            .map(move |p| {
                if inner_key_off >= p.len() {
                    &[]
                } else {
                    &p[inner_key_off..]
                }
            })
            .zip(self.prefixes_nexts.iter().map(move |p| {
                if inner_key_off >= p.len() {
                    GLOBAL_SHARD_END_KEY
                } else {
                    &p[inner_key_off..]
                }
            }))
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

#[derive(Clone, Default)]
pub struct ShardRange {
    pub outer_start: Bytes,
    pub outer_end: Bytes,
    pub inner_key_off: usize,
}

impl ShardRange {
    pub fn new(outer_start: &[u8], outer_end: &[u8], inner_key_off: usize) -> Self {
        Self {
            outer_start: Bytes::from(outer_start.to_vec()),
            outer_end: Bytes::from(outer_end.to_vec()),
            inner_key_off,
        }
    }

    pub(crate) fn from_snap(snap: &kvenginepb::Snapshot) -> Self {
        Self::new(
            snap.get_outer_start(),
            snap.get_outer_end(),
            snap.inner_key_off as usize,
        )
    }

    pub fn inner_start(&self) -> &[u8] {
        &self.outer_start[self.inner_key_off..]
    }

    pub fn inner_end(&self) -> &[u8] {
        if self.inner_key_off == self.outer_end.len() {
            return GLOBAL_SHARD_END_KEY;
        }
        &self.outer_end[self.inner_key_off..]
    }

    pub(crate) fn prefix(&self) -> &[u8] {
        &self.outer_start[..self.inner_key_off]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete_prefix() {
        for inner_key_off in [0, 4] {
            let assert_prefix_invariant = |del_prefix: &DeletePrefixes| {
                assert_eq!(del_prefix.prefixes.len(), del_prefix.prefixes_nexts.len());
                assert_eq!(
                    del_prefix.prefixes.len(),
                    del_prefix.inner_delete_ranges().count()
                );
                assert!(del_prefix.inner_delete_ranges().all(|(p, p_n)| {
                    let mut p_c = p.to_vec();
                    tidb_query_common::util::convert_to_prefix_next(&mut p_c);
                    p_c == p_n
                }));
            };

            let mut del_prefix = DeletePrefixes::new_with_inner_key_off(inner_key_off);
            assert_prefix_invariant(&del_prefix);
            assert!(del_prefix.is_empty());
            del_prefix = del_prefix.merge("0000101".as_bytes());
            assert!(!del_prefix.is_empty());
            assert_eq!(del_prefix.prefixes.len(), 1);
            assert_prefix_invariant(&del_prefix);
            for prefix in ["00001010", "0000101"] {
                let inner_prefix = &prefix.as_bytes()[inner_key_off..];
                assert!(del_prefix.cover_prefix(inner_prefix));
            }
            for prefix in ["000010", "0000103"] {
                let inner_prefix = &prefix.as_bytes()[inner_key_off..];
                assert!(!del_prefix.cover_prefix(inner_prefix));
            }

            del_prefix = del_prefix.merge("0000105".as_bytes());
            assert_prefix_invariant(&del_prefix);
            let bin = del_prefix.marshal();
            del_prefix = DeletePrefixes::unmarshal(&bin, inner_key_off);
            assert_prefix_invariant(&del_prefix);
            assert_eq!(del_prefix.prefixes.len(), 2);
            for prefix in ["00001010", "0000101", "00001050", "0000105"] {
                let inner_prefix = &prefix.as_bytes()[inner_key_off..];
                assert!(del_prefix.cover_prefix(inner_prefix));
            }
            for prefix in ["000010", "0000103"] {
                let inner_prefix = &prefix.as_bytes()[inner_key_off..];
                assert!(!del_prefix.cover_prefix(inner_prefix));
            }

            del_prefix = del_prefix.merge("000010".as_bytes());
            assert_prefix_invariant(&del_prefix);
            assert_eq!(del_prefix.prefixes.len(), 1);
            for prefix in ["000010", "0000101", "0000103", "0000104"] {
                let inner_prefix = &prefix.as_bytes()[inner_key_off..];
                assert!(del_prefix.cover_prefix(inner_prefix));
            }
            for prefix in ["00001", "000011"] {
                let inner_prefix = &prefix.as_bytes()[inner_key_off..];
                assert!(!del_prefix.cover_prefix(inner_prefix));
            }

            del_prefix = DeletePrefixes::new_with_inner_key_off(inner_key_off);
            del_prefix = del_prefix.merge("0000101".as_bytes());
            del_prefix = del_prefix.merge("0000102".as_bytes());
            assert_prefix_invariant(&del_prefix);
            for (start, end) in [("0000101", "00001011"), ("0000102", "00001022")] {
                let inner_start = &start.as_bytes()[inner_key_off..];
                let inner_end = &end.as_bytes()[inner_key_off..];
                assert!(del_prefix.cover_range(inner_start, inner_end));
            }
            for (start, end) in [
                ("000099", "0000100"),
                ("0000101", "0000102"),
                ("0000102", "0000103"),
            ] {
                let inner_start = &start.as_bytes()[inner_key_off..];
                let inner_end = &end.as_bytes()[inner_key_off..];
                assert!(!del_prefix.cover_range(inner_start, inner_end));
            }

            del_prefix = DeletePrefixes::new_with_inner_key_off(inner_key_off);
            del_prefix = del_prefix.merge("0000101".as_bytes());
            del_prefix = del_prefix.merge("00001033".as_bytes());
            del_prefix = del_prefix.merge("00001055".as_bytes());
            del_prefix = del_prefix.merge("0000107".as_bytes());
            assert_prefix_invariant(&del_prefix);

            let split_del_range =
                del_prefix.build_split("00001033".as_bytes(), "00001055".as_bytes(), inner_key_off);
            assert_prefix_invariant(&split_del_range);
            assert_eq!(split_del_range.prefixes.len(), 1);
            assert_eq!(&split_del_range.prefixes[0], "00001033".as_bytes());

            let split_del_range =
                del_prefix.build_split("00001034".as_bytes(), "00001055".as_bytes(), inner_key_off);
            assert_prefix_invariant(&split_del_range);
            assert_eq!(split_del_range.prefixes.len(), 0);

            let split_del_range = del_prefix.build_split(
                "000010334".as_bytes(),
                "000010555".as_bytes(),
                inner_key_off,
            );
            assert_prefix_invariant(&split_del_range);
            assert_eq!(split_del_range.prefixes.len(), 2);
            assert_eq!(&split_del_range.prefixes[0], "00001033".as_bytes());
            assert_eq!(&split_del_range.prefixes[1], "00001055".as_bytes());

            del_prefix = DeletePrefixes::new_with_inner_key_off(inner_key_off)
                .merge("0000100".as_bytes())
                .merge("0000200".as_bytes())
                .merge("0000300".as_bytes());
            assert_prefix_invariant(&del_prefix);
            del_prefix = del_prefix.split(
                &DeletePrefixes::new_with_inner_key_off(inner_key_off).merge("0000100".as_bytes()),
            );
            assert_prefix_invariant(&del_prefix);
            assert_eq!(del_prefix.prefixes.len(), 2);
            assert!(!del_prefix.cover_prefix(&"0000100".as_bytes()[inner_key_off..]));
            assert!(del_prefix.cover_prefix(&"0000200".as_bytes()[inner_key_off..]));
            assert!(del_prefix.cover_prefix(&"0000300".as_bytes()[inner_key_off..]));
            del_prefix = del_prefix.split(
                &DeletePrefixes::new_with_inner_key_off(inner_key_off)
                    .merge("0000200".as_bytes())
                    .merge("0000300".as_bytes()),
            );
            assert_prefix_invariant(&del_prefix);
            assert_eq!(del_prefix.prefixes.len(), 0);
            assert!(!del_prefix.cover_prefix(&"0000100".as_bytes()[inner_key_off..]));
            assert!(!del_prefix.cover_prefix(&"0000200".as_bytes()[inner_key_off..]));
            assert!(!del_prefix.cover_prefix(&"0000300".as_bytes()[inner_key_off..]));
        }
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
