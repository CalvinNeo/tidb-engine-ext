// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;

use bytes::Bytes;

use crate::{load_bool, metrics::ENGINE_OPEN_FILES, IdVer, EXTRA_CF, NUM_CFS, WRITE_CF};

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct EngineStats {
    pub num_shards: usize,
    pub num_initial_flushed_shard: usize,
    pub num_active_shards: usize,
    pub num_compacting_shards: usize,
    pub num_has_del_prefixes_shards: usize,
    pub num_inner_key_shards: usize,
    pub ready_destroy_range_shards: Vec<IdVer>,
    pub mem_tables_count: usize,
    pub mem_tables_size: u64,
    pub l0_tables_count: usize,
    pub l0_tables_size: u64,
    pub blob_tables_count: usize,
    pub in_use_blob_size: u64,
    pub total_blob_size: u64,
    pub partial_l0_count: usize,
    pub partial_blob_count: usize,
    pub partial_ln_count: usize,
    pub cfs_num_files: Vec<usize>,
    pub cf_total_sizes: Vec<u64>,
    pub level_num_files: Vec<usize>,
    pub level_total_sizes: Vec<u64>,
    pub index_size: u64,
    pub in_mem_index_size: u64,
    pub filter_size: u64,
    pub in_mem_filter_size: u64,
    pub open_files: i64,
    pub max_ts: u64, // Use to check whether PiTR has completed.
    pub entries: usize,
    pub old_entries: usize,
    pub tombs: usize,
    pub kv_size: u64,
    pub top_10_write: Vec<ShardStats>,
}

impl EngineStats {
    pub fn new() -> Self {
        let mut stats = EngineStats::default();
        stats.cfs_num_files = vec![0; 3];
        stats.cf_total_sizes = vec![0; 3];
        stats.level_num_files = vec![0; 3];
        stats.level_total_sizes = vec![0; 3];
        stats
    }
}

impl super::Engine {
    pub fn get_all_shard_stats(&self) -> Vec<ShardStats> {
        self.get_all_shard_id_vers()
            .into_iter()
            .filter_map(|id_ver| self.get_shard(id_ver.id).map(|shard| shard.get_stats()))
            .collect()
    }

    pub fn get_shard_stat(&self, region_id: u64) -> ShardStats {
        self.shards
            .get(&region_id)
            .map_or(ShardStats::default(), |shard| shard.get_stats())
    }

    pub fn get_engine_stats(mut shard_stats: Vec<ShardStats>) -> EngineStats {
        let mut engine_stats = EngineStats::new();
        engine_stats.num_shards = shard_stats.len();
        for shard in &shard_stats {
            if shard.active {
                engine_stats.num_active_shards += 1;
            }
            if shard.compacting {
                engine_stats.num_compacting_shards += 1;
            }
            if shard.flushed {
                engine_stats.num_initial_flushed_shard += 1;
            }
            if shard.has_del_prefixes {
                engine_stats.num_has_del_prefixes_shards += 1;
            }
            if shard.ready_to_destroy_range {
                engine_stats
                    .ready_destroy_range_shards
                    .push(IdVer::new(shard.id, shard.ver));
            }
            if shard.inner_key_off > 0 {
                engine_stats.num_inner_key_shards += 1;
            }
            engine_stats.mem_tables_count += shard.mem_table_count;
            engine_stats.mem_tables_size += shard.mem_table_size;
            engine_stats.l0_tables_count += shard.l0_table_count;
            engine_stats.l0_tables_size += shard.l0_table_size;
            engine_stats.blob_tables_count += shard.blob_table_count;
            engine_stats.in_use_blob_size += shard.in_use_blob_size;
            engine_stats.total_blob_size += shard.total_blob_size;
            engine_stats.partial_l0_count += shard.partial_l0s;
            engine_stats.partial_blob_count += shard.shared_blob_tables;
            engine_stats.partial_ln_count += shard.partial_tbls;
            engine_stats.index_size += shard.index_size;
            engine_stats.in_mem_index_size += shard.in_mem_index_size;
            engine_stats.filter_size += shard.filter_size;
            engine_stats.in_mem_filter_size += shard.in_mem_filter_size;
            engine_stats.max_ts = cmp::max(engine_stats.max_ts, shard.max_ts);
            engine_stats.entries += shard.entries;
            engine_stats.old_entries += shard.old_entries;
            engine_stats.tombs += shard.tombs;
            engine_stats.kv_size += shard.kv_size;
            engine_stats.open_files += shard.open_files;
            for cf in 0..NUM_CFS {
                let shard_cf_stat = &shard.cfs[cf];
                for (i, level_stat) in shard_cf_stat.levels.iter().enumerate() {
                    engine_stats.level_num_files[i] += level_stat.num_tables;
                    engine_stats.cfs_num_files[cf] += level_stat.num_tables;
                    engine_stats.level_total_sizes[i] += level_stat.data_size;
                    engine_stats.cf_total_sizes[cf] += level_stat.data_size;
                }
            }
        }
        ENGINE_OPEN_FILES.set(engine_stats.open_files);
        shard_stats.sort_by(|a, b| {
            let a_size = a.mem_table_size + a.l0_table_size;
            let b_size = b.mem_table_size + b.l0_table_size;
            b_size.cmp(&a_size)
        });
        shard_stats.truncate(10);
        engine_stats.top_10_write = shard_stats;
        engine_stats
    }
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ShardStats {
    pub id: u64,
    pub ver: u64,
    pub start: Bytes,
    pub end: Bytes,
    pub inner_key_off: usize,
    pub active: bool,
    pub compacting: bool,
    pub flushed: bool,
    pub mem_table_count: usize,
    pub mem_table_size: u64,
    pub l0_table_count: usize,
    pub blob_table_count: usize,
    pub in_use_blob_size: u64,
    pub total_blob_size: u64,

    pub l0_table_size: u64,
    pub cfs: Vec<CfStats>,

    pub index_size: u64,
    pub in_mem_index_size: u64,
    pub filter_size: u64,
    pub in_mem_filter_size: u64,
    pub max_ts: u64,
    pub entries: usize,
    pub old_entries: usize,
    pub tombs: usize,
    pub kv_size: u64,
    pub open_files: i64,
    pub base_version: u64,
    pub meta_sequence: u64,
    pub write_sequence: u64,
    // Total size of all SST files and blobs referenced by the shard (not blob table file size).
    pub total_size: u64,
    pub partial_l0s: usize,
    pub shared_blob_tables: usize,
    pub partial_tbls: usize,
    pub compaction_cf: isize,
    pub compaction_level: isize,
    pub compaction_score: f64,
    pub has_over_bound_data: bool,
    pub has_del_prefixes: bool,
    pub ready_to_destroy_range: bool,
    pub truncate_ts: Option<u64>,
    pub trim_over_bound: bool,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CfStats {
    pub levels: Vec<LevelStats>,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct LevelStats {
    pub level: usize,
    pub num_tables: usize,
    pub data_size: u64,
    pub index_size: u64,
    pub in_mem_index_size: u64,
    pub filter_size: u64,
    pub in_mem_filter_size: u64,
    pub max_ts: u64,
    pub entries: usize,
    pub old_entries: usize,
    pub tombs: usize,
    pub kv_size: u64,
    pub in_use_blob_size: u64,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ShardTruncateTsStats {
    pub id: u64,
    pub ver: u64,
    pub cur_max_ts: u64,
    pub truncate_ts: u64,
}

impl super::Shard {
    pub fn get_stats(&self) -> ShardStats {
        let mut total_size = 0;
        let mut tbl_index_size = 0;
        let mut in_mem_tbl_index_size = 0;
        let mut tbl_filter_size = 0;
        let mut in_mem_tbl_filter_size = 0;
        let mut max_ts = 0;
        let mut entries = 0;
        let mut old_entries = 0;
        let mut tombs = 0;
        let mut kv_size = 0;
        let mut open_files = 0;
        let data = self.get_data();
        let mem_table_count = data.mem_tbls.len();
        let mut mem_table_size = 0;
        for mem_tbl in data.mem_tbls.as_slice() {
            mem_table_size += mem_tbl.size();
            max_ts = cmp::max(max_ts, mem_tbl.data_max_ts());
        }
        total_size += mem_table_size;
        let mut partial_l0s = 0;
        let mut shared_blob_tables = 0;
        let l0_table_count = data.l0_tbls.len();
        let mut l0_table_size = 0;
        let mut total_blob_size = 0;
        let mut in_use_blob_size = 0;
        let blob_table_count = data.blob_tbl_map.len();
        // FIXME: Calculate the total size of blob files.
        let mut blob_table_size = 0;
        for v in data.blob_tbl_map.values() {
            if self.cover_full_table(v.smallest_key(), v.biggest_key()) {
                blob_table_size += v.size();
            } else {
                blob_table_size += v.size() / 2;
                shared_blob_tables += 1;
            }
        }
        total_blob_size += blob_table_size;
        for l0_tbl in data.l0_tbls.as_slice() {
            if self.cover_full_table(l0_tbl.smallest(), l0_tbl.biggest()) {
                l0_table_size += l0_tbl.size();
            } else {
                l0_table_size += l0_tbl.size() / 2;
                partial_l0s += 1;
            }
            for cf in 0..NUM_CFS {
                if let Some(cf_tbl) = l0_tbl.get_cf(cf) {
                    tbl_index_size += cf_tbl.index_size();
                    tbl_filter_size += cf_tbl.filter_size();
                    in_mem_tbl_filter_size += cf_tbl.in_mem_filter_size();
                    max_ts = max_ts_by_cf(max_ts, cf, cf_tbl.max_ts);
                    entries += cf_tbl.entries as usize;
                    old_entries += cf_tbl.old_entries as usize;
                    tombs += cf_tbl.tombs as usize;
                    if cf == WRITE_CF {
                        kv_size += cf_tbl.kv_size;
                        cf_tbl.expire_cache(0);
                        if cf_tbl.has_open_file() {
                            open_files += 1;
                        }
                    }
                    in_use_blob_size += cf_tbl.total_blob_size();
                }
            }
        }
        total_size += l0_table_size;
        let mut partial_tbls = 0;
        let mut cfs = vec![];
        for cf in 0..NUM_CFS {
            let scf = data.get_cf(cf);
            let mut cf_stat = CfStats { levels: vec![] };
            for l in scf.levels.as_slice() {
                let mut level_stats = LevelStats::default();
                level_stats.level = l.level;
                level_stats.num_tables = l.tables.len();
                for t in l.tables.as_slice() {
                    t.expire_cache(l.level);
                    if t.has_open_file() {
                        open_files += 1;
                    }
                    if data.cover_full_table(t.smallest(), t.biggest()) {
                        level_stats.data_size += t.size();
                        level_stats.index_size += t.index_size();
                        level_stats.in_mem_index_size += t.in_mem_index_size();
                        level_stats.filter_size += t.filter_size();
                        level_stats.in_mem_filter_size += t.in_mem_filter_size();
                        level_stats.entries += t.entries as usize;
                        level_stats.old_entries += t.old_entries as usize;
                        level_stats.tombs += t.tombs as usize;
                        if cf == WRITE_CF {
                            level_stats.kv_size += t.kv_size;
                        }
                        level_stats.in_use_blob_size += t.total_blob_size();
                    } else {
                        level_stats.data_size += t.size() / 2;
                        level_stats.index_size += t.index_size() / 2;
                        level_stats.filter_size += t.filter_size() / 2;
                        level_stats.entries += t.entries as usize / 2;
                        level_stats.old_entries += t.old_entries as usize / 2;
                        level_stats.tombs += t.tombs as usize / 2;
                        if cf == WRITE_CF {
                            level_stats.kv_size += t.kv_size / 2;
                        }
                        partial_tbls += 1;
                        level_stats.in_use_blob_size += t.total_blob_size() / 2;
                    }
                    level_stats.max_ts = max_ts_by_cf(level_stats.max_ts, cf, t.max_ts);
                }
                total_size += level_stats.data_size;
                tbl_index_size += level_stats.index_size;
                in_mem_tbl_index_size += level_stats.in_mem_index_size;
                tbl_filter_size += level_stats.filter_size;
                in_mem_tbl_filter_size += level_stats.in_mem_filter_size;
                max_ts = cmp::max(max_ts, level_stats.max_ts);
                entries += level_stats.entries;
                old_entries += level_stats.old_entries;
                tombs += level_stats.tombs;
                kv_size += level_stats.kv_size;
                in_use_blob_size += level_stats.in_use_blob_size;
                cf_stat.levels.push(level_stats);
            }
            cfs.push(cf_stat);
        }
        total_size += in_use_blob_size;
        let priority = self.compaction_priority.read().unwrap().clone();
        let compaction_cf = priority.as_ref().map_or(0, |x| x.cf());
        let compaction_level = priority.as_ref().map_or(0, |x| x.level());
        let compaction_score = priority.as_ref().map_or(0f64, |x| x.score());
        ShardStats {
            id: self.id,
            ver: self.ver,
            start: self.outer_start.clone(),
            end: self.outer_end.clone(),
            inner_key_off: self.inner_key_off,
            active: self.is_active(),
            compacting: load_bool(&self.compacting),
            flushed: self.get_initial_flushed(),
            mem_table_count,
            mem_table_size,
            l0_table_count,
            blob_table_count,
            l0_table_size,
            total_blob_size,
            in_use_blob_size,
            cfs,
            base_version: self.get_base_version(),
            meta_sequence: self.get_meta_sequence(),
            write_sequence: self.get_write_sequence(),
            total_size,
            index_size: tbl_index_size,
            in_mem_index_size: in_mem_tbl_index_size,
            filter_size: tbl_filter_size,
            in_mem_filter_size: in_mem_tbl_filter_size,
            max_ts,
            entries,
            old_entries,
            tombs,
            kv_size,
            open_files,
            partial_l0s,
            shared_blob_tables,
            partial_tbls,
            compaction_cf,
            compaction_level,
            compaction_score,
            has_over_bound_data: data.has_over_bound_data(),
            has_del_prefixes: !data.del_prefixes.is_empty(),
            ready_to_destroy_range: data.ready_to_destroy_range(),
            truncate_ts: data.truncate_ts.map(|x| x.inner()),
            trim_over_bound: data.trim_over_bound,
        }
    }
}

#[inline]
pub fn max_ts_by_cf(max_ts: u64, cf: usize, cf_max_ts: u64) -> u64 {
    // Ignore LOCK_CF, as `ts` in LOCK_CF is not a TSO.
    if (cf == WRITE_CF || cf == EXTRA_CF) && max_ts < cf_max_ts {
        cf_max_ts
    } else {
        max_ts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LOCK_CF;

    #[test]
    fn test_max_ts_by_cf() {
        let cases = vec![
            (100, WRITE_CF, 200, 200),
            (100, LOCK_CF, 200, 100),
            (100, EXTRA_CF, 200, 200),
            (200, WRITE_CF, 100, 200),
            (200, LOCK_CF, 100, 200),
            (200, EXTRA_CF, 100, 200),
        ];
        for (max_ts, cf, cf_max_ts, expected) in cases {
            assert_eq!(max_ts_by_cf(max_ts, cf, cf_max_ts), expected,);
        }
    }
}
