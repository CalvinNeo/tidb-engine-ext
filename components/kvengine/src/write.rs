// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, collections::HashMap};

use bytes::{Buf, BytesMut};
use slog_global::info;

use crate::{
    table::{self, memtable},
    *,
};

pub struct WriteBatch {
    shard_id: u64,
    cf_batches: [memtable::WriteBatch; NUM_CFS],
    properties: HashMap<String, BytesMut>,
    sequence: u64,
    switch_mem_table: bool,
}

impl WriteBatch {
    pub fn new(shard_id: u64) -> Self {
        let cf_batches = [
            memtable::WriteBatch::new(),
            memtable::WriteBatch::new(),
            memtable::WriteBatch::new(),
        ];
        Self {
            shard_id,
            cf_batches,
            properties: HashMap::new(),
            sequence: 1,
            switch_mem_table: false,
        }
    }

    pub fn put(
        &mut self,
        cf: usize,
        key: &[u8],
        val: &[u8],
        meta: u8,
        user_meta: &[u8],
        version: u64,
    ) {
        self.validate_version(cf, version);
        self.get_cf_mut(cf).put(key, meta, user_meta, version, val);
    }

    fn validate_version(&self, cf: usize, version: u64) {
        if CF_MANAGED[cf] {
            if version == 0 {
                panic!("version is zero for managed CF")
            }
        } else if version != 0 {
            panic!("version is not zero for not managed CF")
        }
    }

    pub fn delete(&mut self, cf: usize, key: &[u8], version: u64) {
        self.validate_version(cf, version);
        self.get_cf_mut(cf)
            .put(key, table::BIT_DELETE, &[], version, &[]);
    }

    pub fn set_property(&mut self, key: &str, val: &[u8]) {
        self.properties.insert(key.to_string(), BytesMut::from(val));
    }

    pub fn set_sequence(&mut self, seq: u64) {
        self.sequence = seq;
    }

    pub fn set_switch_mem_table(&mut self) {
        self.switch_mem_table = true;
    }

    pub fn num_entries(&self) -> usize {
        let mut num = 0;
        for wb in &self.cf_batches {
            num += wb.len();
        }
        num
    }

    pub fn reset(&mut self) {
        for wb in &mut self.cf_batches {
            wb.reset();
        }
        self.sequence = 0;
        self.properties.clear();
        self.switch_mem_table = false;
    }

    pub fn get_cf_mut(&mut self, cf: usize) -> &mut memtable::WriteBatch {
        &mut self.cf_batches[cf]
    }

    pub fn cf_len(&self, cf: usize) -> usize {
        self.cf_batches[cf].len()
    }
}

impl Engine {
    pub fn switch_mem_table(&self, shard: &Shard, version: u64) {
        let data = shard.get_data();
        let mem_table = data.get_writable_mem_table();
        if mem_table.is_empty() {
            return;
        }
        mem_table.set_version(version);
        let new_tbl = memtable::CfTable::new();
        let mut new_mem_tbls = Vec::with_capacity(data.mem_tbls.len() + 1);
        new_mem_tbls.push(new_tbl);
        new_mem_tbls.extend_from_slice(data.mem_tbls.as_slice());
        let new_data = ShardData::new(
            data.range.clone(),
            data.del_prefixes.clone(),
            data.truncate_ts,
            data.trim_over_bound,
            new_mem_tbls,
            data.l0_tbls.clone(),
            data.blob_tbl_map.clone(),
            data.cfs.clone(),
        );
        shard.set_data(new_data);
        info!(
            "shard {} switch mem-table version {}, size {}",
            shard.tag(),
            version,
            mem_table.size()
        );
        let props = shard.properties.to_pb(shard.id);
        mem_table.set_properties(props);
    }

    pub fn write(&self, wb: &mut WriteBatch) -> u64 {
        let shard = self.get_shard(wb.shard_id).unwrap_or_else(|| {
            let tag = ShardTag::new(self.get_engine_id(), IdVer::new(wb.shard_id, 0));
            panic!("{} unable to get shard", tag);
        });
        if shard.inner_key_off > 0 {
            for cf in &mut wb.cf_batches {
                for entry in &mut cf.entries {
                    entry.trim_to_inner_key(shard.inner_key_off);
                }
            }
        }
        let version = shard.get_base_version() + wb.sequence;
        self.update_write_batch_version(wb, version);
        let data = shard.get_data();
        let snap = shard.new_snap_access();
        let mem_tbl = data.get_writable_mem_table();
        for cf in 0..NUM_CFS {
            mem_tbl.get_cf(cf).put_batch(wb.get_cf_mut(cf), &snap, cf);
        }
        for (k, v) in std::mem::take(&mut wb.properties) {
            if k == DEL_PREFIXES_KEY {
                shard.merge_del_prefix(v.chunk());
                let data = shard.get_data();
                if data.writable_mem_table_has_data_in_deleted_prefix() {
                    wb.set_switch_mem_table();
                }
                self.refresh_shard_states(&shard);
                shard
                    .properties
                    .set(k.as_str(), &data.del_prefixes.marshal());
            } else if k == TRUNCATE_TS_KEY {
                if shard.set_truncate_ts(v.chunk()) {
                    let data = shard.get_data();
                    if data.writable_mem_table_need_truncate_ts() {
                        wb.set_switch_mem_table();
                    }
                    self.refresh_shard_states(&shard);
                    shard.properties.set(k.as_str(), v.chunk());
                }
            } else if k == TRIM_OVER_BOUND {
                shard.set_trim_over_bound(v.chunk());
                self.refresh_shard_states(&shard);
                shard.properties.set(k.as_str(), v.chunk());
            } else {
                shard.properties.set(k.as_str(), v.chunk());
            }
        }
        store_u64(&shard.write_sequence, wb.sequence);
        let size = mem_tbl.size();
        if wb.switch_mem_table || size > self.opts.max_mem_table_size {
            self.switch_mem_table(&shard, version);
            self.trigger_flush(&shard);
            0
        } else {
            size
        }
    }

    fn update_write_batch_version(&self, wb: &mut WriteBatch, version: u64) {
        for cf in 0..NUM_CFS {
            if !CF_MANAGED[cf] {
                wb.get_cf_mut(cf).iterate(|e, _| {
                    e.version = version;
                });
            };
        }
    }

    pub fn flush_shard_for_restore(&self, shard: &Shard) {
        let ver = shard.get_base_version()
            + cmp::max(shard.get_write_sequence(), shard.get_meta_sequence())
            + 1;
        debug!(
            "{} flush_shard_for_restore, ver: {}, base_ver: {}, write_seq: {}, meta_seq: {}",
            shard.tag(),
            ver,
            shard.get_base_version(),
            shard.get_write_sequence(),
            shard.get_meta_sequence(),
        );

        self.switch_mem_table(shard, ver);
        self.set_shard_active(shard.id, true);
        self.trigger_flush(shard);
    }
}
