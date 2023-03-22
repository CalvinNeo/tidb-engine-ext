// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{max, min},
    sync::{
        atomic::{Ordering, Ordering::Release},
        Arc,
    },
};

use bytes::Buf;
use dashmap::mapref::entry::Entry;
use kvenginepb as pb;
use slog_global::info;

use crate::*;

impl Engine {
    pub fn split(&self, mut cs: pb::ChangeSet, initial_seq: u64) -> Result<()> {
        let split = cs.take_split();
        let sequence = cs.get_sequence();

        let old_shard = self.get_shard_with_ver(cs.shard_id, cs.shard_ver)?;
        self.prepare_update_shard_version(&old_shard, sequence);

        let mut new_shards = vec![];
        let new_shard_props = split.get_new_shards();
        let new_ver = old_shard.ver + new_shard_props.len() as u64 - 1;
        for i in 0..=split.keys.len() {
            let (start_key, end_key) = get_splitting_start_end(
                old_shard.start.chunk(),
                old_shard.end.chunk(),
                split.get_keys(),
                i,
            );
            let mut new_shard = Shard::new(
                self.get_engine_id(),
                &new_shard_props[i],
                new_ver,
                start_key,
                end_key,
                self.opts.clone(),
            );
            new_shard.parent_id = old_shard.id;
            {
                let mut guard = new_shard.parent_snap.write().unwrap();
                *guard = Some(cs.get_snapshot().clone());
            }
            if new_shard.id == old_shard.id {
                new_shard.set_active(old_shard.is_active());
                new_shard.base_version = old_shard.base_version;
                store_u64(&new_shard.meta_seq, sequence);
                store_u64(&new_shard.write_sequence, sequence);
            } else {
                new_shard.base_version = old_shard.base_version + sequence;
                store_u64(&new_shard.meta_seq, initial_seq);
                store_u64(&new_shard.write_sequence, initial_seq);
            }
            new_shards.push(Arc::new(new_shard));
        }
        let old_data = old_shard.get_data();
        for new_shard in &new_shards {
            let new_mem_tbls = new_shard.split_mem_tables(&old_data.mem_tbls);
            let mut new_l0s = vec![];
            for l0 in &old_data.l0_tbls {
                if new_shard.overlap_table(l0.smallest(), l0.biggest()) {
                    new_l0s.push(l0.clone());
                }
            }
            let mut new_cfs = [ShardCF::new(0), ShardCF::new(1), ShardCF::new(2)];
            for cf in 0..NUM_CFS {
                let old_scf = old_data.get_cf(cf);
                for lh in &old_scf.levels {
                    let mut new_level_tbls = vec![];
                    for tbl in lh.tables.as_slice() {
                        if new_shard.overlap_table(tbl.smallest(), tbl.biggest()) {
                            new_level_tbls.push(tbl.clone());
                        }
                    }
                    let new_level = LevelHandler::new(lh.level, new_level_tbls);
                    new_cfs[cf].set_level(new_level);
                }
            }
            let new_del_prefixes = old_data
                .del_prefixes
                .build_split(&new_shard.start, &new_shard.end);
            let new_data = ShardData::new(
                new_shard.start.clone(),
                new_shard.end.clone(),
                new_del_prefixes,
                old_data.truncate_ts, // TODO: maybe not necessary to truncate ts on the new shard.
                old_data.trim_over_bound,
                new_mem_tbls,
                new_l0s,
                new_cfs,
            );
            new_shard.set_data(new_data);
        }
        for shard in new_shards.drain(..) {
            self.refresh_shard_states(&shard);
            let id = shard.id;
            if id != old_shard.id {
                match self.shards.entry(id) {
                    Entry::Occupied(_) => {
                        // The shard already exists, it must be created by ingest, and it maybe
                        // newer than this one, we avoid insert it.
                        continue;
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(shard.clone());
                    }
                }
            } else {
                self.shards.insert(id, shard.clone());
            }
            let all_files = shard.get_all_files();
            info!(
                "split new shard {}, start {:x}, end {:x}, all files {:?}",
                shard.tag(),
                shard.start,
                shard.end,
                all_files
            );
        }
        Ok(())
    }

    pub(crate) fn prepare_update_shard_version(&self, shard: &Shard, sequence: u64) {
        shard.write_sequence.store(sequence, Release);
        let version = shard.load_mem_table_version();
        // Switch the old shard mem-table, so the first mem-table is always empty.
        // ignore the read-only mem-table to be flushed. let the new shard handle it.
        self.switch_mem_table(&shard, version);
        self.flush_tx.send(FlushMsg::Clear(shard.id)).unwrap();
        self.compact_tx
            .send(CompactMsg::Clear(IDVer::new(shard.id, shard.ver)))
            .unwrap();
    }

    pub fn check_merge(
        &self,
        source_id: u64,
        source_ver: u64,
        target_id: u64,
        target_ver: u64,
    ) -> Result<(bool /*source*/, bool /*target*/)> {
        let source_shard = self.get_shard_with_ver(source_id, source_ver)?;
        if !source_shard.get_initial_flushed() {
            return Err(Error::CheckMerge("source not initial flushed".to_string()));
        }
        let target_shard = self.get_shard_with_ver(target_id, target_ver)?;
        if !target_shard.get_initial_flushed() {
            return Err(Error::CheckMerge("target not initial flushed".to_string()));
        }
        Ok((
            source_shard.has_over_bound_data(),
            target_shard.has_over_bound_data(),
        ))
    }

    pub fn prepare_merge(
        &self,
        shard_id: u64,
        shard_ver: u64,
        parent_snap: kvenginepb::Snapshot,
        sequence: u64,
    ) {
        let old_shard = self.get_shard_with_ver(shard_id, shard_ver).unwrap();
        self.prepare_update_shard_version(&old_shard, sequence);
        let mut new_shard = self.new_shard_version(&old_shard, sequence);
        // source shard may have non-empty mem-table, we need to flush them before commit merge.
        // The initial_flushed of the new shard is false, set the parent for later initial flush.
        new_shard.parent_id = old_shard.id;
        {
            let mut guard = new_shard.parent_snap.write().unwrap();
            *guard = Some(parent_snap);
        }
        info!("{} shard prepared merge", new_shard.tag());
        self.shards.insert(new_shard.id, Arc::new(new_shard));
    }

    pub fn rollback_merge(&self, shard_id: u64, shard_ver: u64, sequence: u64) {
        let old_shard = self.get_shard_with_ver(shard_id, shard_ver).unwrap();
        self.prepare_update_shard_version(&old_shard, sequence);
        let new_shard = self.new_shard_version(&old_shard, sequence);
        // There is no write during merging state, so we can directly set initial_flushed to true.
        new_shard.initial_flushed.store(true, Ordering::Release);
        info!("{} shard rollback merge", new_shard.tag());
        self.shards.insert(new_shard.id, Arc::new(new_shard));
    }

    pub fn commit_merge(
        &self,
        shard_id: u64,
        shard_ver: u64,
        parent_snap: kvenginepb::Snapshot,
        source: &ChangeSet,
        sequence: u64,
    ) -> Result<()> {
        let old_shard = self.get_shard_with_ver(shard_id, shard_ver)?;
        self.prepare_update_shard_version(&old_shard, sequence);
        let mut new_shard = self.new_shard_version(&old_shard, sequence);
        let source_snap = source.get_snapshot();
        new_shard.start = min(old_shard.start.clone(), source_snap.start.clone().into());
        new_shard.end = max(old_shard.end.clone(), source_snap.end.clone().into());
        new_shard.ver = max(shard_ver, source.shard_ver) + 1;
        // make sure the new mem-table version is greater than source.
        let source_mem_tbl_version = source_snap.base_version + source.sequence;
        let target_mem_tbl_version = old_shard.base_version + sequence;
        new_shard.base_version = max(source_mem_tbl_version, target_mem_tbl_version) - sequence;
        let old_data = old_shard.get_data();
        let mem_tbls = old_data.mem_tbls.clone();
        let mut l0_tbls = old_data.l0_tbls.clone();
        for (_, l0) in &source.l0_tables {
            l0_tbls.push(l0.clone())
        }
        l0_tbls.sort_by(|a, b| b.version().cmp(&a.version()));
        let mut new_cf_builders = [
            ShardCFBuilder::new(0),
            ShardCFBuilder::new(1),
            ShardCFBuilder::new(2),
        ];
        for cf in 0..NUM_CFS {
            let old_scf = old_data.get_cf(cf);
            for level in 1..=CF_LEVELS[cf] {
                let old_level = old_scf.get_level(level);
                let cf_builder = &mut new_cf_builders[cf];
                for tbl in old_level.tables.as_slice() {
                    cf_builder.add_table(tbl.clone(), level);
                }
            }
        }
        for tbl_create in source_snap.get_table_creates() {
            let tbl = source.ln_tables.get(&tbl_create.id).unwrap().clone();
            let cf_builder = &mut new_cf_builders[tbl_create.cf as usize];
            cf_builder.add_table(tbl, tbl_create.level as usize);
        }
        let new_cfs = [
            new_cf_builders[0].build(),
            new_cf_builders[1].build(),
            new_cf_builders[2].build(),
        ];
        let data = ShardData::new(
            new_shard.start.clone(),
            new_shard.end.clone(),
            old_data.del_prefixes.clone(),
            old_data.truncate_ts,
            old_data.trim_over_bound,
            mem_tbls,
            l0_tbls,
            new_cfs,
        );
        new_shard.set_data(data);
        new_shard.parent_id = shard_id;
        {
            let mut guard = new_shard.parent_snap.write().unwrap();
            *guard = Some(parent_snap);
        }
        let all_files = new_shard.get_all_files();
        info!(
            "merged new shard {}, start {:x}, end {:x}, all files {:?}",
            new_shard.tag(),
            new_shard.start,
            new_shard.end,
            all_files
        );
        self.refresh_shard_states(&new_shard);
        self.shards.insert(shard_id, Arc::new(new_shard));
        Ok(())
    }

    pub(crate) fn new_shard_version(&self, old_shard: &Shard, sequence: u64) -> Shard {
        let engine_id = self.get_engine_id();
        let mut new_shard = Shard::new(
            engine_id,
            &old_shard.properties.to_pb(old_shard.id),
            old_shard.ver + 1,
            &old_shard.start,
            &old_shard.end,
            old_shard.opt.clone(),
        );
        new_shard.set_data(old_shard.get_data());
        new_shard.set_active(old_shard.is_active());
        new_shard.base_version = old_shard.base_version;
        store_u64(&new_shard.meta_seq, sequence);
        store_u64(&new_shard.write_sequence, sequence);
        new_shard
    }
}

pub fn get_split_shard_index(split_keys: &[Vec<u8>], key: &[u8]) -> usize {
    for i in 0..split_keys.len() {
        if key < split_keys[i].as_slice() {
            return i;
        }
    }
    split_keys.len()
}
