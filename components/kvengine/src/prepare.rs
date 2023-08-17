// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    io::Write,
    iter::Iterator,
    path::PathBuf,
    sync::{atomic::Ordering::Relaxed, Arc},
};

use bytes::{Buf, Bytes};
use cloud_encryption::EncryptionKey;
use file_system::{IoOp, IoType};
use tikv_util::{mpsc::Receiver, time::Instant};

use crate::{
    apply::ChangeSet, metrics::ENGINE_LEVEL_WRITE_VEC, table::sstable::LocalFile, EngineCore, *,
};

pub const BLOB_LEVEL: u32 = 1 << 31;
pub const LOAD_FILE_CONCURRENCY: usize = 8;

pub fn is_blob_file(flags: u32) -> bool {
    flags == BLOB_LEVEL
}

impl EngineCore {
    pub fn prepare_change_set(
        &self,
        cs: kvenginepb::ChangeSet,
        use_direct_io: bool,
        table_filter: Option<LoadTableFilterFn>,
    ) -> Result<ChangeSet> {
        let mut ids: HashMap<u64, FileMeta> = HashMap::new();
        let mut cs = ChangeSet::new(cs);
        if cs.has_flush() {
            let flush = cs.get_flush();
            if flush.has_l0_create() {
                ids.insert(
                    flush.get_l0_create().id,
                    FileMeta::from_l0_table(flush.get_l0_create()),
                );
            }
        }
        if cs.has_compaction() {
            let comp = cs.get_compaction();
            if !is_move_down(comp) {
                for tbl in &comp.table_creates {
                    ids.insert(tbl.id, FileMeta::from_table(tbl));
                }
                for bt in comp.get_blob_tables() {
                    ids.insert(bt.get_id(), FileMeta::from_blob_table(bt));
                }
            }
        }
        if cs.has_destroy_range() {
            for t in cs.get_destroy_range().get_table_creates() {
                ids.insert(t.id, FileMeta::from_table(t));
            }
        }
        if cs.has_truncate_ts() {
            for t in cs.get_truncate_ts().get_table_creates() {
                ids.insert(t.id, FileMeta::from_table(t));
            }
        }
        if cs.has_trim_over_bound() {
            for t in cs.get_trim_over_bound().get_table_creates() {
                ids.insert(t.id, FileMeta::from_table(t));
            }
        }
        if cs.has_snapshot() {
            self.collect_snap_ids(cs.get_snapshot(), &mut ids);
        }
        if cs.has_initial_flush() {
            self.collect_snap_ids(cs.get_initial_flush(), &mut ids);
        }
        if cs.has_restore_shard() {
            self.collect_snap_ids(cs.get_restore_shard(), &mut ids);
        }
        if cs.has_ingest_files() {
            let ingest_files = cs.get_ingest_files();
            for l0 in ingest_files.get_l0_creates() {
                ids.insert(l0.id, FileMeta::from_l0_table(l0));
            }
            for tbl in ingest_files.get_table_creates() {
                ids.insert(tbl.id, FileMeta::from_table(tbl));
            }
            for blob in ingest_files.get_blob_creates() {
                ids.insert(blob.id, FileMeta::from_blob_table(blob));
            }
        }
        if cs.has_major_compaction() {
            let major_comp = cs.get_major_compaction();
            for tbl in major_comp.get_sstable_change().get_table_creates() {
                ids.insert(tbl.get_id(), FileMeta::from_table(tbl));
            }
            for blob in major_comp.get_new_blob_tables() {
                ids.insert(blob.get_id(), FileMeta::from_blob_table(blob));
            }
        }

        if let Some(table_filter) = table_filter {
            info!(
                "[{}:{}] is preparing change set (before table filter)", cs.shard_id, cs.shard_ver;
                "ids" => ?ids.keys(),
            );
            cs.unloaded_tables = ids
                .drain_filter(|_, tb| !table_filter(cs.shard_id, tb)) // !table_filter: table will not load
                .collect();
        }

        info!(
            "[{}:{}] is preparing change set, loading file by ids", cs.shard_id, cs.shard_ver;
            "ids" => ?ids.keys(),
        );
        let encryption_key = if cs.has_snapshot() {
            get_shard_property(ENCRYPTION_KEY, cs.get_snapshot().get_properties())
                .map(|v| self.master_key.decrypt_encryption_key(&v).unwrap())
        } else {
            match self.get_shard(cs.shard_id) {
                Some(shard) => shard.encryption_key.clone(),
                None => {
                    // If shard not exists it means the peer has been destroyed, return the empty
                    // changeset.
                    return Ok(cs);
                }
            }
        };
        self.load_tables_by_ids(
            cs.shard_id,
            cs.shard_ver,
            &ids,
            &mut cs,
            use_direct_io,
            encryption_key,
        )?;
        Ok(cs)
    }

    fn collect_snap_ids(&self, snap: &kvenginepb::Snapshot, ids: &mut HashMap<u64, FileMeta>) {
        for blob in snap.get_blob_creates() {
            ids.insert(blob.id, FileMeta::from_blob_table(blob));
        }
        for l0 in snap.get_l0_creates() {
            ids.insert(l0.id, FileMeta::from_l0_table(l0));
        }
        for ln in snap.get_table_creates() {
            ids.insert(ln.id, FileMeta::from_table(ln));
        }
    }

    fn load_tables_by_ids(
        &self,
        shard_id: u64,
        shard_ver: u64,
        ids: &HashMap<u64, FileMeta>,
        cs: &mut ChangeSet,
        use_direct_io: bool,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<()> {
        let (result_tx, result_rx) = tikv_util::mpsc::bounded(ids.len());
        let runtime = self.fs.get_runtime();
        let opts = dfs::Options::new(shard_id, shard_ver);
        let mut msg_count = 0;
        for (&id, tb) in ids {
            if tb.is_blob_file() {
                if let Ok(file) = self.open_blob_table_file(id) {
                    cs.add_file(
                        id,
                        file,
                        tb.get_level(),
                        self.cache.clone(),
                        encryption_key.clone(),
                    )?;
                    continue;
                }
            } else if let Ok(file) = self.open_sstable_file(id) {
                cs.add_file(
                    id,
                    file,
                    tb.get_level(),
                    self.cache.clone(),
                    encryption_key.clone(),
                )?;
                continue;
            }
            let level = tb.get_level();
            let fs = self.fs.clone();
            let tx = result_tx.clone();
            runtime.spawn(async move {
                let res = fs.read_file(id, opts).await;
                let _ = tx.send(res.map(|data| (id, level, data)));
            });
            if msg_count < LOAD_FILE_CONCURRENCY {
                msg_count += 1;
            } else {
                self.recv_file_data(cs, use_direct_io, &result_rx, encryption_key.clone())?;
            }
        }
        for _ in 0..msg_count {
            self.recv_file_data(cs, use_direct_io, &result_rx, encryption_key.clone())?;
        }
        Ok(())
    }

    fn recv_file_data(
        &self,
        cs: &mut ChangeSet,
        use_direct_io: bool,
        result_tx: &Receiver<dfs::Result<(u64, u32, Bytes)>>,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<()> {
        let (id, level, data) = result_tx.recv().unwrap()?;
        let data_len = data.len();
        let start = Instant::now();
        self.write_local_file(id, data, use_direct_io, is_blob_file(level))?;
        info!(
            "write local file {} takes {:?}",
            id,
            start.saturating_elapsed()
        );
        let file = if is_blob_file(level) {
            self.open_blob_table_file(id)?
        } else {
            self.open_sstable_file(id)?
        };
        cs.add_file(id, file, level, self.cache.clone(), encryption_key)?;
        ENGINE_LEVEL_WRITE_VEC
            .with_label_values(&[&level.to_string()])
            .inc_by(data_len as u64);
        Ok(())
    }

    pub fn load_unloaded_tables(
        &self,
        shard_id: u64,
        shard_ver: u64,
        use_direct_io: bool,
    ) -> Result<()> {
        let shard = self.shards.get(&shard_id).unwrap();
        let mut cs = ChangeSet::new(kvenginepb::ChangeSet::default());
        let data = shard.get_data();

        let load_tables = data
            .unloaded_tbls
            .iter()
            .filter(|&(_, tbl)| shard.overlap_table(tbl.smallest(), tbl.biggest()))
            .map(|(id, tbl)| (*id, tbl.clone()))
            .collect();
        info!("{} load_unloaded_tables: {:?}", shard.tag(), load_tables);

        self.load_tables_by_ids(
            shard_id,
            shard_ver,
            &load_tables,
            &mut cs,
            use_direct_io,
            shard.encryption_key.clone(),
        )?;

        // level 0
        let mut new_l0s = data.l0_tbls.clone();
        for (_, tbl) in cs.l0_tables.drain() {
            new_l0s.push(tbl);
        }
        new_l0s.sort_by(|a, b| b.version().cmp(&a.version()));

        // blob
        let mut new_blob_tbl_map = data.blob_tbl_map.as_ref().clone();
        for (id, tbl) in cs.blob_tables.drain() {
            new_blob_tbl_map.insert(id, tbl);
        }

        // level n
        let mut scf_builders = vec![];
        for cf in 0..NUM_CFS {
            let scf = ShardCfBuilder::new(cf);
            scf_builders.push(scf);
        }
        for (cf, shard_cf) in data.cfs.iter().enumerate() {
            for lh in &shard_cf.levels {
                for sst in &(*lh.tables) {
                    let scf = &mut scf_builders.as_mut_slice()[cf];
                    scf.add_table(sst.clone(), lh.level);
                }
            }
        }
        for (id, tbl) in load_tables
            .into_iter()
            .filter(|(_, tbl)| tbl.get_level() > 0 && !tbl.is_blob_file())
        {
            let sst = cs.ln_tables.get(&id).unwrap();
            let scf = &mut scf_builders.as_mut_slice()[tbl.get_cf() as usize];
            scf.add_table(sst.clone(), tbl.get_level() as usize);
        }
        let mut scfs = [ShardCf::new(0), ShardCf::new(1), ShardCf::new(2)];
        for cf in 0..NUM_CFS {
            let scf = &mut scf_builders.as_mut_slice()[cf];
            scfs[cf] = scf.build();
        }

        let new_data = ShardData::new(
            data.range.clone(),
            data.mem_tbls.clone(),
            new_l0s,
            Arc::new(new_blob_tbl_map),
            scfs,
            HashMap::new(),
        );
        shard.set_data(new_data);
        Ok(())
    }

    fn write_local_file(
        &self,
        id: u64,
        data: Bytes,
        use_direct_io: bool,
        blob_file: bool,
    ) -> std::io::Result<()> {
        let local_file_name = if blob_file {
            self.local_blob_file_path(id)
        } else {
            self.local_sst_file_path(id)
        };

        let tmp_file_name = self.tmp_file_path(id);
        if use_direct_io {
            let mut writer =
                file_system::DirectWriter::new(self.rate_limiter.clone(), IoType::Compaction);
            writer.write_to_file(data.chunk(), &tmp_file_name)?;
        } else {
            let mut file = std::fs::File::create(&tmp_file_name)?;
            let mut start_off = 0;
            let write_batch_size = 256 * 1024;
            while start_off < data.len() {
                self.rate_limiter
                    .request(IoType::Compaction, IoOp::Write, write_batch_size);
                let end_off = std::cmp::min(start_off + write_batch_size, data.len());
                file.write_all(&data[start_off..end_off])?;
                start_off = end_off;
            }
            file.sync_data()?;
        }
        std::fs::rename(&tmp_file_name, local_file_name)
    }

    fn open_sstable_file(&self, id: u64) -> Result<LocalFile> {
        let _guard = self.lock_file(id);
        Ok(LocalFile::open(
            id,
            self.local_sst_file_path(id).as_path(),
            self.loaded.load(Relaxed),
        )?)
    }

    fn open_blob_table_file(&self, id: u64) -> Result<LocalFile> {
        let _guard = self.lock_file(id);
        Ok(LocalFile::open(
            id,
            self.local_blob_file_path(id).as_path(),
            self.loaded.load(Relaxed),
        )?)
    }

    pub(crate) fn local_sst_file_path(&self, file_id: u64) -> PathBuf {
        self.opts.local_dir.join(new_sst_filename(file_id))
    }

    pub(crate) fn local_blob_file_path(&self, file_id: u64) -> PathBuf {
        self.opts.local_dir.join(new_blob_filename(file_id))
    }

    fn tmp_file_path(&self, file_id: u64) -> PathBuf {
        let tmp_id = self
            .tmp_file_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.opts.local_dir.join(new_tmp_filename(file_id, tmp_id))
    }
}
