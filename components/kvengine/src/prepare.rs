// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, io::Write, path::PathBuf, sync::atomic::Ordering::Relaxed};

use bytes::{Buf, Bytes};
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
    ) -> Result<ChangeSet> {
        let mut ids = HashMap::new();
        let mut cs = ChangeSet::new(cs);
        if cs.has_flush() {
            let flush = cs.get_flush();
            if flush.has_l0_create() {
                ids.insert(flush.get_l0_create().id, 0);
            }
            if flush.has_blob_create() {
                ids.insert(flush.get_blob_create().id, BLOB_LEVEL);
            }
        }
        if cs.has_compaction() {
            let comp = cs.get_compaction();
            if !is_move_down(comp) {
                for tbl in &comp.table_creates {
                    ids.insert(tbl.id, tbl.level);
                }
            }
        }
        if cs.has_destroy_range() {
            for t in cs.get_destroy_range().get_table_creates() {
                ids.insert(t.id, t.level);
            }
        }
        if cs.has_truncate_ts() {
            for t in cs.get_truncate_ts().get_table_creates() {
                ids.insert(t.id, t.level);
            }
        }
        if cs.has_trim_over_bound() {
            for t in cs.get_trim_over_bound().get_table_creates() {
                ids.insert(t.id, t.level);
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
                ids.insert(l0.id, 0);
            }
            for tbl in ingest_files.get_table_creates() {
                ids.insert(tbl.id, tbl.level);
            }
            for blob in ingest_files.get_blob_creates() {
                ids.insert(blob.id, BLOB_LEVEL);
            }
        }
        if cs.has_major_compaction() {
            let major_comp = cs.get_major_compaction();
            for tbl in major_comp.get_sstable_change().get_table_creates() {
                ids.insert(tbl.get_id(), tbl.get_level());
            }
            for blob in major_comp.get_new_blob_tables() {
                ids.insert(blob.get_id(), BLOB_LEVEL);
            }
        }
        debug!(
            "[{}:{}] is preparing change set, loading file by ids", cs.shard_id, cs.shard_ver;
            "ids" => ?ids,
        );
        self.load_tables_by_ids(cs.shard_id, cs.shard_ver, ids, &mut cs, use_direct_io)?;
        Ok(cs)
    }

    fn collect_snap_ids(&self, snap: &kvenginepb::Snapshot, ids: &mut HashMap<u64, u32>) {
        for blob in snap.get_blob_creates() {
            ids.insert(blob.id, BLOB_LEVEL);
        }
        for l0 in snap.get_l0_creates() {
            ids.insert(l0.id, 0);
        }
        for ln in snap.get_table_creates() {
            ids.insert(ln.id, ln.level);
        }
    }

    fn load_tables_by_ids(
        &self,
        shard_id: u64,
        shard_ver: u64,
        ids: HashMap<u64, u32>,
        cs: &mut ChangeSet,
        use_direct_io: bool,
    ) -> Result<()> {
        let (result_tx, result_rx) = tikv_util::mpsc::bounded(ids.len());
        let runtime = self.fs.get_runtime();
        let opts = dfs::Options::new(shard_id, shard_ver);
        let mut msg_count = 0;
        for (&id, &level) in &ids {
            if is_blob_file(level) {
                if let Ok(file) = self.open_blob_table_file(id) {
                    cs.add_file(id, file, level, self.cache.clone())?;
                    continue;
                }
            } else if let Ok(file) = self.open_sstable_file(id) {
                cs.add_file(id, file, level, self.cache.clone())?;
                continue;
            }
            let fs = self.fs.clone();
            let tx = result_tx.clone();
            runtime.spawn(async move {
                let res = fs.read_file(id, opts).await;
                let _ = tx.send(res.map(|data| (id, level, data)));
            });
            if msg_count < LOAD_FILE_CONCURRENCY {
                msg_count += 1;
            } else {
                self.recv_file_data(cs, use_direct_io, &result_rx)?;
            }
        }
        for _ in 0..msg_count {
            self.recv_file_data(cs, use_direct_io, &result_rx)?;
        }
        Ok(())
    }

    fn recv_file_data(
        &self,
        cs: &mut ChangeSet,
        use_direct_io: bool,
        result_tx: &Receiver<dfs::Result<(u64, u32, Bytes)>>,
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
        cs.add_file(id, file, level, self.cache.clone())?;
        ENGINE_LEVEL_WRITE_VEC
            .with_label_values(&[&level.to_string()])
            .inc_by(data_len as u64);
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
