// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    env,
    fmt::{Debug, Display, Formatter},
    iter::{FromIterator, Iterator},
    ops::Deref,
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    thread,
    time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::{mapref::entry::Entry, DashMap};
use file_system::IoRateLimiter;
use fslock;
use moka::sync::SegmentedCache;
use slog_global::info;
use tikv_util::{mpsc, sys::thread::StdThreadBuildWrapper};

use crate::{
    apply::ChangeSet,
    meta::ShardMeta,
    table::{
        memtable::CfTable,
        sstable::{BlockCacheKey, MAGIC_NUMBER, ZSTD_COMPRESSION},
    },
    *,
};

#[derive(Clone)]
pub struct Engine {
    pub core: Arc<EngineCore>,
    pub(crate) meta_change_listener: Box<dyn MetaChangeListener>,
}

impl Deref for Engine {
    type Target = EngineCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl Debug for Engine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // TODO: complete debug info.
        let cnt = self.shards.len();
        let str = format!("num_shards: {}", cnt);
        f.write_str(&str)
    }
}

const FILE_LOCK_SLOTS: usize = 1024;

impl Engine {
    pub fn open(
        fs: Arc<dyn dfs::Dfs>,
        opts: Arc<Options>,
        meta_iter: &mut impl MetaIterator,
        recoverer: impl RecoverHandler + 'static,
        id_allocator: Arc<dyn IdAllocator>,
        meta_change_listener: Box<dyn MetaChangeListener>,
        rate_limiter: Arc<IoRateLimiter>,
    ) -> Result<Engine> {
        info!("open KVEngine");
        if !opts.local_dir.exists() {
            std::fs::create_dir_all(&opts.local_dir).unwrap();
        }
        if !opts.local_dir.is_dir() {
            panic!("path {:?} is not dir", &opts.local_dir);
        }
        let lock_path = opts.local_dir.join("LOCK");
        let mut x = fslock::LockFile::open(&lock_path)?;
        x.lock()?;
        let mut max_capacity = opts.max_block_cache_size as usize;
        if max_capacity < 512 * opts.table_builder_options.block_size {
            max_capacity = 512 * opts.table_builder_options.block_size;
        }
        let cache: SegmentedCache<BlockCacheKey, Bytes> = SegmentedCache::builder(256)
            .weigher(|_k: &BlockCacheKey, v: &Bytes| (12 + v.len()) as u32)
            .max_capacity(max_capacity as u64)
            .build();
        let (flush_tx, flush_rx) = mpsc::unbounded();
        let (compact_tx, compact_rx) = mpsc::unbounded();
        let (free_tx, free_rx) = mpsc::unbounded();
        let compression_lvl = opts.table_builder_options.compression_lvl;
        let allow_fallback_local = opts.allow_fallback_local;
        let file_locks = (0..FILE_LOCK_SLOTS).map(|_| Mutex::new(())).collect();
        let core = EngineCore {
            engine_id: AtomicU64::new(meta_iter.engine_id()),
            shards: DashMap::new(),
            opts: opts.clone(),
            flush_tx,
            compact_tx,
            fs: fs.clone(),
            cache,
            comp_client: CompactionClient::new(
                fs.clone(),
                opts.remote_compactor_addr.clone(),
                compression_lvl,
                allow_fallback_local,
            ),
            id_allocator,
            managed_safe_ts: AtomicU64::new(0),
            tmp_file_id: AtomicU64::new(0),
            rate_limiter,
            free_tx,
            loaded: AtomicBool::new(false),
            file_locks,
            shutting_down: AtomicBool::new(false),
        };
        let en = Engine {
            core: Arc::new(core),
            meta_change_listener,
        };
        let metas = en.read_meta(meta_iter)?;
        info!("engine load {} shards", metas.len());
        en.load_shards(metas, recoverer)?;
        en.loaded.store(true, Ordering::Relaxed);
        let flush_en = en.clone();
        thread::Builder::new()
            .name("flush".to_string())
            .spawn_wrapper(move || {
                flush_en.run_flush_worker(flush_rx);
            })
            .unwrap();
        let compact_en = en.clone();
        thread::Builder::new()
            .name("compaction".to_string())
            .spawn_wrapper(move || {
                compact_en.run_compaction(compact_rx);
            })
            .unwrap();
        thread::Builder::new()
            .name("free_mem".to_string())
            .spawn_wrapper(move || {
                free_mem(free_rx);
            })
            .unwrap();
        Ok(en)
    }

    // This method is also used by native-br for cluster restore.
    pub fn load_shards(
        &self,
        metas: HashMap<u64, ShardMeta>,
        recoverer: impl RecoverHandler + 'static,
    ) -> Result<()> {
        let mut parents = HashMap::new();
        for meta in metas.values() {
            if let Some(parent) = &meta.parent {
                let id_ver = IdVer::new(parent.id, parent.ver);
                if !parents.contains_key(&id_ver) {
                    info!("load parent of {}", meta.tag());
                    tikv_util::set_current_region(id_ver.id);
                    let parent_shard = Arc::new(self.load_parent_shard(parent)?);

                    // Ingest the parent shard before recovery, as recoverer depends on the shard
                    // existing in kvengine.
                    let normal_shard = self.shards.insert(parent.id, parent_shard.clone());
                    recoverer.recover(self, &parent_shard, parent)?;
                    parents.insert(IdVer::new(parent.id, parent.ver), parent_shard);
                    // Do not keep the parent in the engine as we only use the parent's mem-table
                    // for children.
                    if let Some(normal_shard) = normal_shard {
                        self.shards.insert(parent.id, normal_shard);
                    } else {
                        self.shards.remove(&parent.id);
                    }
                }
            }
        }
        let concurrency = usize::from_str(&env::var("RECOVERY_CONCURRENCY").unwrap_or_default())
            .unwrap_or_else(|_| std::cmp::min(num_cpus::get() * 8, 64));
        info!("recovery concurrency {}", concurrency);
        let (token_tx, token_rx) = tikv_util::mpsc::bounded(concurrency);
        for _ in 0..concurrency {
            token_tx.send(true).unwrap();
        }
        for meta in metas.values() {
            tikv_util::set_current_region(meta.id);
            let meta = meta.clone();
            let engine = self.clone();
            let recoverer = recoverer.clone();
            let token_tx = token_tx.clone();
            token_rx.recv().unwrap();
            let parent_shard = meta.parent.as_ref().map(|parent_meta| {
                parents
                    .get(&IdVer::new(parent_meta.id, parent_meta.ver))
                    .cloned()
                    .unwrap()
            });
            std::thread::spawn(move || {
                tikv_util::set_current_region(meta.id);
                let shard = engine.load_and_ingest_shard(&meta).unwrap();
                if let Some(parent) = parent_shard {
                    shard.add_parent_mem_tbls(parent)
                }
                recoverer.recover(&engine, &shard, &meta).unwrap();
                token_tx.send(true).unwrap();
            });
        }
        for _ in 0..concurrency {
            token_rx.recv().unwrap();
        }
        Ok(())
    }

    /// Should close engine explicitly if engine is not useful anymore but
    /// process is still running.
    pub fn close(&self) {
        info!(
            "Close kvengine {} and stop the background worker, core ref count {}",
            self.get_engine_id(),
            Arc::strong_count(&self.core)
        );
        self.shutting_down.store(true, Ordering::Release);
        self.compact_tx.send(CompactMsg::Stop).unwrap();
        self.flush_tx.send(FlushMsg::Stop).unwrap();
        self.free_tx.send(FreeMemMsg::Stop).unwrap();
    }
}

pub struct EngineCore {
    pub(crate) engine_id: AtomicU64,
    pub(crate) shards: DashMap<u64, Arc<Shard>>,
    pub opts: Arc<Options>,
    pub(crate) flush_tx: mpsc::Sender<FlushMsg>,
    pub(crate) compact_tx: mpsc::Sender<CompactMsg>,
    pub(crate) fs: Arc<dyn dfs::Dfs>,
    pub(crate) cache: SegmentedCache<BlockCacheKey, Bytes>,
    pub comp_client: CompactionClient,
    pub(crate) id_allocator: Arc<dyn IdAllocator>,
    pub(crate) managed_safe_ts: AtomicU64,
    pub(crate) tmp_file_id: AtomicU64,
    pub(crate) rate_limiter: Arc<IoRateLimiter>,
    pub(crate) free_tx: mpsc::Sender<FreeMemMsg>,
    pub(crate) loaded: AtomicBool,
    pub(crate) file_locks: Vec<Mutex<()>>,
    pub(crate) shutting_down: AtomicBool,
}

impl Drop for EngineCore {
    fn drop(&mut self) {
        info!("Drop kvengine {:?} core ", self.get_engine_id());
    }
}

impl EngineCore {
    pub fn set_engine_id(&self, engine_id: u64) {
        self.engine_id.store(engine_id, Ordering::Release);
    }

    pub fn get_engine_id(&self) -> u64 {
        self.engine_id.load(Ordering::Acquire)
    }

    // This method is also used by native-br for cluster restore.
    pub fn read_meta(&self, meta_iter: &mut impl MetaIterator) -> Result<HashMap<u64, ShardMeta>> {
        let mut metas = HashMap::new();
        let engine_id = meta_iter.engine_id();
        meta_iter.iterate(|cs| {
            let meta = ShardMeta::new(engine_id, &cs);
            metas.insert(meta.id, meta);
        })?;
        Ok(metas)
    }

    /// Load shard from meta and ingest to the engine.
    fn load_and_ingest_shard(&self, meta: &ShardMeta) -> Result<Arc<Shard>> {
        if let Some(shard) = self.get_shard(meta.id) {
            if shard.ver == meta.ver {
                return Ok(shard);
            }
        }
        info!("load and ingest shard {}", meta.tag());
        let change_set = self.prepare_change_set(meta.to_change_set(), false)?;
        self.ingest(change_set, false)?;
        let shard = self.get_shard(meta.id);
        Ok(shard.unwrap())
    }

    /// Load parent shard for recovery.
    /// The shard is not ingested to the engine.
    fn load_parent_shard(&self, meta: &ShardMeta) -> Result<Shard> {
        info!("load parent shard {}", meta.tag());
        let change_set = self.prepare_change_set(meta.to_change_set(), false)?;
        let shard = self.new_shard_from_change_set(change_set);
        shard.refresh_states();
        Ok(shard)
    }

    fn new_shard_from_change_set(&self, cs: ChangeSet) -> Shard {
        let engine_id = self.engine_id.load(Ordering::Acquire);
        let shard = Shard::new_for_ingest(engine_id, &cs, self.opts.clone());
        let (l0s, blob_tbls, scfs) = create_snapshot_tables(cs.get_snapshot(), &cs);
        let old_data = shard.get_data();
        let data = ShardData::new(
            shard.range.clone(),
            old_data.del_prefixes.clone(),
            old_data.truncate_ts,
            old_data.trim_over_bound,
            vec![CfTable::new()],
            l0s,
            Arc::new(blob_tbls),
            scfs,
        );
        shard.set_data(data);
        shard
    }

    pub fn ingest(&self, cs: ChangeSet, active: bool) -> Result<()> {
        let shard = self.new_shard_from_change_set(cs);
        shard.set_active(active);
        self.refresh_shard_states(&shard);
        match self.shards.entry(shard.id) {
            Entry::Occupied(entry) => {
                let old = entry.get();
                let mut old_mem_tbls = old.get_data().mem_tbls.clone();
                let old_total_seq = old.get_write_sequence() + old.get_meta_sequence();
                let new_total_seq = shard.get_write_sequence() + shard.get_meta_sequence();
                // It's possible that the new version shard has the same write_sequence and meta
                // sequence, so we need to compare shard version first.
                if shard.ver > old.ver || new_total_seq > old_total_seq {
                    entry.replace_entry(Arc::new(shard));
                    for mem_tbl in old_mem_tbls.drain(..) {
                        self.send_free_mem_msg(FreeMemMsg::FreeMem(mem_tbl));
                    }
                } else {
                    info!(
                        "ingest shard {} found old shard {} already exists with higher or equal sequence, skip insert",
                        shard.tag(), old.tag();
                        "new_write_seq" => shard.get_write_sequence(),
                        "new_meta_seq" => shard.get_meta_sequence(),
                        "old_write_seq" => old.get_write_sequence(),
                        "old_meta_seq" => old.get_meta_sequence(),
                    );
                    return Ok(());
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(shard));
            }
        }
        Ok(())
    }

    pub fn get_snap_access(&self, id: u64) -> Option<SnapAccess> {
        if let Some(ptr) = self.shards.get(&id) {
            return Some(ptr.new_snap_access());
        }
        None
    }

    pub fn get_shard(&self, id: u64) -> Option<Arc<Shard>> {
        if let Some(ptr) = self.shards.get(&id) {
            return Some(ptr.value().clone());
        }
        None
    }

    pub fn get_shard_with_ver(&self, shard_id: u64, shard_ver: u64) -> Result<Arc<Shard>> {
        let shard = self.get_shard(shard_id).ok_or(Error::ShardNotFound)?;
        if shard.ver != shard_ver {
            warn!(
                "shard {} version not match, request {}",
                shard.tag(),
                shard_ver,
            );
            return Err(Error::ShardNotMatch);
        }
        Ok(shard)
    }

    pub fn remove_shard(&self, shard_id: u64) -> bool {
        let x = self.shards.remove(&shard_id);
        if let Some((_, _ptr)) = x {
            return true;
        }
        false
    }

    pub fn size(&self) -> u64 {
        self.shards
            .iter()
            .map(|x| x.value().estimated_size.load(Ordering::Relaxed) + 1)
            .reduce(|x, y| x + y)
            .unwrap_or(0)
    }

    pub(crate) fn trigger_flush(&self, shard: &Shard) {
        if !shard.is_active() {
            return;
        }
        if !shard.get_initial_flushed() {
            self.trigger_initial_flush(shard);
            return;
        }
        let data = shard.get_data();
        let mut mem_tbls = data.mem_tbls.clone();
        while let Some(mem_tbl) = mem_tbls.pop() {
            // writable mem-table's version is 0.
            if mem_tbl.get_version() != 0 {
                self.send_flush_msg(FlushMsg::Task(Box::new(FlushTask::new_normal(
                    shard, mem_tbl,
                ))));
            }
        }
    }

    fn trigger_initial_flush(&self, shard: &Shard) {
        let guard = shard.parent_snap.read().unwrap();
        let parent_snap = guard.as_ref().unwrap().clone();
        let mut mem_tbls = vec![];
        let data = shard.get_data();
        // A newly split shard's meta_sequence is an in-mem state until initial flush.
        let data_sequence = shard.get_meta_sequence();
        for mem_tbl in &data.mem_tbls.as_slice()[1..] {
            debug!(
                "trigger initial flush check mem table version {}, size {}, parent base {} parent write {}",
                mem_tbl.get_version(),
                mem_tbl.size(),
                parent_snap.base_version,
                parent_snap.data_sequence,
            );
            if mem_tbl.get_version() > parent_snap.base_version + parent_snap.data_sequence
                && mem_tbl.get_version() <= shard.get_base_version() + data_sequence
                && mem_tbl.has_data_in_range(shard.inner_start(), shard.inner_end())
            {
                mem_tbls.push(mem_tbl.clone());
            }
        }
        self.send_flush_msg(FlushMsg::Task(Box::new(FlushTask::new_initial(
            shard,
            InitialFlush {
                parent_snap,
                mem_tbls,
                base_version: shard.get_base_version(),
                data_sequence,
            },
        ))));
    }

    pub fn build_ingest_files(
        &self,
        shard_id: u64,
        shard_ver: u64,
        mut iter: Box<dyn table::Iterator>,
        ingest_id: Vec<u8>,
        meta: ShardMeta,
    ) -> Result<kvenginepb::ChangeSet> {
        let shard = self.get_shard_with_ver(shard_id, shard_ver)?;
        let l0_version = shard.load_mem_table_version();
        let mut cs = new_change_set(shard_id, shard_ver);
        let ingest_files = cs.mut_ingest_files();
        ingest_files
            .mut_properties()
            .mut_keys()
            .push(INGEST_ID_KEY.to_string());
        ingest_files.mut_properties().mut_values().push(ingest_id);
        let opts = dfs::Options::new(shard_id, shard_ver);
        let (tx, rx) = tikv_util::mpsc::unbounded();
        let mut tbl_cnt = 0;
        let block_size = self.opts.table_builder_options.block_size;
        let max_table_size = self.opts.table_builder_options.max_table_size;
        let zstd_compression_lvl = self.opts.table_builder_options.compression_lvl;
        let mut builder =
            table::sstable::Builder::new(0, block_size, ZSTD_COMPRESSION, zstd_compression_lvl);
        let mut fids = vec![];

        for (id, smallest, biggest) in meta.get_blob_files() {
            let mut blob_create = kvenginepb::BlobCreate::new();
            warn!("ingest blob file: {}, {:?}, {:?}", id, smallest, biggest);
            blob_create.set_id(id);
            blob_create.set_smallest(smallest);
            blob_create.set_biggest(biggest);
            ingest_files.mut_blob_creates().push(blob_create);
        }

        iter.rewind();
        while iter.valid() {
            if fids.is_empty() {
                fids = self.id_allocator.alloc_id(10).unwrap();
            }
            let id = fids.pop().unwrap();
            builder.reset(id);
            while iter.valid() {
                builder.add(iter.key(), &iter.value(), None);
                iter.next();
                if builder.estimated_size() > max_table_size || !iter.valid() {
                    info!("builder estimated_size {}", builder.estimated_size());
                    let mut buf = BytesMut::with_capacity(builder.estimated_size());
                    let res = builder.finish(0, &mut buf);
                    let level = meta.get_ingest_level(&res.smallest, &res.biggest);
                    assert!(!is_blob_file(level));
                    if level == 0 {
                        let mut offsets = vec![buf.len() as u32; NUM_CFS];
                        offsets[0] = 0;
                        for offset in offsets {
                            buf.put_u32_le(offset);
                        }
                        buf.put_u64_le(l0_version);
                        buf.put_u32_le(NUM_CFS as u32);
                        buf.put_u32_le(MAGIC_NUMBER);
                        let mut l0_create = kvenginepb::L0Create::new();
                        l0_create.set_id(id);
                        l0_create.set_smallest(res.smallest);
                        l0_create.set_biggest(res.biggest);
                        ingest_files.mut_l0_creates().push(l0_create);
                    } else {
                        let mut tbl_create = kvenginepb::TableCreate::new();
                        tbl_create.set_id(id);
                        tbl_create.set_cf(0);
                        tbl_create.set_level(level);
                        tbl_create.set_smallest(res.smallest);
                        tbl_create.set_biggest(res.biggest);
                        ingest_files.mut_table_creates().push(tbl_create);
                    }
                    tbl_cnt += 1;
                    let fs = self.fs.clone();
                    let atx = tx.clone();
                    self.fs.get_runtime().spawn(async move {
                        if let Err(err) = fs.create(id, buf.freeze(), opts).await {
                            atx.send(Err(err)).unwrap();
                        } else {
                            atx.send(Ok(())).unwrap();
                        }
                    });
                    break;
                }
            }
        }
        let mut errs = vec![];
        for _ in 0..tbl_cnt {
            if let Err(err) = rx.recv().unwrap() {
                errs.push(err)
            }
        }
        if !errs.is_empty() {
            return Err(errs.pop().unwrap().into());
        }
        Ok(cs)
    }

    // get_all_shard_id_vers collects all the id and vers of the engine.
    // To prevent the shard change during the iteration, we iterate twice and make
    // sure there is no change during the iteration.
    // Use this method first, then get each shard by id to reduce lock contention.
    pub fn get_all_shard_id_vers(&self) -> Vec<IdVer> {
        loop {
            let id_vers = self.collect_shard_id_vers();
            let id_vers_set = HashSet::<_>::from_iter(id_vers.iter());
            let recheck = self.collect_shard_id_vers();
            if recheck.len() == id_vers_set.len()
                && recheck.iter().all(|id_ver| id_vers_set.contains(id_ver))
            {
                return id_vers;
            }
        }
    }

    fn collect_shard_id_vers(&self) -> Vec<IdVer> {
        self.shards
            .iter()
            .map(|x| IdVer::new(x.id, x.ver))
            .collect()
    }

    // meta_committed should be called when a change set is committed in the raft
    // group.
    pub fn meta_committed(&self, cs: &kvenginepb::ChangeSet, rejected: bool) {
        if cs.has_flush() || cs.has_initial_flush() {
            let table_version = change_set_table_version(cs);
            let id_ver = IdVer::new(cs.shard_id, cs.shard_ver);
            self.send_flush_msg(FlushMsg::Committed((id_ver, table_version)));
        }
        if rejected
            && (cs.has_compaction()
                || cs.has_destroy_range()
                || cs.has_truncate_ts()
                || cs.has_trim_over_bound()
                || cs.has_major_compaction())
        {
            // This compaction may be conflicted with initial flush, so we have to trigger
            // next compaction if needed.
            let shard = self.get_shard(cs.shard_id).unwrap();
            store_bool(&shard.compacting, false);
            // Notify the compaction runner otherwise the shard can't be compacted any more.
            self.send_compact_msg(CompactMsg::Applied(IdVer::new(cs.shard_id, cs.shard_ver)));
            self.refresh_shard_states(&shard);
        }
    }

    pub fn set_shard_active(&self, shard_id: u64, active: bool) {
        if let Some(shard) = self.get_shard(shard_id) {
            info!("shard {} set active {}", shard.tag(), active);
            shard.set_active(active);
            if active {
                self.refresh_shard_states(&shard);
                self.trigger_flush(&shard);
            } else {
                store_bool(&shard.compacting, false);
                self.send_flush_msg(FlushMsg::Clear(shard_id));
                self.send_compact_msg(CompactMsg::Clear(IdVer::new(shard.id, shard.ver)));
            }
        }
    }

    pub(crate) fn refresh_shard_states(&self, shard: &Shard) {
        shard.refresh_states();

        fail::fail_point!("before_engine_trigger_compact", |_| ());
        if shard.ready_to_compact() {
            self.trigger_compact(shard.id_ver());
        }
    }

    pub fn trigger_compact(&self, id_ver: IdVer) {
        self.send_compact_msg(CompactMsg::Compact(id_ver));
    }

    pub fn get_cache_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    // lock_file is used to prevent race between local file gc and
    // prepare_change_set.
    pub fn lock_file(&self, file_id: u64) -> MutexGuard<'_, ()> {
        let idx = file_id as usize % FILE_LOCK_SLOTS;
        self.file_locks[idx].lock().unwrap()
    }

    pub(crate) fn send_compact_msg(&self, msg: CompactMsg) {
        if let Err(e) = self.compact_tx.send(msg) {
            assert!(self.shutting_down.load(Ordering::Acquire));
            info!(
                "Engine {} is shutting down, cannot send compact msg {:?}",
                self.get_engine_id(),
                e
            );
        }
    }

    pub(crate) fn send_flush_msg(&self, msg: FlushMsg) {
        if let Err(e) = self.flush_tx.send(msg) {
            assert!(self.shutting_down.load(Ordering::Acquire));
            info!(
                "Engine {} is shutting down, cannot send flush msg {:?}",
                self.get_engine_id(),
                e
            );
        }
    }

    pub(crate) fn send_free_mem_msg(&self, msg: FreeMemMsg) {
        if let Err(e) = self.free_tx.send(msg) {
            assert!(self.shutting_down.load(Ordering::Acquire));
            info!(
                "Engine {} is shutting down, cannot send free mem msg {:?}",
                self.get_engine_id(),
                e
            );
        }
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct ShardTag {
    pub engine_id: u64,
    pub id_ver: IdVer,
}

impl ShardTag {
    pub fn new(engine_id: u64, id_ver: IdVer) -> Self {
        Self { engine_id, id_ver }
    }

    pub fn from_comp_req(req: &CompactionRequest) -> Self {
        Self {
            engine_id: req.engine_id,
            id_ver: IdVer::new(req.shard_id, req.shard_ver),
        }
    }
}

impl Display for ShardTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.engine_id, self.id_ver.id, self.id_ver.ver
        )
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdVer {
    pub id: u64,
    pub ver: u64,
}

impl IdVer {
    pub fn new(id: u64, ver: u64) -> Self {
        Self { id, ver }
    }

    pub fn from_change_set(cs: &kvenginepb::ChangeSet) -> Self {
        Self::new(cs.shard_id, cs.shard_ver)
    }
}

pub fn new_sst_filename(file_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.sst", file_id))
}

pub fn new_tmp_filename(file_id: u64, tmp_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.{}.tmp", file_id, tmp_id))
}

pub fn new_blob_filename(file_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.sst", file_id))
}

pub(crate) enum FreeMemMsg {
    /// Free CfTable
    FreeMem(CfTable),
    /// Stop the free mem background worker
    Stop,
}

fn free_mem(free_rx: mpsc::Receiver<FreeMemMsg>) {
    loop {
        let cnt = free_rx.len();
        let mut tables = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            match free_rx.recv().unwrap() {
                FreeMemMsg::FreeMem(tbl) => {
                    tables.push(tbl);
                }
                FreeMemMsg::Stop => {
                    drop(tables);
                    info!("Engine free mem worker receive stop msg and stop now");
                    return;
                }
            }
        }
        drop(tables);
        thread::sleep(Duration::from_secs(5));
    }
}
