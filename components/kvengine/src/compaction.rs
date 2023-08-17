// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use core::panic;
use std::{
    cmp::Ordering as CmpOrdering,
    collections::{HashMap, HashSet},
    hash::Hash,
    iter::Iterator as StdIterator,
    ops::{Deref, Sub},
    sync::{atomic::Ordering, Arc, Mutex},
    time::{Duration, Instant},
};

use api_version::{
    api_v2::{self, KEYSPACE_PREFIX_LEN},
    ApiV2, KeyMode, KvFormat,
};
use bytes::{Buf, Bytes, BytesMut};
use cloud_encryption::{EncryptionKey, MasterKey};
use http::StatusCode;
use kvenginepb as pb;
use pb::{BlobCreate, TableCreate};
use protobuf::Message;
use slog_global::error;
use tikv_util::mpsc;

use crate::{
    dfs,
    table::{
        blobtable::{
            blobtable::BlobTable,
            builder::{BlobTableBuildOptions, BlobTableBuilder},
        },
        search,
        sstable::{self, builder::TableBuilderOptions, InMemFile, L0Builder, SsTable},
        InnerKey,
    },
    Error::{FallbackLocalCompactorDisabled, IncompatibleRemoteCompactor, RemoteCompaction},
    Iterator, EXTRA_CF, LOCK_CF, WRITE_CF, *,
};

static RETRY_INTERVAL: Duration = Duration::from_secs(600);

#[derive(Default, Eq, PartialEq, Hash, Clone)]
pub struct RemoteCompactor {
    remote_url: String,
    permanent: bool,
}

impl RemoteCompactor {
    pub fn new(remote_url: String, permanent: bool) -> Self {
        Self {
            remote_url,
            permanent,
        }
    }
}

pub struct RemoteCompactors {
    remote_urls: Vec<RemoteCompactor>,
    index: usize,
    last_failure: Instant,
}

impl RemoteCompactors {
    pub fn new(remote_url: String) -> Self {
        let mut remote_urls = Vec::new();
        if !remote_url.is_empty() {
            remote_urls.push(RemoteCompactor::new(remote_url, true));
        };
        Self {
            remote_urls,
            index: 0,
            last_failure: Instant::now().sub(RETRY_INTERVAL),
        }
    }
}

#[derive(Clone)]
pub struct CompactionClient {
    dfs: Arc<dyn dfs::Dfs>,
    id_allocator: Arc<dyn IdAllocator>,
    remote_compactors: Arc<Mutex<RemoteCompactors>>,
    client: Option<hyper::Client<hyper::client::HttpConnector>>,
    compression_lvl: i32,
    allow_fallback_local: bool,
    master_key: MasterKey,
}

impl CompactionClient {
    pub(crate) fn new(
        dfs: Arc<dyn dfs::Dfs>,
        remote_url: String,
        compression_lvl: i32,
        allow_fallback_local: bool,
        id_allocator: Arc<dyn IdAllocator>,
        master_key: MasterKey,
    ) -> Self {
        let remote_compactors = RemoteCompactors::new(remote_url);
        let client = hyper::Client::builder()
            .pool_max_idle_per_host(0)
            .build_http();
        Self {
            dfs,
            remote_compactors: Arc::new(Mutex::new(remote_compactors)),
            client: Some(client),
            compression_lvl,
            allow_fallback_local,
            id_allocator,
            master_key,
        }
    }

    pub fn add_remote_compactor(&mut self, remote_url: String) {
        if !remote_url.is_empty() {
            let mut remote_compactors = self.remote_compactors.lock().unwrap();
            remote_compactors.last_failure = Instant::now().sub(RETRY_INTERVAL);
            if remote_compactors
                .remote_urls
                .iter()
                .any(|x| x.remote_url == remote_url.clone())
            {
                return;
            }
            remote_compactors
                .remote_urls
                .push(RemoteCompactor::new(remote_url.clone(), false));
            info!(
                "add remote compactor {}, total {}",
                remote_url,
                remote_compactors.remote_urls.len()
            );
        };
    }

    pub fn delete_remote_compactor(&self, remote_compactor: &RemoteCompactor) {
        let mut remote_compactors = self.remote_compactors.lock().unwrap();
        if !remote_compactor.permanent {
            remote_compactors
                .remote_urls
                .retain(|x| x.remote_url != remote_compactor.remote_url);
            info!(
                "delete remote compactor {}, total {}",
                remote_compactor.remote_url,
                remote_compactors.remote_urls.len()
            );
        } else if remote_compactors.remote_urls.len() == 1 {
            remote_compactors.last_failure = Instant::now();
        }
    }

    pub fn get_remote_compactors(&self) -> Vec<String> {
        self.remote_compactors
            .lock()
            .unwrap()
            .remote_urls
            .clone()
            .into_iter()
            .map(|r| r.remote_url)
            .collect()
    }

    pub fn get_remote_compactor(&self) -> RemoteCompactor {
        let mut remote_compactors = self.remote_compactors.lock().unwrap();
        if remote_compactors.remote_urls.is_empty()
            || remote_compactors.last_failure.elapsed().le(&RETRY_INTERVAL)
        {
            RemoteCompactor::default()
        } else {
            remote_compactors.index += 1;
            if remote_compactors.index >= remote_compactors.remote_urls.len() {
                remote_compactors.index = 0;
            }
            remote_compactors.remote_urls[remote_compactors.index].clone()
        }
    }

    pub(crate) fn compact(&self, req: CompactionRequest) -> Result<pb::ChangeSet> {
        let encryption_key = if req.exported_encryption_key.is_empty() {
            None
        } else {
            Some(
                self.master_key
                    .decrypt_encryption_key(&req.exported_encryption_key)
                    .unwrap(),
            )
        };
        let ctx = CompactionCtx {
            req: Arc::new(req),
            dfs: self.dfs.clone(),
            compression_lvl: self.compression_lvl,
            id_allocator: self.id_allocator.clone(),
            encryption_key,
        };
        let req = &ctx.req;
        let mut remote_compactor = self.get_remote_compactor();
        if remote_compactor.remote_url.is_empty() {
            if req.compactor_version >= 3 {
                return local_compact_v3(&ctx);
            }
            local_compact(&ctx)
        } else {
            let (tx, rx) = tikv_util::mpsc::bounded(1);
            let mut retry_cnt = 0;
            loop {
                let tx = tx.clone();
                let req_clone = req.clone();
                let client = self.clone();
                let remote_url = remote_compactor.remote_url.clone();
                self.dfs.get_runtime().spawn(async move {
                    let result = client.remote_compact(&req_clone, remote_url).await;
                    tx.send(result).unwrap();
                });
                match rx.recv().unwrap() {
                    result @ Ok(_) => break result,
                    Err(e @ IncompatibleRemoteCompactor { .. }) => {
                        if self.allow_fallback_local {
                            warn!("fall back to local compactor due to error: {:?}", e);
                            if req.compactor_version >= 3 {
                                break local_compact_v3(&ctx);
                            }
                            break local_compact(&ctx);
                        } else {
                            warn!(
                                "remote compactor is incompatible and local compaction is not allowed"
                            );
                            break Err(FallbackLocalCompactorDisabled);
                        }
                    }
                    Err(e) => {
                        retry_cnt += 1;
                        let tag = ShardTag::from_comp_req(req);
                        error!(
                            "shard {}, req {:?}, remote compaction failed {:?}, retrying {} remote compactor {}",
                            tag, req, e, retry_cnt, remote_compactor.remote_url
                        );

                        if remote_compactor.permanent && retry_cnt >= 5
                            || !remote_compactor.permanent && retry_cnt >= 3
                        {
                            retry_cnt = 0;
                            self.delete_remote_compactor(&remote_compactor);
                            remote_compactor = self.get_remote_compactor();
                        }
                        if remote_compactor.remote_url.is_empty() {
                            if self.allow_fallback_local {
                                break local_compact(&ctx);
                            } else {
                                warn!(
                                    "no remote compactor available and local compaction is not allowed"
                                );
                                break Err(FallbackLocalCompactorDisabled);
                            }
                        }
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        }
    }

    async fn remote_compact(
        &self,
        comp_req: &CompactionRequest,
        remote_url: String,
    ) -> Result<pb::ChangeSet> {
        let body_str = serde_json::to_string(&comp_req).unwrap();
        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(remote_url.clone())
            .header("content-type", "application/json")
            .body(hyper::Body::from(body_str))?;
        let tag = ShardTag::from_comp_req(comp_req);
        info!("{} send request to remote compactor", tag);
        let response = self.client.as_ref().unwrap().request(req).await?;
        info!("{} got response from remote compactor", tag);
        let status = response.status();
        let body = hyper::body::to_bytes(response.into_body()).await?;
        if !status.is_success() {
            let err_msg = String::from_utf8_lossy(body.chunk()).to_string();
            return if status == INCOMPATIBLE_COMPACTOR_ERROR_CODE {
                Err(IncompatibleRemoteCompactor {
                    url: remote_url,
                    msg: err_msg,
                })
            } else {
                Err(RemoteCompaction(err_msg))
            };
        }
        let mut cs = pb::ChangeSet::new();
        if let Err(err) = cs.merge_from_bytes(&body) {
            return Err(RemoteCompaction(err.to_string()));
        }
        Ok(cs)
    }
}

/// `CURRENT_COMPACTOR_VERSION` is used for version compatibility checking of
/// remote compactor. NOTE: Increase `CURRENT_COMPACTOR_VERSION` by 1 when add
/// new feature to remote compactor.
const CURRENT_COMPACTOR_VERSION: u32 = 3;

const INCOMPATIBLE_COMPACTOR_ERROR_CODE: StatusCode = StatusCode::NOT_IMPLEMENTED;

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CompactionRequest {
    pub engine_id: u64,
    pub shard_id: u64,
    pub shard_ver: u64,

    #[serde(rename = "start")]
    pub outer_start: Vec<u8>,
    #[serde(rename = "end")]
    pub outer_end: Vec<u8>,
    pub inner_key_off: usize,

    pub file_ids: Vec<u64>,

    pub compaction_tp: CompactionType,
    /// Required version of remote compactor.
    /// Must be set to `CURRENT_COMPACTOR_VERSION`.
    pub compactor_version: u32,

    /// All following fields belong to v2 compaction, will be deprecated in v3.
    pub cf: isize,
    pub level: usize,
    pub tops: Vec<u64>,
    /// If `destroy_range` is true, `in_place_compact_files` will be compacted
    /// in place to filter out data that covered by `del_prefixes`.
    pub destroy_range: bool,
    pub del_prefixes: Vec<u8>,
    // Vec<(id, level, cf)>
    pub in_place_compact_files: Vec<(u64, u32, i32)>,

    /// If `truncate_ts` is some, `in_place_compact_files` will be compacted in
    /// place to filter out data with version > `truncated_ts`.
    pub truncate_ts: Option<u64>,

    /// Requires `compactor_version >= 1`.
    /// If `trim_over_bound` is true, `in_place_compact_files` will be compacted
    /// in place to filter out data out of shard bound.
    pub trim_over_bound: bool,

    // Used for L1+ compaction.
    pub bottoms: Vec<u64>,

    // Used for L0 compaction.
    pub multi_cf_bottoms: Vec<Vec<u64>>,
    // End of deprecating members
    pub overlap: bool,
    pub safe_ts: u64,
    pub block_size: usize,
    pub max_table_size: usize,
    pub compression_tp: u8,
    pub exported_encryption_key: Vec<u8>,
}

impl CompactionRequest {
    pub fn inner_start(&self) -> InnerKey<'_> {
        InnerKey::from_outer_key(&self.outer_start, self.inner_key_off)
    }

    pub fn inner_end(&self) -> InnerKey<'_> {
        InnerKey::from_outer_end_key(&self.outer_end, self.inner_key_off)
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct MajorCompaction {
    safe_ts: u64,
    l0_tables: Vec<u64>,
    // L1 plus sstables of all cfs, map from cf id to sstables that are organized by level.
    ln_tables: HashMap<usize, Vec<(usize, Vec<u64>)>>,
    blob_tables: Vec<u64>,
    sst_config: TableBuilderOptions,
    bt_config: Option<BlobTableBuildOptions>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct L0Compaction {
    safe_ts: u64,
    l0_tables: Vec<u64>,
    multi_cf_l1_tables: Vec<Vec<u64>>,
    sst_config: TableBuilderOptions,
    bt_config: Option<BlobTableBuildOptions>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct L1PlusCompaction {
    cf: isize,
    level: usize,
    safe_ts: u64,
    // Whether to keep the latest tombstone before the safe ts.
    keep_latest_obsolete_tombstone: bool,
    upper_level: Vec<u64>,
    lower_level: Vec<u64>,
    sst_config: TableBuilderOptions,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub enum InPlaceCompaction {
    #[default]
    Unknown,
    TruncateTs(u64),
    TrimOverBound,
    DestroyRange(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub enum CompactionType {
    #[default]
    Unknown,
    Major(MajorCompaction),
    L0(L0Compaction),
    L1Plus(L1PlusCompaction),
    InPlace {
        file_ids: Vec<(u64, u32, i32)>,
        block_size: usize,
        spec: InPlaceCompaction,
    },
}

const MAX_COMPACTION_EXPAND_SIZE: u64 = 256 * 1024 * 1024;

impl Engine {
    pub fn update_managed_safe_ts(&self, ts: u64) {
        loop {
            let old = load_u64(&self.managed_safe_ts);
            if old < ts
                && self
                    .managed_safe_ts
                    .compare_exchange(old, ts, Ordering::Release, Ordering::Relaxed)
                    .is_err()
            {
                continue;
            }
            break;
        }
    }

    pub fn get_keyspace_gc_safepoint_v2(&self, key: &[u8]) -> u64 {
        match &self.ks_safepoint_v2 {
            Some(sp_map) => {
                if key[0] == b'x' && key.len() >= KEYSPACE_PREFIX_LEN {
                    // Api v2 key.
                    let keyspace_id = ApiV2::get_keyspace_id(key);
                    let keyspace_id_u32 = ApiV2::get_u32_keyspace_id(keyspace_id);

                    let keyspace_sp_ts = sp_map.get(&keyspace_id_u32);
                    match keyspace_sp_ts {
                        None => 0,
                        Some(ks2sp) => {
                            let ks_gc_sp = *ks2sp.value();
                            debug!(
                                "Get gc safe point v2, key:{:?}, keyspace_id:{}, gc safepoint:{}",
                                log_wrappers::Value::key(key),
                                keyspace_id_u32,
                                ks_gc_sp
                            );
                            ks_gc_sp
                        }
                    }
                } else {
                    // Api v1 key.
                    let gc_safe_point_ts = load_u64(&self.managed_safe_ts);
                    debug!(
                        "Get gc safe point v1, key:{:?}, gc safepoint:{}",
                        log_wrappers::Value::key(key),
                        gc_safe_point_ts,
                    );
                    gc_safe_point_ts
                }
            }
            None => {
                // Api v1 key.
                let gc_safe_point_ts = load_u64(&self.managed_safe_ts);
                debug!(
                    "Get gc safe point v1, key:{:?}, gc safepoint:{}",
                    log_wrappers::Value::key(key),
                    gc_safe_point_ts,
                );
                gc_safe_point_ts
            }
        }
    }

    pub(crate) fn run_compaction(&self, compact_rx: mpsc::Receiver<CompactMsg>) {
        let mut runner = CompactRunner::new(self.clone(), compact_rx);
        runner.run();
    }

    pub(crate) fn compact(&self, id_ver: IdVer) -> Option<Result<pb::ChangeSet>> {
        let shard = self.get_shard_with_ver(id_ver.id, id_ver.ver).ok()?;
        let tag = shard.tag();
        if !shard.ready_to_compact() {
            info!("Shard {} is not ready for compaction", tag);
            return None;
        }
        store_bool(&shard.compacting, true);
        match shard.get_compaction_priority() {
            Some(CompactionPriority::L0 { .. }) => self.trigger_l0_compaction(&shard),
            Some(CompactionPriority::L1Plus { cf, level, .. }) => {
                self.trigger_l1_plus_compaction(&shard, cf, level, id_ver)
            }
            Some(CompactionPriority::Major { .. }) => self.trigger_major_compacton(&shard),
            Some(CompactionPriority::DestroyRange) => Some(self.destroy_range(&shard)),
            Some(CompactionPriority::TruncateTs) => self.truncate_ts(&shard).transpose(),
            Some(CompactionPriority::TrimOverBound) => self.trim_over_bound(&shard).transpose(),
            None => {
                info!("Shard {} is not urgent for compaction", tag);
                store_bool(&shard.compacting, false);
                None
            }
        }
    }

    pub(crate) fn set_alloc_ids_for_request(
        &self,
        req: &mut CompactionRequest,
        cur_num_files: usize,
        num_files_at_most: usize,
    ) {
        let ids = self
            .id_allocator
            .alloc_id(num_files_at_most * 2 + 16 + cur_num_files)
            .unwrap();
        req.file_ids = ids;
    }

    pub(crate) fn new_compact_request_with_shard(
        &self,
        shard: &Shard,
        cf: isize,
        level: usize,
    ) -> CompactionRequest {
        let exported_encryption_key = shard
            .properties
            .get(ENCRYPTION_KEY)
            .unwrap_or_default()
            .to_vec();
        self.new_compact_request(
            shard.engine_id,
            shard.id,
            shard.ver,
            shard.range.clone(),
            cf,
            level,
            exported_encryption_key,
        )
    }

    pub(crate) fn new_compact_request_with_meta(
        &self,
        meta: &ShardMeta,
        cf: isize,
        level: usize,
    ) -> CompactionRequest {
        let exported_encryption_key = meta
            .properties
            .get(ENCRYPTION_KEY)
            .unwrap_or_default()
            .to_vec();
        self.new_compact_request(
            meta.engine_id,
            meta.id,
            meta.ver,
            meta.range.clone(),
            cf,
            level,
            exported_encryption_key,
        )
    }

    fn new_compact_request(
        &self,
        engine_id: u64,
        shard_id: u64,
        shard_ver: u64,
        range: ShardRange,
        cf: isize,
        level: usize,
        exported_encryption_key: Vec<u8>,
    ) -> CompactionRequest {
        CompactionRequest {
            engine_id,
            shard_id,
            shard_ver,
            outer_start: range.outer_start.to_vec(),
            outer_end: range.outer_end.to_vec(),
            inner_key_off: range.inner_key_off,
            file_ids: vec![],
            compaction_tp: CompactionType::Unknown,
            compactor_version: self.opts.compaction_request_version,
            // all fields below will be deprecated in v3.
            destroy_range: false,
            del_prefixes: vec![],
            in_place_compact_files: vec![],
            truncate_ts: None,
            trim_over_bound: false,
            cf,
            level,
            safe_ts: self.get_keyspace_gc_safepoint_v2(range.outer_start.chunk()),
            block_size: self.opts.table_builder_options.block_size,
            max_table_size: self.opts.table_builder_options.max_table_size,
            compression_tp: self.opts.table_builder_options.compression_tps[level],
            overlap: level == 0,
            tops: vec![],
            bottoms: vec![],
            multi_cf_bottoms: vec![],
            exported_encryption_key,
        }
    }

    fn destroy_range(&self, shard: &Shard) -> Result<pb::ChangeSet> {
        let del_prefixes = shard.get_del_prefixes();
        let data = shard.get_data();
        assert!(!del_prefixes.is_empty());
        // Tables that full covered by delete-prefixes.
        let mut deletes = vec![];
        // Tables that partially covered by delete-prefixes.
        let mut overlaps = vec![];
        for t in &data.l0_tbls {
            if del_prefixes.cover_range(t.smallest(), t.biggest()) {
                let mut delete = pb::TableDelete::default();
                delete.set_id(t.id());
                delete.set_level(0);
                delete.set_cf(-1);
                deletes.push(delete);
            } else if del_prefixes
                .inner_delete_ranges()
                .any(|(start, end)| t.has_data_in_range(start, end))
            {
                overlaps.push((t.id(), 0, -1));
            }
        }
        data.for_each_level(|cf, lh| {
            for t in lh.tables.as_slice() {
                if del_prefixes.cover_range(t.smallest(), t.biggest()) {
                    let mut delete = pb::TableDelete::default();
                    delete.set_id(t.id());
                    delete.set_level(lh.level as u32);
                    delete.set_cf(cf as i32);
                    deletes.push(delete);
                } else if del_prefixes
                    .inner_delete_ranges()
                    .any(|(start, end)| t.has_overlap(start, end, false))
                {
                    overlaps.push((t.id(), lh.level as u32, cf as i32));
                }
            }
            false
        });

        info!(
            "start destroying range for {}, {:?}, destroyed: {}, overlapping: {}",
            shard.tag(),
            del_prefixes,
            deletes.len(),
            overlaps.len()
        );

        let mut cs = if overlaps.is_empty() {
            let mut cs = pb::ChangeSet::default();
            cs.mut_destroy_range().set_table_deletes(deletes.into());
            cs
        } else {
            let mut req = self.new_compact_request_with_shard(shard, 0, 0);
            req.destroy_range = true;
            req.in_place_compact_files = overlaps.clone();
            req.del_prefixes = del_prefixes.marshal();
            req.file_ids = self.id_allocator.alloc_id(overlaps.len()).unwrap();
            let in_place_compaction = InPlaceCompaction::DestroyRange(del_prefixes.marshal());
            req.compaction_tp = CompactionType::InPlace {
                file_ids: overlaps,
                block_size: self.opts.table_builder_options.block_size,
                spec: in_place_compaction,
            };

            let mut cs = self.comp_client.compact(req)?;
            let dr = cs.mut_destroy_range();
            deletes.extend(dr.take_table_deletes().into_iter());
            dr.set_table_deletes(deletes.into());
            cs
        };
        cs.set_shard_id(shard.id);
        cs.set_shard_ver(shard.ver);
        cs.set_property_key(DEL_PREFIXES_KEY.to_string());
        cs.set_property_value(del_prefixes.marshal());
        Ok(cs)
    }

    pub(crate) fn truncate_ts(&self, shard: &Shard) -> Result<Option<pb::ChangeSet>> {
        self.truncate_with_ts(shard, shard.get_truncate_ts().unwrap())
    }

    // Also used by keyspace restore
    pub fn truncate_with_ts(
        &self,
        shard: &Shard,
        truncate_ts: TruncateTs,
    ) -> Result<Option<pb::ChangeSet>> {
        let data = shard.get_data();
        // TODO: record min_ts to directly delete a SSTable.

        let mut overlaps = vec![];
        for t in &data.l0_tbls {
            if truncate_ts.inner() < t.max_ts() {
                overlaps.push((t.id(), 0, -1));
            }
        }
        data.for_each_level(|cf, lh| {
            for t in lh.tables.iter() {
                if truncate_ts.inner() < t.max_ts {
                    overlaps.push((t.id(), lh.level as u32, cf as i32));
                }
            }
            false
        });

        info!(
            "start truncate ts for {}, truncate_ts: {:?}, overlaps: {}",
            shard.tag(),
            truncate_ts,
            overlaps.len(),
        );

        let mut cs = if overlaps.is_empty() {
            let mut cs = pb::ChangeSet::default();
            cs.set_truncate_ts(pb::TableChange::default());
            cs
        } else {
            let mut req = self.new_compact_request_with_shard(shard, 0, 0);
            req.truncate_ts = Some(truncate_ts.inner());
            req.in_place_compact_files = overlaps.clone();
            req.file_ids = self.id_allocator.alloc_id(overlaps.len()).unwrap();
            let in_place_compaction = InPlaceCompaction::TruncateTs(truncate_ts.inner());
            req.compaction_tp = CompactionType::InPlace {
                file_ids: overlaps,
                block_size: self.opts.table_builder_options.block_size,
                spec: in_place_compaction,
            };
            self.comp_client.compact(req)?
        };
        cs.set_shard_id(shard.id);
        cs.set_shard_ver(shard.ver);
        cs.set_property_key(TRUNCATE_TS_KEY.to_string());
        cs.set_property_value(truncate_ts.marshal().to_vec());
        Ok(Some(cs))
    }

    fn trim_over_bound(&self, shard: &Shard) -> Result<Option<pb::ChangeSet>> {
        let data = shard.get_data();
        if !shard.get_trim_over_bound() {
            // `data.trim_over_bound is possible to be false.
            // See https://github.com/tidbcloud/cloud-storage-engine/issues/663.
            info!(
                "{} shard.data.trim_over_bound is false, skip trim_over_bound",
                shard.tag()
            );
            return Ok(None);
        }

        // Tables that are entirely over bound.
        let mut deletes = vec![];
        // Tables that are partially over bound.
        let mut overlaps = vec![];
        for t in &data.l0_tbls {
            if t.biggest() < data.inner_start() || t.smallest() >= data.inner_end() {
                // -----smallest-----biggest-----[start----------end)
                // [start----------end)-----smallest-----biggest-----
                let mut delete = pb::TableDelete::default();
                delete.set_id(t.id());
                delete.set_level(0);
                delete.set_cf(-1);
                deletes.push(delete);
            } else if t.smallest() < data.inner_start() || t.biggest() >= data.inner_end() {
                // -----smallest-----[start----------end)-----biggest-----
                overlaps.push((t.id(), 0, -1));
            }
        }
        data.for_each_level(|cf, lh| {
            for t in lh.tables.iter() {
                if t.biggest() < data.inner_start() || t.smallest() >= data.inner_end() {
                    // -----smallest-----biggest-----[start----------end)
                    // [start----------end)-----smallest-----biggest-----
                    let mut delete = pb::TableDelete::default();
                    delete.set_id(t.id());
                    delete.set_level(lh.level as u32);
                    delete.set_cf(cf as i32);
                    deletes.push(delete);
                } else if t.smallest() < data.inner_start() || t.biggest() >= data.inner_end() {
                    // -----smallest-----[start----------end)-----biggest-----
                    overlaps.push((t.id(), lh.level as u32, cf as i32));
                }
            }
            false
        });

        info!(
            "start trim_over_bound for {}, destroyed: {}, overlapping: {}",
            shard.tag(),
            deletes.len(),
            overlaps.len()
        );

        let mut cs = if overlaps.is_empty() {
            let mut cs = pb::ChangeSet::default();
            cs.mut_trim_over_bound().set_table_deletes(deletes.into());
            cs
        } else {
            let mut req = self.new_compact_request_with_shard(shard, 0, 0);
            req.trim_over_bound = true;
            req.in_place_compact_files = overlaps.clone();
            req.file_ids = self.id_allocator.alloc_id(overlaps.len()).unwrap();
            req.compaction_tp = CompactionType::InPlace {
                file_ids: overlaps,
                block_size: self.opts.table_builder_options.block_size,
                spec: InPlaceCompaction::TrimOverBound,
            };
            let mut cs = self.comp_client.compact(req)?;
            let tc = cs.mut_trim_over_bound();
            deletes.extend(tc.take_table_deletes().into_iter());
            tc.set_table_deletes(deletes.into());
            cs
        };
        cs.set_shard_id(shard.id);
        cs.set_shard_ver(shard.ver);
        cs.set_property_key(TRIM_OVER_BOUND.to_string());
        cs.set_property_value(TRIM_OVER_BOUND_DISABLE.to_vec());
        Ok(Some(cs))
    }

    pub fn trim_over_bound_by_meta(&self, meta: &ShardMeta) -> Result<pb::ChangeSet> {
        // Tables that are entirely over bound.
        let mut deletes = vec![];
        // Tables that are partially over bound.
        let mut overlaps = vec![];
        for (&id, f) in &meta.files {
            if meta.entirely_over_bound_table(f.smallest(), f.biggest()) {
                let mut delete = pb::TableDelete::default();
                delete.set_id(id);
                delete.set_level(f.level as u32);
                delete.set_cf(f.cf as i32);
                deletes.push(delete);
            } else if meta.partially_over_bound_table(f.smallest(), f.biggest()) {
                overlaps.push((id, f.level as u32, f.cf as i32));
            }
        }

        debug!(
            "start trim_over_bound_by_meta for {}:{}, destroyed: {}, overlapping: {}",
            meta.id,
            meta.ver,
            deletes.len(),
            overlaps.len()
        );

        let mut res_cs = if overlaps.is_empty() {
            let mut cs = pb::ChangeSet::default();
            if !deletes.is_empty() {
                cs.mut_trim_over_bound().set_table_deletes(deletes.into());
            }
            cs
        } else {
            let mut req = self.new_compact_request_with_meta(meta, 0, 0);
            req.trim_over_bound = true;
            req.in_place_compact_files = overlaps.clone();
            req.file_ids = self.id_allocator.alloc_id(overlaps.len()).unwrap();
            req.compaction_tp = CompactionType::InPlace {
                file_ids: overlaps,
                block_size: self.opts.table_builder_options.block_size,
                spec: InPlaceCompaction::TrimOverBound,
            };
            let mut cs = self.comp_client.compact(req)?;
            let tc = cs.mut_trim_over_bound();
            deletes.extend(tc.take_table_deletes().into_iter());
            tc.set_table_deletes(deletes.into());
            cs
        };
        res_cs.set_shard_id(meta.id);
        res_cs.set_shard_ver(meta.ver);
        res_cs.set_property_key(TRIM_OVER_BOUND.to_string());
        res_cs.set_property_value(TRIM_OVER_BOUND_DISABLE.to_vec());
        Ok(res_cs)
    }

    fn get_blob_table_build_options_or_none(&self, shard: &Shard) -> Option<BlobTableBuildOptions> {
        if shard.outer_start.len() >= api_v2::KEYSPACE_PREFIX_LEN {
            let start_key_mode = ApiV2::parse_key_mode(&shard.outer_start);
            let end_key_mode = ApiV2::parse_key_mode(&shard.outer_end);
            if (start_key_mode == KeyMode::Raw || start_key_mode == KeyMode::Txn)
                && (end_key_mode == KeyMode::Raw || end_key_mode == KeyMode::Txn)
            {
                let keyspace_id =
                    ApiV2::get_u32_keyspace_id(ApiV2::get_keyspace_id(&shard.outer_start));
                match self
                    .per_keyspace_configs
                    .get(&keyspace_id)
                    .map(|c| c.blob_table_build_options)
                {
                    Some(bt_build_options) => {
                        if bt_build_options.min_blob_size != 0 {
                            return Some(bt_build_options);
                        } else {
                            return None;
                        }
                    }
                    _ => return None,
                };
            }
        }
        None
    }

    pub(crate) fn trigger_l0_compaction(&self, shard: &Shard) -> Option<Result<pb::ChangeSet>> {
        let tag = shard.tag();
        let data = shard.get_data();
        if data.l0_tbls.is_empty() {
            info!("{} zero L0 tables", tag);
            store_bool(&shard.compacting, false);
            return None;
        }
        let mut req = self.new_compact_request_with_shard(shard, -1, 0);
        let mut l0_tbls = vec![];
        let mut multi_cfs_l1_tbls = vec![];
        let mut total_size = 0;
        let mut smallest = data.l0_tbls[0].smallest();
        let mut biggest = data.l0_tbls[0].biggest();
        let mut estimated_blob_size = 0;
        for l0 in &data.l0_tbls {
            if smallest > l0.smallest() {
                smallest = l0.smallest();
            }
            if biggest < l0.biggest() {
                biggest = l0.biggest();
            }
            l0_tbls.push(l0.id());
            if l0.size() > l0.entries() * self.opts.blob_table_build_options.min_blob_size as u64 {
                estimated_blob_size += l0.size()
                    - l0.entries() * self.opts.blob_table_build_options.min_blob_size as u64;
            }
            total_size += l0.size();
        }

        req.tops.extend(l0_tbls.clone());
        for cf in 0..NUM_CFS {
            let lh = data.get_cf(cf).get_level(1);
            let mut l1_tbls = vec![];
            for tbl in lh.tables.as_slice() {
                if tbl.biggest() < smallest || tbl.smallest() > biggest {
                    info!(
                        "{} skip L1 table {} for L0 compaction, tbl smallest {:?}, tbl biggest {:?}, L0 smallest {:?}, L0 biggest {:?}",
                        tag,
                        tbl.id(),
                        tbl.smallest(),
                        tbl.biggest(),
                        smallest,
                        biggest,
                    );
                    continue;
                }
                l1_tbls.push(tbl.id());
                total_size += tbl.size();
                if tbl.size()
                    > tbl.entries as u64 * self.opts.blob_table_build_options.min_blob_size as u64
                {
                    estimated_blob_size += tbl.size()
                        - tbl.entries as u64
                            * self.opts.blob_table_build_options.min_blob_size as u64;
                }
            }
            req.multi_cf_bottoms.push(l1_tbls.clone());
            multi_cfs_l1_tbls.push(l1_tbls);
        }
        let sst_config = self.opts.table_builder_options;

        // Only opt-in users will use blob store.
        let mut bt_config = self.get_blob_table_build_options_or_none(shard);
        if let Some(build_opts) = &bt_config {
            if estimated_blob_size < build_opts.target_blob_table_size as u64 {
                bt_config = None;
            }
        }
        info!(
            "{} build blob in L0 compaction, estimated blob size {}, blob table build options {:?}",
            tag, estimated_blob_size, bt_config
        );
        let estimated_num_files = total_size as usize
            / bt_config.map_or(sst_config.max_table_size, |c| {
                std::cmp::min(c.max_blob_table_size, sst_config.max_table_size)
            });
        self.set_alloc_ids_for_request(
            &mut req,
            l0_tbls.len() + multi_cfs_l1_tbls.len(),
            estimated_num_files,
        );
        let l0_compaction = L0Compaction {
            safe_ts: self.get_keyspace_gc_safepoint_v2(&req.outer_start),
            l0_tables: l0_tbls,
            multi_cf_l1_tables: multi_cfs_l1_tbls,
            sst_config,
            bt_config,
        };
        req.compaction_tp = CompactionType::L0(l0_compaction);
        info!("start compact L0 for {}", tag);
        Some(self.comp_client.compact(req))
    }

    fn trigger_l1_plus_compaction(
        &self,
        shard: &Shard,
        cf: isize,
        level: usize,
        id_ver: IdVer,
    ) -> Option<Result<pb::ChangeSet>> {
        let tag = shard.tag();
        let data = shard.get_data();
        let scf = data.get_cf(cf as usize);
        let upper_level = &scf.levels[level - 1];
        let lower_level = &scf.levels[level];
        if upper_level.tables.len() == 0 {
            store_bool(&shard.compacting, false);
            return None;
        }
        let upper_level_candidates =
            if upper_level.has_over_bound_data(shard.inner_start(), shard.inner_end()) {
                if upper_level.tables.first().unwrap().smallest() < shard.inner_start() {
                    Arc::new(vec![upper_level.tables.first().unwrap().clone()])
                } else {
                    Arc::new(vec![upper_level.tables.last().unwrap().clone()])
                }
            } else {
                upper_level.tables.clone()
            };

        let sum_tbl_size = |tbls: &[sstable::SsTable]| tbls.iter().map(|tbl| tbl.size()).sum();

        let calc_ratio = |upper_size: u64, lower_size: u64| {
            if lower_size == 0 {
                return upper_size as f64;
            }
            upper_size as f64 / lower_size as f64
        };
        // First pick one table has max topSize/bottomSize ratio.
        let mut candidate_ratio = 0f64;
        let mut upper_left_idx = 0;
        let mut upper_right_idx = 0;
        let mut upper_size = 0;
        let mut lower_left_idx = 0;
        let mut lower_right_idx = 0;
        let mut lower_size = 0;
        for (i, tbl) in upper_level_candidates.iter().enumerate() {
            let (left, right) =
                get_tables_in_range(&lower_level.tables, tbl.smallest(), tbl.biggest());
            let new_lower_size: u64 = sum_tbl_size(&lower_level.tables[left..right]);
            let ratio = calc_ratio(tbl.size(), new_lower_size);
            if candidate_ratio < ratio {
                candidate_ratio = ratio;
                upper_left_idx = i;
                upper_right_idx = i + 1;
                upper_size = tbl.size();
                lower_left_idx = left;
                lower_right_idx = right;
                lower_size = new_lower_size;
            }
        }
        if upper_left_idx == upper_right_idx {
            store_bool(&shard.compacting, false);
            return None;
        }
        // Expand to left to include more tops as long as the ratio doesn't decrease and
        // the total size do not exceed maxCompactionExpandSize.
        let cur_upper_left_idx = upper_left_idx;
        for i in (0..cur_upper_left_idx).rev() {
            let t = &upper_level_candidates[i];
            let (left, right) = get_tables_in_range(&lower_level.tables, t.smallest(), t.biggest());
            if right < lower_left_idx {
                // A bottom table is skipped, we can compact in another run.
                break;
            }
            let new_upper_size = t.size() + upper_size;
            let new_lower_size =
                sum_tbl_size(&lower_level.tables[left..lower_left_idx]) + lower_size;
            let new_ratio = calc_ratio(new_upper_size, new_lower_size);
            if new_ratio > candidate_ratio {
                upper_left_idx -= 1;
                lower_left_idx = left;
                upper_size = new_upper_size;
                lower_size = new_lower_size;
            } else {
                break;
            }
        }
        // Expand to right to include more tops as long as the ratio doesn't decrease
        // and the total size do not exceeds maxCompactionExpandSize.
        let cur_upper_right_idx = upper_right_idx;
        for i in cur_upper_right_idx..upper_level_candidates.len() {
            let t = &upper_level_candidates[i];
            let (left, right) = get_tables_in_range(&lower_level.tables, t.smallest(), t.biggest());
            if left > lower_right_idx {
                // A bottom table is skipped, we can compact in another run.
                break;
            }
            let new_upper_size = t.size() + upper_size;
            let new_lower_size =
                sum_tbl_size(&lower_level.tables[lower_right_idx..right]) + lower_size;
            let new_ratio = calc_ratio(new_upper_size, new_lower_size);
            if new_ratio > candidate_ratio
                && (new_upper_size + new_lower_size) < MAX_COMPACTION_EXPAND_SIZE
            {
                upper_right_idx += 1;
                lower_right_idx = right;
                upper_size = new_upper_size;
                lower_size = new_lower_size;
            } else {
                break;
            }
        }
        let upper_level_table_ids: Vec<_> = upper_level_candidates[upper_left_idx..upper_right_idx]
            .iter()
            .map(|t| t.id())
            .collect();
        let lower_level_table_ids: Vec<_> = lower_level.tables[lower_left_idx..lower_right_idx]
            .iter()
            .map(|t| t.id())
            .collect();
        if lower_level_table_ids.is_empty() && cf as usize == WRITE_CF {
            info!("move down L{} CF{} for {}", level, cf, tag);
            // Move down. only write CF benefits from this optimization.
            let mut comp = pb::Compaction::new();
            comp.set_cf(cf as i32);
            comp.set_level(level as u32);
            comp.set_top_deletes(upper_level_table_ids);
            let tbl_creates = upper_level_candidates[upper_left_idx..upper_right_idx]
                .iter()
                .map(|top_tbl| {
                    let mut tbl_create = pb::TableCreate::new();
                    tbl_create.set_id(top_tbl.id());
                    tbl_create.set_cf(cf as i32);
                    tbl_create.set_level(level as u32 + 1);
                    tbl_create.set_smallest(top_tbl.smallest().to_vec());
                    tbl_create.set_biggest(top_tbl.biggest().to_vec());
                    tbl_create
                })
                .collect::<Vec<_>>();
            comp.set_table_creates(tbl_creates.into());
            let mut cs = new_change_set(id_ver.id, id_ver.ver);
            cs.set_compaction(comp);
            return Some(Ok(cs));
        }
        let mut has_overlap = false;
        let mut kr = KeyRange::default();
        // The range for overlapping check should include both upper level and lower
        // level.
        kr.update(&upper_level_candidates[upper_left_idx..upper_right_idx]);
        kr.update(&lower_level.tables[lower_left_idx..lower_right_idx]);
        for lvl_idx in (level + 1)..scf.levels.len() {
            let lh = &scf.levels[lvl_idx];
            let (left, right) = lh.overlapping_tables(&kr);
            if left < right {
                has_overlap = true;
            }
        }
        let mut req = self.new_compact_request_with_shard(shard, cf, level);
        req.overlap = has_overlap;
        req.tops = upper_level_table_ids.clone();
        req.bottoms = lower_level_table_ids.clone();
        req.cf = cf;
        req.level = level;
        let sst_config = self.opts.table_builder_options;
        let estimated_num_files = (upper_size + lower_size) as usize / sst_config.max_table_size;
        self.set_alloc_ids_for_request(
            &mut req,
            upper_level_table_ids.len() + lower_level_table_ids.len(),
            estimated_num_files,
        );
        let l1_plus: L1PlusCompaction = L1PlusCompaction {
            cf,
            level,
            safe_ts: self.get_keyspace_gc_safepoint_v2(&req.outer_start),
            keep_latest_obsolete_tombstone: has_overlap,
            upper_level: upper_level_table_ids,
            lower_level: lower_level_table_ids,
            sst_config,
        };
        req.compaction_tp = CompactionType::L1Plus(l1_plus);
        info!(
            "start compact L{} CF{} for {}, num_ids: {}, input_size: {}",
            level,
            cf,
            tag,
            req.file_ids.len(),
            upper_size + lower_size,
        );
        Some(self.comp_client.compact(req))
    }

    pub(crate) fn trigger_major_compacton(&self, shard: &Shard) -> Option<Result<pb::ChangeSet>> {
        let mut req = self.new_compact_request_with_shard(shard, 0, 0);
        let mut ln_tables: HashMap<usize, Vec<(usize, Vec<u64>)>> = HashMap::new();
        let mut total_size = 0;
        let data = shard.get_data();
        let mut num_ln_files = 0;
        for (cf, shard_cf) in data.cfs.iter().enumerate() {
            let mut ssts_for_cf = vec![];
            for lh in &shard_cf.levels {
                if lh.tables.is_empty() {
                    continue;
                }
                num_ln_files += lh.tables.len();
                ssts_for_cf.push((lh.level, lh.tables.iter().map(|t| t.id()).collect()));
                total_size += lh.tables.iter().map(|t| t.size()).sum::<u64>();
            }
            ln_tables.insert(cf, ssts_for_cf);
        }
        total_size += data.l0_tbls.iter().map(|t| t.size()).sum::<u64>();
        total_size += data.blob_tbl_map.values().map(|t| t.size()).sum::<u64>();
        let sst_config = self.opts.table_builder_options;
        let bt_config = self.get_blob_table_build_options_or_none(shard);
        let estimated_num_files = total_size as usize
            / std::cmp::min(
                bt_config.map_or(usize::MAX, |bt_config| bt_config.max_blob_table_size),
                sst_config.max_table_size,
            );
        self.set_alloc_ids_for_request(
            &mut req,
            data.l0_tbls.len() + num_ln_files + data.blob_tbl_map.len(),
            estimated_num_files,
        );
        let major_compaction = MajorCompaction {
            safe_ts: self.get_keyspace_gc_safepoint_v2(&req.outer_start),
            l0_tables: data.l0_tbls.iter().map(|t| t.id()).collect(),
            ln_tables,
            blob_tables: data.blob_tbl_map.keys().copied().collect(),
            sst_config,
            bt_config,
        };
        req.compaction_tp = CompactionType::Major(major_compaction);
        info!(
            "start major compact for {}, num_ids: {}, input_size: {}",
            shard.tag(),
            req.file_ids.len(),
            total_size,
        );
        Some(self.comp_client.compact(req))
    }

    pub(crate) fn handle_compact_response(&self, cs: pb::ChangeSet) {
        self.meta_change_listener.on_change_set(cs);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum CompactionPriority {
    L0 { score: f64 },
    L1Plus { cf: isize, score: f64, level: usize },
    Major { score: f64 },
    DestroyRange,
    TruncateTs,
    TrimOverBound,
}

impl CompactionPriority {
    pub(crate) fn score(&self) -> f64 {
        match self {
            CompactionPriority::L0 { score } => *score,
            CompactionPriority::L1Plus { score, .. } => *score,
            CompactionPriority::Major { score } => *score,
            CompactionPriority::DestroyRange => f64::MAX,
            CompactionPriority::TruncateTs => f64::MAX,
            CompactionPriority::TrimOverBound => f64::MAX,
        }
    }

    pub(crate) fn level(&self) -> isize {
        match self {
            CompactionPriority::L0 { .. } => 0,
            CompactionPriority::L1Plus { level, .. } => *level as isize,
            CompactionPriority::Major { .. } => -1,
            CompactionPriority::DestroyRange => -1,
            CompactionPriority::TruncateTs => -1,
            CompactionPriority::TrimOverBound => -1,
        }
    }

    pub(crate) fn cf(&self) -> isize {
        match self {
            CompactionPriority::L0 { .. } => -1,
            CompactionPriority::L1Plus { cf, .. } => *cf,
            CompactionPriority::Major { .. } => -1,
            CompactionPriority::DestroyRange => -1,
            CompactionPriority::TruncateTs => -1,
            CompactionPriority::TrimOverBound => -1,
        }
    }
}

impl Ord for CompactionPriority {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.score()
            .partial_cmp(&other.score())
            .unwrap_or(CmpOrdering::Equal)
    }
}

impl PartialOrd for CompactionPriority {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        self.score().partial_cmp(&other.score())
    }
}

impl PartialEq for CompactionPriority {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == CmpOrdering::Equal
    }
}

impl Eq for CompactionPriority {}

#[derive(Clone, Default)]
pub(crate) struct KeyRange {
    pub left: Bytes,
    pub right: Bytes,
}

impl KeyRange {
    pub(crate) fn left_key(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(&self.left)
    }

    pub(crate) fn right_key(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(&self.right)
    }

    pub(crate) fn update(&mut self, tables: &[SsTable]) {
        if tables.is_empty() {
            return;
        }
        let lower_smallest = tables.first().unwrap().smallest();
        if self.left.is_empty() || lower_smallest < self.left_key() {
            self.left = Bytes::copy_from_slice(lower_smallest.deref());
        }
        let lower_biggest = tables.last().unwrap().biggest();
        if lower_biggest > self.right_key() {
            self.right = Bytes::copy_from_slice(lower_biggest.deref());
        }
    }
}

pub(crate) fn get_tables_in_range(
    tables: &[SsTable],
    start: InnerKey<'_>,
    end: InnerKey<'_>,
) -> (usize, usize) {
    let left = search(tables.len(), |i| start <= tables[i].biggest());
    let right = search(tables.len(), |i| end < tables[i].smallest());
    (left, right)
}

pub(crate) fn compact_l0(ctx: &CompactionCtx) -> Result<Vec<pb::TableCreate>> {
    let req = &ctx.req;
    let fs = &ctx.dfs;
    let compression_lvl = ctx.compression_lvl;
    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let l0_files = load_table_files(&req.tops, fs.clone(), opts)?;
    let mut l0_tbls = in_mem_files_to_l0_tables(l0_files, ctx.encryption_key.clone());
    l0_tbls.sort_by(|a, b| b.version().cmp(&a.version()));
    let mut mult_cf_bot_tbls = vec![];
    for cf in 0..NUM_CFS {
        let bot_ids = &req.multi_cf_bottoms[cf];
        let bot_files = load_table_files(bot_ids, fs.clone(), opts)?;
        let mut bot_tbls = in_mem_files_to_tables(bot_files, ctx.encryption_key.clone());
        bot_tbls.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
        mult_cf_bot_tbls.push(bot_tbls);
    }

    let channel_cap = req.file_ids.len();
    let (tx, rx) = tikv_util::mpsc::bounded(channel_cap);
    let mut id_idx = 0;
    for cf in 0..NUM_CFS {
        let mut iter = build_compact_l0_iterator(
            cf,
            l0_tbls.clone(),
            std::mem::take(&mut mult_cf_bot_tbls[cf]),
            req.inner_start(),
        );
        let mut helper = CompactL0Helper::new(cf, req, compression_lvl, ctx.encryption_key.clone());
        loop {
            if id_idx >= req.file_ids.len() {
                panic!(
                    "index out of bounds: the len is {} but the index is {}, req {:?}",
                    req.file_ids.len(),
                    id_idx,
                    req
                );
            }
            let id = req.file_ids[id_idx];
            let (tbl_create, data) = helper.build_one(&mut iter, id)?;
            if data.is_empty() {
                break;
            }
            let afs = fs.clone();
            let atx = tx.clone();
            fs.get_runtime().spawn(async move {
                atx.send(
                    afs.create(tbl_create.id, data, opts)
                        .await
                        .map(|_| tbl_create),
                )
                .unwrap();
            });
            id_idx += 1;
        }
    }
    let mut table_creates = vec![];
    let count = id_idx;
    let mut errors = vec![];
    for _ in 0..count {
        match rx.recv().unwrap() {
            Err(err) => errors.push(err),
            Ok(tbl_create) => table_creates.push(tbl_create),
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    Ok(table_creates)
}

fn load_table_files(
    tbl_ids: &[u64],
    fs: Arc<dyn dfs::Dfs>,
    opts: dfs::Options,
) -> Result<Vec<InMemFile>> {
    let mut files = vec![];
    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<(u64, Bytes)>>(tbl_ids.len());
    for id in tbl_ids {
        let aid = *id;
        let atx = tx.clone();
        let afs = fs.clone();
        fs.get_runtime().spawn(async move {
            let res = afs
                .read_file(aid, opts)
                .await
                .map(|data| (aid, data))
                .map_err(|e| Error::DfsError(e));
            atx.send(res).map_err(|_| "send file data failed").unwrap();
        });
    }
    let mut errors = vec![];
    for _ in tbl_ids {
        match rx.recv().unwrap() {
            Err(err) => errors.push(err),
            Ok((id, data)) => {
                let file = InMemFile::new(id, data);
                files.push(file);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap());
    }
    Ok(files)
}

fn in_mem_files_to_l0_tables(
    files: Vec<InMemFile>,
    encryption_key: Option<EncryptionKey>,
) -> Vec<sstable::L0Table> {
    files
        .into_iter()
        .map(|f| sstable::L0Table::new(Arc::new(f), None, false, encryption_key.clone()).unwrap())
        .collect()
}

fn in_mem_files_to_tables(
    files: Vec<InMemFile>,
    encryption_key: Option<EncryptionKey>,
) -> Vec<sstable::SsTable> {
    files
        .into_iter()
        .map(|f| sstable::SsTable::new(Arc::new(f), None, false, encryption_key.clone()).unwrap())
        .collect()
}

fn build_compact_l0_iterator(
    cf: usize,
    top_tbls: Vec<sstable::L0Table>,
    bot_tbls: Vec<sstable::SsTable>,
    start: InnerKey<'_>,
) -> Box<dyn table::Iterator> {
    let mut iters: Vec<Box<dyn table::Iterator>> = vec![];
    for top_tbl in top_tbls {
        if let Some(tbl) = top_tbl.get_cf(cf) {
            let iter = tbl.new_iterator(false, false);
            iters.push(iter);
        }
    }
    if !bot_tbls.is_empty() {
        let iter = ConcatIterator::new_with_tables(bot_tbls, false, false);
        iters.push(Box::new(iter));
    }
    let mut iter = table::new_merge_iterator(iters, false);
    iter.seek(start);
    iter
}

struct CompactL0Helper {
    cf: usize,
    builder: sstable::Builder,
    last_key: BytesMut,
    skip_key: BytesMut,
    safe_ts: u64,
    max_table_size: usize,
    end: Vec<u8>,
}

impl CompactL0Helper {
    fn new(
        cf: usize,
        req: &CompactionRequest,
        compression_lvl: i32,
        encryption_key: Option<EncryptionKey>,
    ) -> Self {
        Self {
            cf,
            builder: sstable::Builder::new(
                0,
                req.block_size,
                req.compression_tp,
                compression_lvl,
                encryption_key,
            ),
            last_key: BytesMut::new(),
            skip_key: BytesMut::new(),
            safe_ts: req.safe_ts,
            max_table_size: req.max_table_size,
            end: req.inner_end().to_vec(),
        }
    }

    fn build_one<'a>(
        &mut self,
        iter: &mut Box<dyn table::Iterator + 'a>,
        id: u64,
    ) -> Result<(pb::TableCreate, Bytes)> {
        self.builder.reset(id);
        self.last_key.clear();
        self.skip_key.clear();

        while iter.valid() {
            let key = iter.key();
            // See if we need to skip this key.
            if !self.skip_key.is_empty() {
                if key.deref() == self.skip_key {
                    iter.next_all_version();
                    continue;
                } else {
                    self.skip_key.clear();
                }
            }
            if key.deref() != self.last_key {
                // We only break on table size.
                if self.builder.estimated_size() > self.max_table_size {
                    break;
                }
                if key.deref() >= self.end.as_slice() {
                    break;
                }
                self.last_key.clear();
                self.last_key.extend_from_slice(key.deref());
            }

            // Only consider the versions which are below the safeTS, otherwise, we might
            // end up discarding the only valid version for a running
            // transaction.
            let val = iter.value();
            if val.version <= self.safe_ts {
                // key is the latest readable version of this key, so we simply discard all the
                // rest of the versions.
                self.skip_key.clear();
                self.skip_key.extend_from_slice(key.deref());
                if !val.is_deleted() {
                    match filter(self.safe_ts, self.cf, val) {
                        Decision::Keep => {}
                        Decision::Drop => {
                            iter.next_all_version();
                            continue;
                        }
                        Decision::MarkTombStone => {
                            // There may have old versions for this key, so convert to delete
                            // tombstone.
                            self.builder
                                .add(key, &table::Value::new_tombstone(val.version), None);
                            iter.next_all_version();
                            continue;
                        }
                    }
                }
            }
            self.builder.add(key, &val, None);
            iter.next_all_version();
        }
        if self.builder.is_empty() {
            return Ok((pb::TableCreate::new(), Bytes::new()));
        }
        let mut buf = Vec::with_capacity(self.builder.estimated_size());
        let res = self.builder.finish(0, &mut buf);
        let mut table_create = pb::TableCreate::new();
        table_create.set_id(id);
        table_create.set_level(1);
        table_create.set_cf(self.cf as i32);
        table_create.set_smallest(res.smallest);
        table_create.set_biggest(res.biggest);
        Ok((table_create, buf.into()))
    }
}

pub(crate) fn compact_tables(ctx: &CompactionCtx) -> Result<Vec<pb::TableCreate>> {
    let req = &ctx.req;
    let fs = &ctx.dfs;
    let compression_lvl = ctx.compression_lvl;
    let id_allocator = &ctx.id_allocator;
    let tag = ShardTag::from_comp_req(req);
    info!(
        "{} compact req tops {:?}, bots {:?}",
        tag, &req.tops, &req.bottoms
    );
    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let top_files = load_table_files(&req.tops, fs.clone(), opts)?;
    let mut top_tables = in_mem_files_to_tables(top_files, ctx.encryption_key.clone());
    top_tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
    let bot_files = load_table_files(&req.bottoms, fs.clone(), opts)?;
    let mut bot_tables = in_mem_files_to_tables(bot_files, ctx.encryption_key.clone());
    bot_tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
    let top_iter = Box::new(ConcatIterator::new_with_tables(top_tables, false, false));
    let bot_iter = Box::new(ConcatIterator::new_with_tables(bot_tables, false, false));
    let mut iter = table::new_merge_iterator(vec![top_iter, bot_iter], false);
    iter.seek(req.inner_start());

    let mut last_key = BytesMut::new();
    let mut skip_key = BytesMut::new();
    let mut builder = sstable::Builder::new(
        0,
        req.block_size,
        req.compression_tp,
        compression_lvl,
        ctx.encryption_key.clone(),
    );
    let mut id_idx = 0;
    let mut file_ids = req.file_ids.clone();
    let (tx, rx) = tikv_util::mpsc::bounded(file_ids.len());
    let mut reach_end = false;
    while iter.valid() && !reach_end {
        if id_idx >= file_ids.len() {
            if id_idx > 4096 {
                panic!(
                    "index out of bounds: the len is {} but the index is {}, req {:?}",
                    file_ids.len(),
                    id_idx,
                    req
                );
            }
            let new_ids = id_allocator.alloc_id(64)?;
            file_ids.extend_from_slice(new_ids.as_slice());
        }
        let id = file_ids[id_idx];
        builder.reset(id);
        last_key.clear();
        while iter.valid() {
            let val = iter.value();
            let key = iter.key();
            let kv_size = val.encoded_size() + key.len();
            // See if we need to skip this key.
            if !skip_key.is_empty() {
                if key.deref() == skip_key {
                    iter.next_all_version();
                    continue;
                } else {
                    skip_key.clear();
                }
            }
            if key.deref() != last_key {
                if !last_key.is_empty() && builder.estimated_size() + kv_size > req.max_table_size {
                    break;
                }
                if key >= req.inner_end() {
                    reach_end = true;
                    break;
                }
                last_key.clear();
                last_key.extend_from_slice(key.deref());
            }

            // Only consider the versions which are below the minReadTs, otherwise, we might
            // end up discarding the only valid version for a running
            // transaction.
            if req.cf as usize == LOCK_CF || val.version <= req.safe_ts {
                // key is the latest readable version of this key, so we simply discard all the
                // rest of the versions.
                skip_key.clear();
                skip_key.extend_from_slice(key.deref());

                if val.is_deleted() {
                    // If this key range has overlap with lower levels, then keep the deletion
                    // marker with the latest version, discarding the rest. We have set skipKey,
                    // so the following key versions would be skipped. Otherwise discard the
                    // deletion marker.
                    if !req.overlap {
                        iter.next_all_version();
                        continue;
                    }
                } else {
                    match filter(req.safe_ts, req.cf as usize, val) {
                        Decision::Keep => {}
                        Decision::Drop => {
                            iter.next_all_version();
                            continue;
                        }
                        Decision::MarkTombStone => {
                            if req.overlap {
                                // There may be old versions for this key, so convert to delete
                                // tombstone.
                                builder.add(key, &table::Value::new_tombstone(val.version), None);
                            }
                            iter.next_all_version();
                            continue;
                        }
                    }
                }
            }
            builder.add(key, &val, None);
            iter.next_all_version();
        }
        if builder.is_empty() {
            continue;
        }
        let mut buf = Vec::with_capacity(builder.estimated_size());
        let res = builder.finish(0, &mut buf);
        let mut tbl_create = pb::TableCreate::new();
        tbl_create.set_id(id);
        tbl_create.set_cf(req.cf as i32);
        tbl_create.set_level(req.level as u32 + 1);
        tbl_create.set_smallest(res.smallest);
        tbl_create.set_biggest(res.biggest);
        let afs = fs.clone();
        let atx = tx.clone();
        fs.get_runtime().spawn(async move {
            atx.send(
                afs.create(tbl_create.id, buf.into(), opts)
                    .await
                    .map(|_| tbl_create),
            )
            .unwrap();
        });
        id_idx += 1;
    }
    let cnt = id_idx;
    let mut tbl_creates = vec![];
    let mut errors = vec![];
    for _ in 0..cnt {
        match rx.recv().unwrap() {
            Err(err) => errors.push(err),
            Ok(tbl_create) => tbl_creates.push(tbl_create),
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    Ok(tbl_creates)
}

enum Decision {
    Keep,
    MarkTombStone,
    Drop,
}

// filter implements the badger.CompactionFilter interface.
// Since we use txn ts as badger version, we only need to filter Delete,
// Rollback and Op_Lock. It is called for the first valid version before safe
// point, older versions are discarded automatically.
fn filter(safe_ts: u64, cf: usize, val: table::Value) -> Decision {
    let user_meta = val.user_meta();
    if cf == WRITE_CF {
        if user_meta.len() == USER_META_SIZE {
            let um = UserMeta::from_slice(user_meta);
            if um.commit_ts < safe_ts && val.is_value_empty() {
                return Decision::MarkTombStone;
            }
        }
    } else if cf == LOCK_CF {
        return Decision::Keep;
    } else {
        assert_eq!(cf, EXTRA_CF);
        if user_meta.len() == USER_META_SIZE {
            let um = UserMeta::from_slice(user_meta);
            if um.start_ts < safe_ts {
                return Decision::Drop;
            }
        }
    }
    // Older version are discarded automatically, we need to keep the first valid
    // version.
    Decision::Keep
}

fn load_blob_tables(
    fs: Arc<dyn dfs::Dfs>,
    blob_table_ids: &[u64],
    opts: dfs::Options,
) -> Result<HashMap<u64, BlobTable>> {
    let mut blob_tables = HashMap::new();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<(u64, Bytes)>>(blob_table_ids.len());
    for id in blob_table_ids {
        let aid = *id;
        let atx = tx.clone();
        let afs = fs.clone();
        fs.get_runtime().spawn(async move {
            let res = afs
                .read_file(aid, opts)
                .await
                .map(|data| (aid, data))
                .map_err(|e| Error::DfsError(e));
            atx.send(res).map_err(|_| "send file data failed").unwrap();
        });
    }
    let mut errors = vec![];
    for _ in blob_table_ids {
        match rx.recv().unwrap() {
            Err(err) => errors.push(err),
            Ok((id, data)) => {
                blob_tables.insert(id, BlobTable::from_bytes(data)?);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap());
    }
    Ok(blob_tables)
}

pub async fn handle_remote_compaction(
    dfs: Arc<dyn dfs::Dfs>,
    req: hyper::Request<hyper::Body>,
    compression_lvl: i32,
    id_allocator: Arc<dyn IdAllocator>,
    master_key: MasterKey,
) -> hyper::Result<hyper::Response<hyper::Body>> {
    let req_body = hyper::body::to_bytes(req.into_body()).await?;
    let result = serde_json::from_slice(req_body.chunk());
    if result.is_err() {
        let err_str = result.unwrap_err().to_string();
        return Ok(hyper::Response::builder()
            .status(400)
            .body(err_str.into())
            .unwrap());
    }
    let comp_req: CompactionRequest = result.unwrap();

    if comp_req.compactor_version > CURRENT_COMPACTOR_VERSION {
        warn!(
            "received incompatible compactor-version({}). Upgrade tikv-worker (version:{}). Request: {:?}",
            comp_req.compactor_version, CURRENT_COMPACTOR_VERSION, comp_req,
        );
        let err_str = format!("incompatible compactor version ({CURRENT_COMPACTOR_VERSION})");
        return Ok(hyper::Response::builder()
            .status(INCOMPATIBLE_COMPACTOR_ERROR_CODE)
            .body(err_str.into())
            .unwrap());
    }
    let encryption_key = if comp_req.exported_encryption_key.is_empty() {
        None
    } else {
        Some(
            master_key
                .decrypt_encryption_key(&comp_req.exported_encryption_key)
                .unwrap(),
        )
    };
    let ctx = CompactionCtx {
        req: Arc::new(comp_req),
        dfs,
        compression_lvl,
        id_allocator,
        encryption_key,
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        tikv_util::set_current_region(ctx.req.shard_id);
        if ctx.req.compactor_version >= 3 {
            let result = local_compact_v3(&ctx);
            tx.send(result).unwrap();
            return;
        }
        let result = local_compact(&ctx);
        tx.send(result).unwrap();
    });
    match rx.await.unwrap() {
        Ok(cs) => {
            let data = cs.write_to_bytes().unwrap();
            Ok(hyper::Response::builder()
                .status(200)
                .body(data.into())
                .unwrap())
        }
        Err(err) => {
            let err_str = format!("{:?}", err);
            error!("compaction failed {}", err_str);
            let body = hyper::Body::from(err_str);
            Ok(hyper::Response::builder().status(500).body(body).unwrap())
        }
    }
}

pub(crate) struct CompactionCtx {
    pub(crate) req: Arc<CompactionRequest>,
    pub(crate) dfs: Arc<dyn dfs::Dfs>,
    pub(crate) compression_lvl: i32,
    pub(crate) id_allocator: Arc<dyn IdAllocator>,
    pub(crate) encryption_key: Option<EncryptionKey>,
}

fn local_compact(ctx: &CompactionCtx) -> Result<pb::ChangeSet> {
    let req = &ctx.req;
    let mut cs = pb::ChangeSet::new();
    cs.set_shard_id(req.shard_id);
    cs.set_shard_ver(req.shard_ver);
    let tag = ShardTag::from_comp_req(req);
    if req.destroy_range {
        info!("start destroying range for {}", tag);
        let tc = compact_destroy_range(
            ctx,
            req.block_size,
            &req.in_place_compact_files,
            &req.del_prefixes,
        )?;
        cs.set_destroy_range(tc);
        info!("finish destroying range for {}", tag);
        return Ok(cs);
    }

    if let Some(truncate_ts) = req.truncate_ts {
        info!("start truncate ts({}) for {}", truncate_ts, tag);
        let tc = compact_truncate_ts(
            ctx,
            req.block_size,
            &req.in_place_compact_files,
            truncate_ts,
        )?;
        info!(
            "finish truncate ts({}) for {}, create:{}, delete:{}",
            truncate_ts,
            tag,
            tc.get_table_creates().len(),
            tc.get_table_deletes().len()
        );
        cs.set_truncate_ts(tc);
        return Ok(cs);
    }

    if req.trim_over_bound {
        info!("start trim_over_bound for {}", tag);
        let tc = compact_trim_over_bound(ctx, req.block_size, &req.in_place_compact_files)?;
        cs.set_trim_over_bound(tc);
        info!("finish trim_over_bound for {}", tag);
        return Ok(cs);
    }

    let mut comp = pb::Compaction::new();
    comp.set_top_deletes(req.tops.clone());
    comp.set_cf(req.cf as i32);
    comp.set_level(req.level as u32);
    if req.level == 0 {
        info!("start compact L0 for {}", tag);
        let tbls = compact_l0(ctx)?;
        comp.set_table_creates(tbls.into());
        let bot_dels = req.multi_cf_bottoms.clone().into_iter().flatten().collect();
        comp.set_bottom_deletes(bot_dels);
        info!("finish compacting L0 for {}", tag);
    } else {
        info!("start compacting L{} CF{} for {}", req.level, req.cf, tag);
        let tbls = compact_tables(ctx)?;
        comp.set_table_creates(tbls.into());
        comp.set_bottom_deletes(req.bottoms.clone());
        info!("finish compacting L{} CF{} for {}", req.level, req.cf, tag);
    }
    cs.set_compaction(comp);
    Ok(cs)
}

fn local_compact_v3(ctx: &CompactionCtx) -> Result<pb::ChangeSet> {
    let req = &ctx.req;
    let mut cs = pb::ChangeSet::new();
    cs.set_shard_id(req.shard_id);
    cs.set_shard_ver(req.shard_ver);
    let tag = ShardTag::from_comp_req(req);
    info!("start compaction for {}, req {:?}", tag, req);
    let mut file_ids = req.file_ids.clone();
    let mut num_files_quota = if 4096 > file_ids.len() {
        4096 - req.file_ids.len()
    } else {
        0
    };
    let id_allocator = ctx.id_allocator.clone();
    let mut allocate_id = move || {
        if file_ids.is_empty() && num_files_quota > 0 {
            let allocated = id_allocator
                .alloc_id(std::cmp::min(64, num_files_quota))
                .unwrap_or_else(|e| {
                    panic!("failed to allocate id: {:?}", e);
                });
            file_ids.extend_from_slice(&allocated);
            num_files_quota -= allocated.len();
        }
        file_ids.pop().unwrap_or_else(|| {
            panic!("compaction runs out of file ids");
        })
    };
    match &req.compaction_tp {
        CompactionType::InPlace {
            file_ids,
            block_size,
            spec,
        } => match spec {
            InPlaceCompaction::DestroyRange(del_prefix) => {
                cs.set_destroy_range(compact_destroy_range(
                    ctx,
                    *block_size,
                    file_ids,
                    del_prefix,
                )?);
            }
            InPlaceCompaction::TrimOverBound => {
                cs.set_trim_over_bound(compact_trim_over_bound(ctx, *block_size, file_ids)?);
            }
            InPlaceCompaction::TruncateTs(truncate_ts) => {
                cs.set_truncate_ts(compact_truncate_ts(
                    ctx,
                    *block_size,
                    file_ids,
                    *truncate_ts,
                )?);
            }
            InPlaceCompaction::Unknown => unreachable!(),
        },
        CompactionType::L0(l0_compaction) => {
            cs.set_compaction(l0_compact_v3(ctx, l0_compaction, &mut allocate_id)?);
        }
        CompactionType::L1Plus(l1_plus_compaction) => {
            cs.set_compaction(l1_plus_compact_v3(
                ctx,
                l1_plus_compaction,
                &mut allocate_id,
            )?);
        }
        CompactionType::Major(major_compaction) => {
            cs.set_major_compaction(major_compact_v3(ctx, major_compaction, &mut allocate_id)?);
        }
        CompactionType::Unknown => unreachable!(),
    }
    Ok(cs)
}

/// Compact files in place to remove data covered by delete prefixes.
fn compact_destroy_range(
    ctx: &CompactionCtx,
    block_size: usize,
    files: &Vec<(u64, u32, i32)>,
    del_prefix: &Vec<u8>,
) -> Result<pb::TableChange> {
    let req = &ctx.req;
    let dfs = &ctx.dfs;
    let compression_lvl = ctx.compression_lvl;
    assert!(!del_prefix.is_empty() && !files.is_empty());
    assert_eq!(files.len(), req.file_ids.len());

    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let mut in_mem_files: HashMap<u64, InMemFile> = load_table_files(
        &files.iter().map(|(id, ..)| *id).collect::<Vec<_>>(),
        dfs.clone(),
        opts,
    )?
    .into_iter()
    .map(|file| (file.id, file))
    .collect();

    let mut deletes = vec![];
    let mut creates = vec![];
    let del_prefixes = DeletePrefixes::unmarshal(del_prefix, req.inner_key_off);
    let (tx, rx) = tikv_util::mpsc::bounded(req.file_ids.len());
    for (&(id, level, cf), &new_id) in files.iter().zip(req.file_ids.iter()) {
        let mut delete = pb::TableDelete::new();
        delete.set_id(id);
        delete.set_level(level);
        delete.set_cf(cf);
        deletes.push(delete);
        let file = in_mem_files.remove(&id).unwrap();
        let (data, smallest, biggest) = if level == 0 {
            let t = sstable::L0Table::new(Arc::new(file), None, false, ctx.encryption_key.clone())
                .unwrap();
            let mut builder =
                L0Builder::new(new_id, block_size, t.version(), ctx.encryption_key.clone());
            for cf in 0..NUM_CFS {
                if let Some(cf_t) = t.get_cf(cf) {
                    let mut iter = cf_t.new_iterator(false, false);
                    iter.seek(req.inner_start());
                    while iter.valid() {
                        let key = iter.key();
                        if key >= req.inner_end() {
                            break;
                        }
                        if !del_prefixes.cover_prefix(key) {
                            builder.add(cf, key, &iter.value(), None);
                        }
                        iter.next_all_version();
                    }
                }
            }
            if builder.is_empty() {
                continue;
            }
            let data = builder.finish();
            let (smallest, biggest) = builder.smallest_biggest();
            (data, smallest.to_vec(), biggest.to_vec())
        } else {
            let t = sstable::SsTable::new(Arc::new(file), None, false, ctx.encryption_key.clone())
                .unwrap();
            let mut builder = sstable::Builder::new(
                new_id,
                block_size,
                t.compression_type(),
                compression_lvl,
                ctx.encryption_key.clone(),
            );
            let mut iter = t.new_iterator(false, false);
            iter.seek(req.inner_start());
            while iter.valid() {
                let key = iter.key();
                if key >= req.inner_end() {
                    break;
                }
                if !del_prefixes.cover_prefix(key) {
                    builder.add(key, &iter.value(), None);
                }
                iter.next_all_version();
            }
            if builder.is_empty() {
                continue;
            }
            let mut buf = Vec::with_capacity(builder.estimated_size());
            let res = builder.finish(0, &mut buf);
            (buf.into(), res.smallest, res.biggest)
        };
        let tx = tx.clone();
        let dfs_clone = dfs.clone();
        dfs.get_runtime().spawn(async move {
            tx.send(dfs_clone.create(new_id, data, opts).await).unwrap();
        });
        let mut create = pb::TableCreate::new();
        create.set_id(new_id);
        create.set_level(level);
        create.set_cf(cf);
        create.set_smallest(smallest);
        create.set_biggest(biggest);
        creates.push(create);
    }
    let mut errors = creates
        .iter()
        .filter_map(|_| match rx.recv().unwrap() {
            Ok(_) => None,
            Err(e) => Some(e),
        })
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    let mut destroy = pb::TableChange::new();
    destroy.set_table_deletes(deletes.into());
    destroy.set_table_creates(creates.into());
    Ok(destroy)
}

fn compact_truncate_ts(
    ctx: &CompactionCtx,
    block_size: usize,
    files: &Vec<(u64, u32, i32)>,
    truncate_ts: u64,
) -> Result<pb::TableChange> {
    let req = &ctx.req;
    let dfs = &ctx.dfs;
    let compression_lvl = ctx.compression_lvl;
    assert!(!files.is_empty());

    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let mut in_mem_files: HashMap<u64, InMemFile> = load_table_files(
        &files.iter().map(|(id, ..)| *id).collect::<Vec<_>>(),
        dfs.clone(),
        opts,
    )?
    .into_iter()
    .map(|file| (file.id, file))
    .collect();

    let mut deletes = vec![];
    let mut creates = vec![];
    let (tx, rx) = tikv_util::mpsc::bounded(req.file_ids.len());
    for (&(id, level, cf), &new_id) in files.iter().zip(req.file_ids.iter()) {
        let file = in_mem_files.remove(&id).unwrap();

        let mut delete = pb::TableDelete::new();
        delete.set_id(id);
        delete.set_level(level);
        delete.set_cf(cf);
        deletes.push(delete);

        let (data, smallest, biggest) = if level == 0 {
            let t = sstable::L0Table::new(Arc::new(file), None, false, ctx.encryption_key.clone())
                .unwrap();
            let mut builder =
                L0Builder::new(new_id, block_size, t.version(), ctx.encryption_key.clone());
            for cf in 0..NUM_CFS {
                if let Some(cf_t) = t.get_cf(cf) {
                    let mut iter = cf_t.new_iterator(false, false);
                    iter.rewind();
                    while iter.valid() {
                        let key = iter.key();
                        let value = iter.value();
                        // Keep data in LOCK_CF. Locks would be resolved by TiDB.
                        // TODO: handle async commit
                        if cf == LOCK_CF || value.version <= truncate_ts {
                            builder.add(cf, key, &value, None);
                        }
                        iter.next_all_version();
                    }
                }
            }
            if builder.is_empty() {
                continue;
            }
            let data = builder.finish();
            let (smallest, biggest) = builder.smallest_biggest();
            (data, smallest.to_vec(), biggest.to_vec())
        } else {
            let t = sstable::SsTable::new(Arc::new(file), None, false, ctx.encryption_key.clone())
                .unwrap();
            let mut builder = sstable::Builder::new(
                new_id,
                block_size,
                t.compression_type(),
                compression_lvl,
                ctx.encryption_key.clone(),
            );
            let mut iter = t.new_iterator(false, false);
            iter.rewind();
            while iter.valid() {
                let key = iter.key();
                let value = iter.value();
                // Keep data in LOCK_CF. Locks would be resolved by TiDB.
                // TODO: handle async commit
                if cf as usize == LOCK_CF || value.version <= truncate_ts {
                    builder.add(key, &value, None);
                }
                iter.next_all_version();
            }
            if builder.is_empty() {
                continue;
            }
            let mut buf = Vec::with_capacity(builder.estimated_size());
            let res = builder.finish(0, &mut buf);
            (buf.into(), res.smallest, res.biggest)
        };

        let tx = tx.clone();
        let dfs_clone = dfs.clone();
        dfs.get_runtime().spawn(async move {
            tx.send(dfs_clone.create(new_id, data, opts).await).unwrap();
        });

        let mut create = pb::TableCreate::new();
        create.set_id(new_id);
        create.set_level(level);
        create.set_cf(cf);
        create.set_smallest(smallest);
        create.set_biggest(biggest);
        creates.push(create);
    }

    let mut errors = creates
        .iter()
        .filter_map(|_| match rx.recv().unwrap() {
            Ok(_) => None,
            Err(e) => Some(e),
        })
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    let mut table_change = pb::TableChange::new();
    table_change.set_table_deletes(deletes.into());
    table_change.set_table_creates(creates.into());
    Ok(table_change)
}

/// Compact files in place to remove data out of shard bound.
fn compact_trim_over_bound(
    ctx: &CompactionCtx,
    block_size: usize,
    files: &Vec<(u64, u32, i32)>,
) -> Result<pb::TableChange> {
    let req = &ctx.req;
    let dfs = &ctx.dfs;
    let compression_lvl = ctx.compression_lvl;
    assert!(!files.is_empty());
    assert_eq!(files.len(), req.file_ids.len());

    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let mut in_mem_files: HashMap<u64, InMemFile> = load_table_files(
        &files.iter().map(|(id, ..)| *id).collect::<Vec<_>>(),
        dfs.clone(),
        opts,
    )?
    .into_iter()
    .map(|file| (file.id, file))
    .collect();

    let mut deletes = vec![];
    let mut creates = vec![];
    let (tx, rx) = tikv_util::mpsc::bounded(req.file_ids.len());
    for (&(id, level, cf), &new_id) in files.iter().zip(req.file_ids.iter()) {
        let file = in_mem_files.remove(&id).unwrap();

        let mut delete = pb::TableDelete::new();
        delete.set_id(id);
        delete.set_level(level);
        delete.set_cf(cf);
        deletes.push(delete);

        let (data, smallest, biggest) = if level == 0 {
            let t = sstable::L0Table::new(Arc::new(file), None, false, ctx.encryption_key.clone())
                .unwrap();
            let mut builder =
                L0Builder::new(new_id, block_size, t.version(), ctx.encryption_key.clone());
            for cf in 0..NUM_CFS {
                if let Some(cf_t) = t.get_cf(cf) {
                    let mut iter = cf_t.new_iterator(false, false);
                    iter.seek(req.inner_start());
                    while iter.valid() {
                        let key = iter.key();
                        if key >= req.inner_end() {
                            break;
                        }
                        builder.add(cf, key, &iter.value(), None);
                        iter.next_all_version();
                    }
                }
            }
            if builder.is_empty() {
                continue;
            }
            let data = builder.finish();
            let (smallest, biggest) = builder.smallest_biggest();
            (data, smallest.to_vec(), biggest.to_vec())
        } else {
            let t = sstable::SsTable::new(Arc::new(file), None, false, ctx.encryption_key.clone())
                .unwrap();
            let mut builder = sstable::Builder::new(
                new_id,
                block_size,
                t.compression_type(),
                compression_lvl,
                ctx.encryption_key.clone(),
            );
            let mut iter = t.new_iterator(false, false);
            iter.seek(req.inner_start());
            while iter.valid() {
                let key = iter.key();
                if key >= req.inner_end() {
                    break;
                }
                builder.add(key, &iter.value(), None);
                iter.next_all_version();
            }
            if builder.is_empty() {
                continue;
            }
            let mut buf = Vec::with_capacity(builder.estimated_size());
            let res = builder.finish(0, &mut buf);
            (buf.into(), res.smallest, res.biggest)
        };
        let tx = tx.clone();
        let dfs_clone = dfs.clone();
        dfs.get_runtime().spawn(async move {
            tx.send(dfs_clone.create(new_id, data, opts).await).unwrap();
        });

        let mut create = pb::TableCreate::new();
        create.set_id(new_id);
        create.set_level(level);
        create.set_cf(cf);
        create.set_smallest(smallest);
        create.set_biggest(biggest);
        creates.push(create);
    }

    let mut errors = creates
        .iter()
        .filter_map(|_| match rx.recv().unwrap() {
            Ok(_) => None,
            Err(e) => Some(e),
        })
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    let mut table_change = pb::TableChange::new();
    table_change.set_table_deletes(deletes.into());
    table_change.set_table_creates(creates.into());
    Ok(table_change)
}

enum FilePersistResult {
    SsTableCreate(pb::TableCreate),
    BlobTableCreate(pb::BlobCreate),
}

fn persist_sst(
    id: u64,
    cf: usize,
    target_lvl: u32,
    builder: &mut sstable::Builder,
    tx: mpsc::Sender<dfs::Result<FilePersistResult>>,
    fs: Arc<dyn dfs::Dfs>,
    opts: dfs::Options,
) {
    let mut buf = Vec::with_capacity(builder.estimated_size());
    let res = builder.finish(0, &mut buf);
    let mut tbl_create = pb::TableCreate::new();
    tbl_create.set_id(id);
    tbl_create.set_cf(cf as i32);
    tbl_create.set_level(target_lvl);
    tbl_create.set_smallest(res.smallest);
    tbl_create.set_biggest(res.biggest);

    let fs_clone = fs.clone();
    fs.get_runtime().spawn(async move {
        tx.send(
            fs_clone
                .create(id, buf.into(), opts)
                .await
                .map(|_| FilePersistResult::SsTableCreate(tbl_create)),
        )
        .unwrap();
    });
}

fn persist_blob_table(
    id: u64,
    builder: &mut BlobTableBuilder,
    tx: mpsc::Sender<dfs::Result<FilePersistResult>>,
    fs: Arc<dyn dfs::Dfs>,
    opts: dfs::Options,
) {
    let buf = builder.finish();
    let mut blob_table_create = pb::BlobCreate::new();
    blob_table_create.set_id(id);
    let (smallest, biggest) = builder.smallest_biggest_key();
    blob_table_create.set_smallest(smallest.to_vec());
    blob_table_create.set_biggest(biggest.to_vec());
    let fs_clone = fs.clone();
    fs.get_runtime().spawn(async move {
        tx.send(
            fs_clone
                .create(id, buf, opts)
                .await
                .map(|_| FilePersistResult::BlobTableCreate(blob_table_create)),
        )
        .unwrap();
    });
}

fn compact_for_cf(
    ctx: &CompactionCtx,
    iter: &mut Box<dyn Iterator>,
    safe_ts: u64,
    opts: dfs::Options,
    cf: usize,
    target_lvl: u32,
    sst_config: &TableBuilderOptions,
    bt_config: &Option<BlobTableBuildOptions>,
    keep_latest_obsolete_tombstone: bool,
    blob_tables: &HashMap<u64, BlobTable>,
    allocate_id: &mut dyn FnMut() -> u64,
) -> Result<(Vec<TableCreate>, Vec<BlobCreate>)> {
    let shard_id = ctx.req.shard_id;
    let start = ctx.req.inner_start();
    let end = ctx.req.inner_end();
    let fs = &ctx.dfs;
    let compression_lvl = ctx.compression_lvl;
    let (tx, rx) = tikv_util::mpsc::bounded(ctx.req.file_ids.len());
    let mut cur_sst_id = allocate_id();

    let mut sst_builder = sstable::Builder::new(
        cur_sst_id,
        sst_config.block_size,
        sst_config.compression_tps[target_lvl as usize - 1],
        compression_lvl,
        ctx.encryption_key.clone(),
    );
    let mut cur_blob_table_id = 0;
    // Owns the decompressed blob value while reading from the orginal blob
    // table, to reduce the memory re-allocation.
    let mut decompressed_blob_buf = vec![];
    let mut blob_table_builder = if let Some(config) = bt_config {
        cur_blob_table_id = allocate_id();
        Some((
            config,
            BlobTableBuilder::new(
                cur_blob_table_id,
                config.compression_type,
                compression_lvl,
                config.min_blob_size,
            ),
        ))
    } else {
        None
    };
    let mut cnt = 0;
    let mut last_key = BytesMut::new();
    // Whether to skip the entry if it has the same key as the last entry iterated.
    // This will be set to true when:
    //   * The last entry is the latest version of a key that is before the GC safe
    //     point.
    //   * Or this function is iterating the LOCK CF.
    let mut skip_same_key = false;
    iter.seek(start);
    while iter.valid() && iter.key() < end {
        let mut val = iter.value();
        let key = iter.key();
        // See if we need to skip this key.
        if key.deref() == last_key {
            if skip_same_key {
                iter.next_all_version();
                continue;
            }
        } else {
            // A new key is met.
            if !last_key.is_empty() {
                // Check if we need to rotate to a new SST or blob table.
                if sst_builder.estimated_size() > sst_config.max_table_size {
                    cnt += 1;
                    persist_sst(
                        cur_sst_id,
                        cf,
                        target_lvl,
                        &mut sst_builder,
                        tx.clone(),
                        fs.clone(),
                        opts,
                    );
                    cur_sst_id = allocate_id();
                    sst_builder.reset(cur_sst_id);
                }
                if let Some((bt_config, bt_builder)) = &mut blob_table_builder {
                    if bt_builder.total_blob_size() as usize > bt_config.max_blob_table_size {
                        cnt += 1;
                        persist_blob_table(
                            cur_blob_table_id,
                            bt_builder,
                            tx.clone(),
                            fs.clone(),
                            opts,
                        );
                        cur_blob_table_id = allocate_id();
                        bt_builder.reset(cur_blob_table_id);
                    }
                }
            }
            last_key.clear();
            last_key.extend_from_slice(key.deref());
            skip_same_key = false;
        }

        // Only consider the versions which are below the minReadTs, otherwise, we might
        // end up discarding the only valid version for a running transaction.
        if cf == LOCK_CF || val.version <= safe_ts {
            // key is the latest readable version of this key, so we simply discard all the
            // rest of the versions.

            skip_same_key = true;

            if val.is_deleted() {
                if !keep_latest_obsolete_tombstone {
                    iter.next_all_version();
                    continue;
                }
            } else {
                let user_meta = val.user_meta();
                if user_meta.len() == USER_META_SIZE {
                    let um = UserMeta::from_slice(user_meta);
                    if cf == WRITE_CF && um.commit_ts < safe_ts && val.is_value_empty() {
                        if keep_latest_obsolete_tombstone {
                            sst_builder.add(key, &table::Value::new_tombstone(val.version), None);
                        }
                        iter.next_all_version();
                        continue;
                    }
                    if cf == EXTRA_CF && um.start_ts < safe_ts {
                        iter.next_all_version();
                        continue;
                    }
                }
            }
        }
        if let Some((bt_config, bt_builder)) = &mut blob_table_builder {
            let mut need_recompress_blob = false;
            if val.is_blob_ref() {
                if blob_tables.is_empty() {
                    sst_builder.add(key, &val, None);
                } else {
                    // If it is a blob link, we need to deref it and may need to decompress it as
                    // well.
                    let blob_ref = val.get_blob_ref();
                    let blob_table = blob_tables.get(&blob_ref.fid).unwrap_or_else(|| {
                        panic!(
                            "[{}] blob table {} not found for key {:?}",
                            shard_id, blob_ref.fid, key,
                        )
                    });
                    if blob_table.compression_tp() != bt_config.compression_type
                        || blob_table.compression_lvl() != compression_lvl
                        || blob_table.min_blob_size() != bt_config.min_blob_size
                    {
                        need_recompress_blob = true;
                    }
                    // The blob value is either the original blob value or the compressed blob
                    // depending on whether need_recompress_blob is true.
                    let blob_or_compressed_blob = blob_table
                        .get_from_preloaded(
                            &blob_ref,
                            need_recompress_blob,
                            &mut decompressed_blob_buf,
                        )
                        .unwrap();
                    // The value needs to be added to the blob table, if:
                    //   1. need_recompress_blob is false, which means the val was already in the
                    //      blob table, and it is guaranteed that the blob size is larger than
                    //      min_blob_size.
                    //   2. need_recompress_blob is true, but the blob size is larger than
                    //      min_blob_size.
                    if !need_recompress_blob
                        || blob_or_compressed_blob.len() >= bt_config.min_blob_size as usize
                    {
                        let new_blob_ref = bt_builder.add_blob(
                            key,
                            blob_or_compressed_blob,
                            Some(blob_ref.original_len),
                        );
                        sst_builder.add(key, &val, Some(new_blob_ref));
                    } else {
                        val.fill_in_blob(blob_or_compressed_blob);
                        sst_builder.add(key, &val, None);
                    }
                }
            } else if val.value_len() >= bt_config.min_blob_size as usize {
                let blob_ref = bt_builder.add(key, &val);
                val.set_blob_ref();
                sst_builder.add(key, &val, Some(blob_ref));
            } else {
                sst_builder.add(key, &val, None);
            }
        } else {
            // If we are not building blob tables, we can write to the SST blindly.
            sst_builder.add(key, &val, None);
        }
        iter.next_all_version();
    }
    if !sst_builder.is_empty() {
        cnt += 1;
        persist_sst(
            cur_sst_id,
            cf,
            target_lvl,
            &mut sst_builder,
            tx.clone(),
            fs.clone(),
            opts,
        );
    }
    if let Some((_, bt_builder)) = &mut blob_table_builder {
        if !bt_builder.is_empty() {
            cnt += 1;
            persist_blob_table(cur_blob_table_id, bt_builder, tx, fs.clone(), opts);
        }
    }
    let mut errors = vec![];
    let mut sst_creates = vec![];
    let mut blob_table_creates = vec![];
    for _ in 0..cnt {
        match rx.recv().unwrap() {
            Err(err) => errors.push(err),
            Ok(FilePersistResult::SsTableCreate(tbl_create)) => {
                sst_creates.push(tbl_create);
            }
            Ok(FilePersistResult::BlobTableCreate(blob_table_create)) => {
                blob_table_creates.push(blob_table_create);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    Ok((sst_creates, blob_table_creates))
}

fn l0_compact_v3(
    ctx: &CompactionCtx,
    l0_compaction: &L0Compaction,
    allocate_id: &mut dyn FnMut() -> u64,
) -> Result<pb::Compaction> {
    let req = &ctx.req;
    let fs = &ctx.dfs;
    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let l0_files = load_table_files(&l0_compaction.l0_tables, fs.clone(), opts)?;
    let mut l0_tbls = in_mem_files_to_l0_tables(l0_files, ctx.encryption_key.clone());
    l0_tbls.sort_by(|a, b| b.version().cmp(&a.version()));
    let mut comp = pb::Compaction::new();
    comp.set_top_deletes(l0_compaction.l0_tables.clone());
    comp.set_level(0_u32);
    comp.set_bottom_deletes(
        l0_compaction
            .multi_cf_l1_tables
            .clone()
            .into_iter()
            .flatten()
            .collect(),
    );
    let mut all_sst_creates = vec![];
    let mut all_bt_creates = vec![];
    for cf in 0..NUM_CFS {
        let l1_ids = &l0_compaction.multi_cf_l1_tables[cf];
        let l1_files = load_table_files(l1_ids, fs.clone(), opts)?;
        let mut l1_tbls = in_mem_files_to_tables(l1_files, ctx.encryption_key.clone());
        l1_tbls.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
        let mut iters: Vec<Box<dyn table::Iterator>> = vec![];
        for l0_tbl in &l0_tbls {
            if let Some(tbl) = l0_tbl.get_cf(cf) {
                let iter = tbl.new_iterator(false, false);
                iters.push(iter);
            }
        }
        if !l1_tbls.is_empty() {
            let iter = ConcatIterator::new_with_tables(l1_tbls, false, false);
            iters.push(Box::new(iter));
        }
        let mut iter = table::new_merge_iterator(iters, false);
        let (sst_creates, bt_creates) = compact_for_cf(
            ctx,
            &mut iter,
            l0_compaction.safe_ts,
            opts,
            cf,
            1,
            &l0_compaction.sst_config,
            &l0_compaction.bt_config,
            //&None,
            true,
            &HashMap::new(),
            allocate_id,
        )?;
        all_sst_creates.extend(sst_creates);
        all_bt_creates.extend(bt_creates);
    }
    comp.set_table_creates(all_sst_creates.into());
    comp.set_blob_tables(all_bt_creates.into());
    Ok(comp)
}

fn l1_plus_compact_v3(
    ctx: &CompactionCtx,
    l1_plus_compaction: &L1PlusCompaction,
    allocate_id: &mut dyn FnMut() -> u64,
) -> Result<pb::Compaction> {
    let req = &ctx.req;
    let fs = &ctx.dfs;
    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let upper_files = load_table_files(&l1_plus_compaction.upper_level, fs.clone(), opts)?;
    let mut upper_tables = in_mem_files_to_tables(upper_files, ctx.encryption_key.clone());
    upper_tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
    let lower_files = load_table_files(&l1_plus_compaction.lower_level, fs.clone(), opts)?;
    let mut lower_tables = in_mem_files_to_tables(lower_files, ctx.encryption_key.clone());
    lower_tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
    let upper_iter = Box::new(ConcatIterator::new_with_tables(upper_tables, false, false));
    let lower_iter = Box::new(ConcatIterator::new_with_tables(lower_tables, false, false));
    let mut iter = table::new_merge_iterator(vec![upper_iter, lower_iter], false);

    let (sst_creates, _) = compact_for_cf(
        ctx,
        &mut iter,
        l1_plus_compaction.safe_ts,
        opts,
        l1_plus_compaction.cf as usize,
        l1_plus_compaction.level as u32 + 1,
        &l1_plus_compaction.sst_config,
        &None,
        l1_plus_compaction.keep_latest_obsolete_tombstone,
        &HashMap::new(),
        allocate_id,
    )?;
    let mut comp = pb::Compaction::new();
    comp.set_top_deletes(l1_plus_compaction.upper_level.clone());
    comp.set_cf(l1_plus_compaction.cf as i32);
    comp.set_level(l1_plus_compaction.level as u32);
    comp.set_table_creates(sst_creates.into());
    comp.set_bottom_deletes(l1_plus_compaction.lower_level.clone());
    Ok(comp)
}

fn major_compact_v3(
    ctx: &CompactionCtx,
    major_compaction: &MajorCompaction,
    allocate_id: &mut dyn FnMut() -> u64,
) -> Result<pb::MajorCompaction> {
    let req = &ctx.req;
    let fs = &ctx.dfs;
    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let mut ret = pb::MajorCompaction::new();
    ret.mut_old_blob_tables()
        .extend_from_slice(&major_compaction.blob_tables);
    let blob_tables = load_blob_tables(fs.clone(), &major_compaction.blob_tables, opts)?;
    let l0_files = load_table_files(&major_compaction.l0_tables, fs.clone(), opts)?;
    let mut l0_tbls = in_mem_files_to_l0_tables(l0_files, ctx.encryption_key.clone());
    l0_tbls.sort_by(|a, b| b.version().cmp(&a.version()));
    l0_tbls.iter().for_each(|tbl| {
        let mut tbl_delete = pb::TableDelete::new();
        tbl_delete.set_id(tbl.id());
        ret.mut_sstable_change()
            .mut_table_deletes()
            .push(tbl_delete);
    });
    for cf in 0..NUM_CFS {
        let mut iters: Vec<Box<dyn table::Iterator>> = vec![];
        for l0_tbl in &l0_tbls {
            if let Some(tbl) = l0_tbl.get_cf(cf) {
                let iter = tbl.new_iterator(false, false);
                iters.push(iter);
            }
        }
        if let Some(sstables) = major_compaction.ln_tables.get(&cf) {
            for (level, table_ids) in sstables.iter() {
                table_ids.iter().for_each(|t| {
                    let mut tbl_delete = pb::TableDelete::new();
                    tbl_delete.set_cf(cf as i32);
                    tbl_delete.set_level(*level as u32);
                    tbl_delete.set_id(*t);
                    ret.mut_sstable_change()
                        .mut_table_deletes()
                        .push(tbl_delete);
                });
                let files = load_table_files(table_ids, fs.clone(), opts)?;
                let mut tables = in_mem_files_to_tables(files, ctx.encryption_key.clone());
                tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
                let level_concat_iter =
                    Box::new(ConcatIterator::new_with_tables(tables, false, false));
                iters.push(level_concat_iter);
            }
        }
        if iters.is_empty() {
            continue;
        }
        let mut iter = table::new_merge_iterator(iters, false);
        let (sst_creates, blob_table_creates) = compact_for_cf(
            ctx,
            &mut iter,
            major_compaction.safe_ts,
            opts,
            cf,
            CF_LEVELS[cf] as u32,
            &major_compaction.sst_config,
            &major_compaction.bt_config,
            false,
            &blob_tables,
            allocate_id,
        )?;
        for sst_create in sst_creates {
            ret.mut_sstable_change()
                .mut_table_creates()
                .push(sst_create);
        }
        for blob_table_create in blob_table_creates {
            ret.mut_new_blob_tables().push(blob_table_create);
        }
    }
    Ok(ret)
}

pub(crate) enum CompactMsg {
    /// The shard is ready to be compacted / destroyed range / truncated ts.
    Compact(IdVer),

    /// Compaction finished.
    Finish {
        task_id: u64,
        id_ver: IdVer,
        // This variant is too large(256 bytes). Box it to reduce size.
        result: Box<Option<Result<pb::ChangeSet>>>,
    },

    /// The compaction change set is applied to the engine, so the shard is
    /// ready for next compaction.
    Applied(IdVer),

    /// Clear message is sent when a shard changed its version or set to
    /// inactive. Then all the previous tasks will be discarded.
    /// This simplifies the logic, avoid race condition.
    Clear(IdVer),

    /// Stop background compact thread.
    Stop,
}

pub(crate) struct CompactRunner {
    engine: Engine,
    rx: mpsc::Receiver<CompactMsg>,
    /// task_id is the identity of each compaction job.
    ///
    /// When a shard is set to inactive, all the compaction jobs of this shard
    /// should be discarded, then when it is set to active again, old result
    /// may arrive and conflict with the new tasks. So we use task_id to
    /// detect and discard obsolete tasks.
    task_id: u64,
    /// `running` contains shards' running compaction `task_id`s.
    running: HashMap<IdVer, u64 /* task_id */>,
    /// `notified` contains shards that have finished a compaction job and been
    /// notified to apply it.
    notified: HashSet<IdVer>,
    /// `pending` contains shards that want to do compaction but the job queue
    /// is full now.
    pending: HashSet<IdVer>,
}

impl CompactRunner {
    pub(crate) fn new(engine: Engine, rx: mpsc::Receiver<CompactMsg>) -> Self {
        Self {
            engine,
            rx,
            task_id: 0,
            running: Default::default(),
            notified: Default::default(),
            pending: Default::default(),
        }
    }

    fn run(&mut self) {
        while let Ok(msg) = self.rx.recv() {
            match msg {
                CompactMsg::Compact(id_ver) => self.compact(id_ver),

                CompactMsg::Finish {
                    task_id,
                    id_ver,
                    result,
                } => self.compaction_finished(id_ver, task_id, *result),

                CompactMsg::Applied(id_ver) => self.compaction_applied(id_ver),

                CompactMsg::Clear(id_ver) => self.clear(id_ver),

                CompactMsg::Stop => {
                    info!(
                        "Engine {} compaction worker receive stop msg and stop now",
                        self.engine.get_engine_id()
                    );
                    break;
                }
            }
        }
    }

    fn compact(&mut self, id_ver: IdVer) {
        // The shard is compacting, and following compaction will be trigger after
        // apply.
        if self.is_compacting(id_ver) {
            return;
        }
        if self.is_full() {
            self.pending.insert(id_ver);
            return;
        }
        self.task_id += 1;
        let task_id = self.task_id;
        self.running.insert(id_ver, task_id);
        let engine = self.engine.clone();
        std::thread::spawn(move || {
            tikv_util::set_current_region(id_ver.id);
            let result = Box::new(engine.compact(id_ver));
            engine.send_compact_msg(CompactMsg::Finish {
                task_id,
                id_ver,
                result,
            });
        });
    }

    fn compaction_finished(
        &mut self,
        id_ver: IdVer,
        task_id: u64,
        result: Option<Result<pb::ChangeSet>>,
    ) {
        let tag = ShardTag::new(self.engine.get_engine_id(), id_ver);
        if self
            .running
            .get(&id_ver)
            .map(|id| *id != task_id)
            .unwrap_or(true)
        {
            info!(
                "shard {} discard old term compaction result {:?}",
                tag, result
            );
            return;
        }
        self.running.remove(&id_ver);
        match result {
            Some(Ok(resp)) => {
                self.engine.handle_compact_response(resp);
                self.notified.insert(id_ver);
                self.schedule_pending_compaction();
            }
            Some(Err(FallbackLocalCompactorDisabled)) => {
                error!("shard {} local compaction disabled, no need retry", tag);
            }
            Some(Err(e)) => {
                error!("shard {} compaction failed {}, retrying", tag, e);
                self.compact(id_ver);
            }
            None => {
                info!("shard {} got empty compaction result", tag);
                // The compaction result can be none under complex situations. To avoid
                // compaction not making progress, we trigger it actively.
                if let Ok(shard) = self.engine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    self.engine.refresh_shard_states(&shard);
                }
                self.schedule_pending_compaction();
            }
        }
    }

    fn compaction_applied(&mut self, id_ver: IdVer) {
        // It's possible the shard is not being pending applied, because it may apply a
        // compaction from the former leader, so we use both `running` and
        // `notified` to detect whether it's compacting. It can avoid spawning
        // more compaction jobs, e.g., a new leader spawns one but not yet
        // finished, and it applies the compaction from the former leader, then the
        // new leader spawns another one. However, it's possible the new compaction is
        // finished and not yet applied, it can result in duplicated compaction
        // but no exceeding compaction jobs.
        self.notified.remove(&id_ver);
    }

    fn clear(&mut self, id_ver: IdVer) {
        self.running.remove(&id_ver);
        self.notified.remove(&id_ver);
        self.pending.remove(&id_ver);
        self.schedule_pending_compaction()
    }

    fn schedule_pending_compaction(&mut self) {
        while let Some(id_ver) = self.pick_highest_pri_pending_shard() {
            if self.is_full() {
                break;
            }
            self.pending.remove(&id_ver);
            self.compact(id_ver);
        }
    }

    fn pick_highest_pri_pending_shard(&mut self) -> Option<IdVer> {
        let engine = self.engine.clone();
        self.pending
            .retain(|id_ver| engine.get_shard_with_ver(id_ver.id, id_ver.ver).is_ok());

        // Find the shard with highest compaction priority.
        self.pending
            .iter()
            .max_by_key(|&&id_ver| {
                self.engine
                    .get_shard_with_ver(id_ver.id, id_ver.ver)
                    .ok()
                    .and_then(|shard| shard.get_compaction_priority())
            })
            .copied()
    }

    fn is_full(&self) -> bool {
        self.running.len() >= self.engine.opts.num_compactors
    }

    fn is_compacting(&self, id_ver: IdVer) -> bool {
        self.running.contains_key(&id_ver) || self.notified.contains(&id_ver)
    }
}
