// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering as CmpOrdering,
    collections::{HashMap, HashSet},
    iter::Iterator as StdIterator,
    ops::Sub,
    sync::{atomic::Ordering, Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::{Buf, Bytes, BytesMut};
use http::StatusCode;
use kvenginepb as pb;
use protobuf::Message;
use slog_global::error;
use tikv_util::mpsc;

use crate::{
    dfs,
    table::{
        search,
        sstable::{self, InMemFile, L0Builder, SSTable},
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
    dfs: Arc<dyn dfs::DFS>,
    remote_compactors: Arc<Mutex<RemoteCompactors>>,
    client: Option<hyper::Client<hyper::client::HttpConnector>>,
    compression_lvl: i32,
    allow_fallback_local: bool,
}

impl CompactionClient {
    pub(crate) fn new(
        dfs: Arc<dyn dfs::DFS>,
        remote_url: String,
        compression_lvl: i32,
        allow_fallback_local: bool,
    ) -> Self {
        let remote_compactors = RemoteCompactors::new(remote_url);
        Self {
            dfs,
            remote_compactors: Arc::new(Mutex::new(remote_compactors)),
            client: Some(hyper::Client::new()),
            compression_lvl,
            allow_fallback_local,
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
        let mut remote_compactor = self.get_remote_compactor();
        if remote_compactor.remote_url.is_empty() {
            local_compact(self.dfs.clone(), &req, self.compression_lvl)
        } else {
            let (tx, rx) = tikv_util::mpsc::bounded(1);
            let req = Arc::new(req);
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
                            break local_compact(self.dfs.clone(), &req, self.compression_lvl);
                        } else {
                            warn!(
                                "remote compactor is incompatible and local compaction is not allowed"
                            );
                            break Err(FallbackLocalCompactorDisabled);
                        }
                    }
                    Err(e) => {
                        retry_cnt += 1;
                        let tag = ShardTag::from_comp_req(&req);
                        error!(
                            "shard {} cf: {} level: {} remote compaction failed {:?}, retrying {} remote compactor {}",
                            tag, req.cf, req.level, e, retry_cnt, remote_compactor.remote_url
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
                                break local_compact(self.dfs.clone(), &req, self.compression_lvl);
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

/// `CURRENT_COMPACTOR_VERSION` is used for version compatibility checking of remote compactor.
/// NOTE: Increase `CURRENT_COMPACTOR_VERSION` by 1 when add new feature to remote compactor.
const CURRENT_COMPACTOR_VERSION: u32 = 1;

const INCOMPATIBLE_COMPACTOR_ERROR_CODE: StatusCode = StatusCode::NOT_IMPLEMENTED;

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CompactionRequest {
    pub cf: isize,
    pub level: usize,
    pub tops: Vec<u64>,

    pub engine_id: u64,
    pub shard_id: u64,
    pub shard_ver: u64,
    pub start: Vec<u8>,
    pub end: Vec<u8>,

    /// If `destroy_range` is true, `in_place_compact_files` will be compacted in place
    /// to filter out data that covered by `del_prefixes`.
    pub destroy_range: bool,
    pub del_prefixes: Vec<u8>,
    // Vec<(id, level, cf)>
    pub in_place_compact_files: Vec<(u64, u32, i32)>,

    /// If `truncate_ts` is some, `in_place_compact_files` will be compacted in place
    /// to filter out data with version > `truncated_ts`.
    pub truncate_ts: Option<u64>,

    /// Requires `compactor_version >= 1`.
    /// If `trim_over_bound` is true, `in_place_compact_files` will be compacted in place
    /// to filter out data out of shard bound.
    pub trim_over_bound: bool,

    // Used for L1+ compaction.
    pub bottoms: Vec<u64>,

    // Used for L0 compaction.
    pub multi_cf_bottoms: Vec<Vec<u64>>,

    pub overlap: bool,
    pub safe_ts: u64,
    pub block_size: usize,
    pub max_table_size: usize,
    pub compression_tp: u8,
    pub file_ids: Vec<u64>,

    /// Required version of remote compactor.
    /// Must be set to `CURRENT_COMPACTOR_VERSION`.
    pub compactor_version: u32,
}

pub struct CompactDef {
    pub(crate) cf: usize,
    pub(crate) level: usize,

    pub(crate) top: Vec<sstable::SSTable>,
    pub(crate) bot: Vec<sstable::SSTable>,

    pub(crate) has_overlap: bool,

    this_range: KeyRange,
    next_range: KeyRange,

    top_size: u64,
    top_left_idx: usize,
    top_right_idx: usize,
    bot_size: u64,
    bot_left_idx: usize,
    bot_right_idx: usize,
}

const MAX_COMPACTION_EXPAND_SIZE: u64 = 256 * 1024 * 1024;

impl CompactDef {
    pub(crate) fn new(cf: usize, level: usize) -> Self {
        Self {
            cf,
            level,
            top: vec![],
            bot: vec![],
            has_overlap: false,
            this_range: KeyRange::default(),
            next_range: KeyRange::default(),
            top_size: 0,
            top_left_idx: 0,
            top_right_idx: 0,
            bot_size: 0,
            bot_left_idx: 0,
            bot_right_idx: 0,
        }
    }

    pub(crate) fn fill_table(
        &mut self,
        shard: &Shard,
        this_level: &LevelHandler,
        next_level: &LevelHandler,
    ) -> bool {
        if this_level.tables.len() == 0 {
            return false;
        }
        let this = if this_level.has_over_bound_data(&shard.start, &shard.end) {
            if this_level.tables.first().unwrap().smallest() < &shard.start {
                Arc::new(vec![this_level.tables.first().unwrap().clone()])
            } else {
                Arc::new(vec![this_level.tables.last().unwrap().clone()])
            }
        } else {
            this_level.tables.clone()
        };
        let next = next_level.tables.clone();

        // First pick one table has max topSize/bottomSize ratio.
        let mut candidate_ratio = 0f64;
        for (i, tbl) in this.iter().enumerate() {
            let (left, right) = get_tables_in_range(&next, tbl.smallest(), tbl.biggest());
            let bot_size: u64 = Self::sume_tbl_size(&next[left..right]);
            let ratio = Self::calc_ratio(tbl.size(), bot_size);
            if candidate_ratio < ratio {
                candidate_ratio = ratio;
                self.top_left_idx = i;
                self.top_right_idx = i + 1;
                self.top_size = tbl.size();
                self.bot_left_idx = left;
                self.bot_right_idx = right;
                self.bot_size = bot_size;
            }
        }
        if self.top_left_idx == self.top_right_idx {
            return false;
        }
        // Expand to left to include more tops as long as the ratio doesn't decrease and the total size
        // do not exceed maxCompactionExpandSize.
        for i in (0..self.top_left_idx).rev() {
            let t = &this[i];
            let (left, right) = get_tables_in_range(&next, t.smallest(), t.biggest());
            if right < self.bot_left_idx {
                // A bottom table is skipped, we can compact in another run.
                break;
            }
            let new_top_size = t.size() + self.top_size;
            let new_bot_size = Self::sume_tbl_size(&next[left..self.bot_left_idx]) + self.bot_size;
            let new_ratio = Self::calc_ratio(new_top_size, new_bot_size);
            if new_ratio > candidate_ratio {
                self.top_left_idx -= 1;
                self.bot_left_idx = left;
                self.top_size = new_top_size;
                self.bot_size = new_bot_size;
            } else {
                break;
            }
        }
        // Expand to right to include more tops as long as the ratio doesn't decrease and the total size
        // do not exceeds maxCompactionExpandSize.
        for i in self.top_right_idx..this.len() {
            let t = &this[i];
            let (left, right) = get_tables_in_range(&next, t.smallest(), t.biggest());
            if left > self.bot_right_idx {
                // A bottom table is skipped, we can compact in another run.
                break;
            }
            let new_top_size = t.size() + self.top_size;
            let new_bot_size =
                Self::sume_tbl_size(&next[self.bot_right_idx..right]) + self.bot_size;
            let new_ratio = Self::calc_ratio(new_top_size, new_bot_size);
            if new_ratio > candidate_ratio
                && (new_top_size + new_bot_size) < MAX_COMPACTION_EXPAND_SIZE
            {
                self.top_right_idx += 1;
                self.bot_right_idx = right;
                self.top_size = new_top_size;
                self.bot_size = new_bot_size;
            } else {
                break;
            }
        }
        self.top = this[self.top_left_idx..self.top_right_idx].to_vec();
        self.bot = next[self.bot_left_idx..self.bot_right_idx].to_vec();
        self.this_range = KeyRange {
            left: Bytes::copy_from_slice(self.top[0].smallest()),
            right: Bytes::copy_from_slice(self.top.last().unwrap().biggest()),
        };
        if !self.bot.is_empty() {
            self.next_range = KeyRange {
                left: Bytes::copy_from_slice(self.bot[0].smallest()),
                right: Bytes::copy_from_slice(self.bot.last().unwrap().biggest()),
            };
        } else {
            self.next_range = self.this_range.clone();
        }
        true
    }

    fn sume_tbl_size(tbls: &[sstable::SSTable]) -> u64 {
        tbls.iter().map(|tbl| tbl.size()).sum()
    }

    fn calc_ratio(top_size: u64, bot_size: u64) -> f64 {
        if bot_size == 0 {
            return top_size as f64;
        }
        top_size as f64 / bot_size as f64
    }

    pub(crate) fn move_down(&self) -> bool {
        self.level > 0 && self.bot.is_empty()
    }
}

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

    pub(crate) fn run_compaction(&self, compact_rx: mpsc::Receiver<CompactMsg>) {
        let mut runner = CompactRunner::new(self.clone(), compact_rx);
        runner.run();
    }

    fn build_compact_request(
        &self,
        shard: &Shard,
        pri: CompactionPriority,
    ) -> Option<(CompactionRequest, Option<CompactDef>)> {
        if pri.cf == -1 {
            return self.build_compact_l0_request(shard).map(|req| (req, None));
        }
        let data = shard.get_data();
        let scf = data.get_cf(pri.cf as usize);
        let this_level = &scf.levels[pri.level - 1];
        let next_level = &scf.levels[pri.level];
        let mut cd = CompactDef::new(pri.cf as usize, pri.level);
        let filled = cd.fill_table(shard, this_level, next_level);
        if !filled {
            return None;
        }
        scf.set_has_overlapping(&mut cd);
        Some((self.build_compact_ln_request(shard, &cd), Some(cd)))
    }

    pub(crate) fn compact(&self, id_ver: IDVer) -> Option<Result<pb::ChangeSet>> {
        let shard = self.get_shard_with_ver(id_ver.id, id_ver.ver).ok()?;
        let tag = shard.tag();
        if !shard.ready_to_compact() {
            info!("avoid shard {} compaction", tag);
            return None;
        }
        // Destroy range & truncate ts has higher priority than compaction.
        if shard.get_data().ready_to_destroy_range() {
            return Some(self.destroy_range(&shard));
        }
        if shard.get_data().ready_to_truncate_ts() {
            return self.truncate_ts(&shard).transpose();
        }
        if shard.get_data().ready_to_trim_over_bound() {
            return Some(self.trim_over_bound(&shard));
        }
        let pri = shard.get_compaction_priority()?;
        let (req, cd) = self.build_compact_request(&shard, pri)?;
        store_bool(&shard.compacting, true);
        if req.level == 0 {
            info!("start compact L0 for {}", tag);
        } else {
            let cd = cd.unwrap();
            info!(
                "start compact L{} CF{} for {}, num_ids: {}, input_size: {}",
                req.level,
                req.cf,
                tag,
                req.file_ids.len(),
                cd.top_size + cd.bot_size,
            );
            if req.bottoms.is_empty() && req.cf as usize == WRITE_CF {
                info!("move down L{} CF{} for {}", req.level, req.cf, tag);
                // Move down. only write CF benefits from this optimization.
                let mut comp = pb::Compaction::new();
                comp.set_cf(req.cf as i32);
                comp.set_level(req.level as u32);
                comp.set_top_deletes(req.tops.clone());
                let tbl_creates = cd
                    .top
                    .into_iter()
                    .map(|top_tbl| {
                        let mut tbl_create = pb::TableCreate::new();
                        tbl_create.set_id(top_tbl.id());
                        tbl_create.set_cf(req.cf as i32);
                        tbl_create.set_level(req.level as u32 + 1);
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
        }
        Some(self.comp_client.compact(req))
    }

    pub(crate) fn build_compact_l0_request(&self, shard: &Shard) -> Option<CompactionRequest> {
        let tag = shard.tag();
        let data = shard.get_data();
        if data.l0_tbls.is_empty() {
            info!("{} zero L0 tables", tag);
            return None;
        }
        let mut req = self.new_compact_request(shard, -1, 0);
        let mut total_size = 0;
        let mut smallest = data.l0_tbls[0].smallest();
        let mut biggest = data.l0_tbls[0].biggest();
        for l0 in &data.l0_tbls {
            if smallest > l0.smallest() {
                smallest = l0.smallest();
            }
            if biggest < l0.biggest() {
                biggest = l0.biggest();
            }
            req.tops.push(l0.id());
            total_size += l0.size();
        }
        for cf in 0..NUM_CFS {
            let lh = data.get_cf(cf).get_level(1);
            let mut bottoms = vec![];
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
                bottoms.push(tbl.id());
                total_size += tbl.size();
            }
            req.multi_cf_bottoms.push(bottoms);
        }
        self.set_alloc_ids_for_request(&mut req, total_size);
        Some(req)
    }

    pub(crate) fn set_alloc_ids_for_request(&self, req: &mut CompactionRequest, total_size: u64) {
        let tag = ShardTag::from_comp_req(req);
        // We must ensure there are enough ids for remote compactor to use, so we need to allocate
        // more ids than needed.
        let mut old_ids_num = 0usize;
        for bot_ids in req.multi_cf_bottoms.iter() {
            old_ids_num += bot_ids.len();
        }
        old_ids_num += req.tops.len() + req.bottoms.len();
        let id_cnt = (total_size as usize / req.max_table_size) * 2 + 16 + old_ids_num;
        info!(
            "{} alloc id count {} for total size {}",
            tag, id_cnt, total_size
        );
        let ids = self.id_allocator.alloc_id(id_cnt);
        req.file_ids = ids;
    }

    pub(crate) fn new_compact_request(
        &self,
        shard: &Shard,
        cf: isize,
        level: usize,
    ) -> CompactionRequest {
        CompactionRequest {
            engine_id: shard.engine_id,
            shard_id: shard.id,
            shard_ver: shard.ver,
            start: shard.start.to_vec(),
            end: shard.end.to_vec(),
            destroy_range: false,
            del_prefixes: vec![],
            in_place_compact_files: vec![],
            truncate_ts: None,
            trim_over_bound: false,
            cf,
            level,
            safe_ts: load_u64(&self.managed_safe_ts),
            block_size: self.opts.table_builder_options.block_size,
            max_table_size: self.opts.table_builder_options.max_table_size,
            compression_tp: self.opts.table_builder_options.compression_tps[level],
            overlap: level == 0,
            tops: vec![],
            bottoms: vec![],
            multi_cf_bottoms: vec![],
            file_ids: vec![],
            compactor_version: CURRENT_COMPACTOR_VERSION,
        }
    }

    pub(crate) fn build_compact_ln_request(
        &self,
        shard: &Shard,
        cd: &CompactDef,
    ) -> CompactionRequest {
        let mut req = self.new_compact_request(shard, cd.cf as isize, cd.level);
        req.overlap = cd.has_overlap;
        for top in &cd.top {
            req.tops.push(top.id());
        }
        for bot in &cd.bot {
            req.bottoms.push(bot.id());
        }
        self.set_alloc_ids_for_request(&mut req, cd.top_size + cd.bot_size);
        req
    }

    fn destroy_range(&self, shard: &Shard) -> Result<pb::ChangeSet> {
        let data = shard.get_data();
        assert!(!data.del_prefixes.is_empty());
        // Tables that full covered by delete-prefixes.
        let mut deletes = vec![];
        // Tables that partially covered by delete-prefixes.
        let mut overlaps = vec![];
        for t in &data.l0_tbls {
            if data.del_prefixes.cover_range(t.smallest(), t.biggest()) {
                let mut delete = pb::TableDelete::default();
                delete.set_id(t.id());
                delete.set_level(0);
                delete.set_cf(-1);
                deletes.push(delete);
            } else if data
                .del_prefixes
                .delete_ranges()
                .any(|(start, end)| t.has_data_in_range(start, end))
            {
                overlaps.push((t.id(), 0, -1));
            }
        }
        data.for_each_level(|cf, lh| {
            for t in lh.tables.iter() {
                if data.del_prefixes.cover_range(t.smallest(), t.biggest()) {
                    let mut delete = pb::TableDelete::default();
                    delete.set_id(t.id());
                    delete.set_level(lh.level as u32);
                    delete.set_cf(cf as i32);
                    deletes.push(delete);
                } else if data
                    .del_prefixes
                    .delete_ranges()
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
            data.del_prefixes,
            deletes.len(),
            overlaps.len()
        );

        let mut cs = if overlaps.is_empty() {
            let mut cs = pb::ChangeSet::default();
            cs.mut_destroy_range().set_table_deletes(deletes.into());
            cs
        } else {
            let mut req = self.new_compact_request(shard, 0, 0);
            req.destroy_range = true;
            req.in_place_compact_files = overlaps;
            req.del_prefixes = data.del_prefixes.marshal();
            req.file_ids = self.id_allocator.alloc_id(req.in_place_compact_files.len());
            let mut cs = self.comp_client.compact(req)?;
            let dr = cs.mut_destroy_range();
            deletes.extend(dr.take_table_deletes().into_iter());
            dr.set_table_deletes(deletes.into());
            cs
        };
        cs.set_shard_id(shard.id);
        cs.set_shard_ver(shard.ver);
        cs.set_property_key(DEL_PREFIXES_KEY.to_string());
        cs.set_property_value(data.del_prefixes.marshal());
        Ok(cs)
    }

    pub(crate) fn truncate_ts(&self, shard: &Shard) -> Result<Option<pb::ChangeSet>> {
        let data = shard.get_data();
        let truncate_ts = data.truncate_ts.unwrap();

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
            let mut req = self.new_compact_request(shard, 0, 0);
            req.truncate_ts = Some(truncate_ts.inner());
            req.in_place_compact_files = overlaps;
            req.file_ids = self.id_allocator.alloc_id(req.in_place_compact_files.len());
            self.comp_client.compact(req)?
        };
        cs.set_shard_id(shard.id);
        cs.set_shard_ver(shard.ver);
        cs.set_property_key(TRUNCATE_TS_KEY.to_string());
        cs.set_property_value(truncate_ts.marshal().to_vec());
        Ok(Some(cs))
    }

    fn trim_over_bound(&self, shard: &Shard) -> Result<pb::ChangeSet> {
        let data = shard.get_data();
        assert!(data.trim_over_bound);
        // Tables that are entirely over bound.
        let mut deletes = vec![];
        // Tables that are partially over bound.
        let mut overlaps = vec![];
        for t in &data.l0_tbls {
            if t.biggest() < data.start || t.smallest() >= data.end {
                // -----smallest-----biggest-----[start----------end)
                // [start----------end)-----smallest-----biggest-----
                let mut delete = pb::TableDelete::default();
                delete.set_id(t.id());
                delete.set_level(0);
                delete.set_cf(-1);
                deletes.push(delete);
            } else if t.smallest() < data.start || t.biggest() >= data.end {
                // -----smallest-----[start----------end)-----biggest-----
                overlaps.push((t.id(), 0, -1));
            }
        }
        data.for_each_level(|cf, lh| {
            for t in lh.tables.iter() {
                if t.biggest() < data.start || t.smallest() >= data.end {
                    // -----smallest-----biggest-----[start----------end)
                    // [start----------end)-----smallest-----biggest-----
                    let mut delete = pb::TableDelete::default();
                    delete.set_id(t.id());
                    delete.set_level(lh.level as u32);
                    delete.set_cf(cf as i32);
                    deletes.push(delete);
                } else if t.smallest() < data.start || t.biggest() >= data.end {
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
            let mut req = self.new_compact_request(shard, 0, 0);
            req.trim_over_bound = true;
            req.in_place_compact_files = overlaps;
            req.file_ids = self.id_allocator.alloc_id(req.in_place_compact_files.len());
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
        Ok(cs)
    }

    pub(crate) fn handle_compact_response(&self, cs: pb::ChangeSet) {
        self.meta_change_listener.on_change_set(cs);
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct CompactionPriority {
    pub cf: isize,
    pub level: usize,
    pub score: f64,
    pub shard_id: u64,
    pub shard_ver: u64,
}

impl Ord for CompactionPriority {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(CmpOrdering::Equal)
    }
}

impl PartialOrd for CompactionPriority {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        self.score.partial_cmp(&other.score)
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

pub(crate) fn get_key_range(tables: &[SSTable]) -> KeyRange {
    let mut smallest = tables[0].smallest();
    let mut biggest = tables[0].biggest();
    for i in 1..tables.len() {
        let tbl = &tables[i];
        if tbl.smallest() < smallest {
            smallest = tbl.smallest();
        }
        if tbl.biggest() > tbl.biggest() {
            biggest = tbl.biggest();
        }
    }
    KeyRange {
        left: Bytes::copy_from_slice(smallest),
        right: Bytes::copy_from_slice(biggest),
    }
}

pub(crate) fn get_tables_in_range(tables: &[SSTable], start: &[u8], end: &[u8]) -> (usize, usize) {
    let left = search(tables.len(), |i| start <= tables[i].biggest());
    let right = search(tables.len(), |i| end < tables[i].smallest());
    (left, right)
}

pub(crate) fn compact_l0(
    req: &CompactionRequest,
    fs: Arc<dyn dfs::DFS>,
    compression_lvl: i32,
) -> Result<Vec<pb::TableCreate>> {
    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let l0_files = load_table_files(&req.tops, fs.clone(), opts)?;
    let mut l0_tbls = in_mem_files_to_l0_tables(l0_files);
    l0_tbls.sort_by(|a, b| b.version().cmp(&a.version()));
    let mut mult_cf_bot_tbls = vec![];
    for cf in 0..NUM_CFS {
        let bot_ids = &req.multi_cf_bottoms[cf];
        let bot_files = load_table_files(bot_ids, fs.clone(), opts)?;
        let mut bot_tbls = in_mem_files_to_tables(bot_files);
        bot_tbls.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        mult_cf_bot_tbls.push(bot_tbls);
    }

    let channel_cap = req.file_ids.len();
    let (tx, rx) = tikv_util::mpsc::bounded(channel_cap as usize);
    let mut id_idx = 0;
    for cf in 0..NUM_CFS {
        let mut iter = build_compact_l0_iterator(
            cf,
            l0_tbls.clone(),
            std::mem::take(&mut mult_cf_bot_tbls[cf]),
            &req.start,
        );
        let mut helper = CompactL0Helper::new(cf, req, compression_lvl);
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

pub(crate) fn load_table_files(
    tbl_ids: &[u64],
    fs: Arc<dyn dfs::DFS>,
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
                .map_err(|e| Error::DFSError(e));
            atx.send(res).unwrap();
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

fn in_mem_files_to_l0_tables(files: Vec<InMemFile>) -> Vec<sstable::L0Table> {
    files
        .into_iter()
        .map(|f| sstable::L0Table::new(Arc::new(f), None, false).unwrap())
        .collect()
}

fn in_mem_files_to_tables(files: Vec<InMemFile>) -> Vec<sstable::SSTable> {
    files
        .into_iter()
        .map(|f| sstable::SSTable::new(Arc::new(f), None, false).unwrap())
        .collect()
}

fn build_compact_l0_iterator(
    cf: usize,
    top_tbls: Vec<sstable::L0Table>,
    bot_tbls: Vec<sstable::SSTable>,
    start: &[u8],
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
    fn new(cf: usize, req: &CompactionRequest, compression_lvl: i32) -> Self {
        Self {
            cf,
            builder: sstable::Builder::new(0, req.block_size, req.compression_tp, compression_lvl),
            last_key: BytesMut::new(),
            skip_key: BytesMut::new(),
            safe_ts: req.safe_ts,
            max_table_size: req.max_table_size,
            end: req.end.clone(),
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
                if key == self.skip_key {
                    iter.next_all_version();
                    continue;
                } else {
                    self.skip_key.clear();
                }
            }
            if key != self.last_key {
                // We only break on table size.
                if self.builder.estimated_size() > self.max_table_size {
                    break;
                }
                if key >= self.end.as_slice() {
                    break;
                }
                self.last_key.clear();
                self.last_key.extend_from_slice(key);
            }

            // Only consider the versions which are below the safeTS, otherwise, we might end up discarding the
            // only valid version for a running transaction.
            let val = iter.value();
            if val.version <= self.safe_ts {
                // key is the latest readable version of this key, so we simply discard all the rest of the versions.
                self.skip_key.clear();
                self.skip_key.extend_from_slice(key);
                if !val.is_deleted() {
                    match filter(self.safe_ts, self.cf, val) {
                        Decision::Keep => {}
                        Decision::Drop => {
                            iter.next_all_version();
                            continue;
                        }
                        Decision::MarkTombStone => {
                            // There may have old versions for this key, so convert to delete tombstone.
                            self.builder
                                .add(key, table::Value::new_tombstone(val.version));
                            iter.next_all_version();
                            continue;
                        }
                    }
                }
            }
            self.builder.add(key, val);
            iter.next_all_version();
        }
        if self.builder.is_empty() {
            return Ok((pb::TableCreate::new(), Bytes::new()));
        }
        let mut buf = BytesMut::with_capacity(self.builder.estimated_size());
        let res = self.builder.finish(0, &mut buf);
        let mut table_create = pb::TableCreate::new();
        table_create.set_id(id);
        table_create.set_level(1);
        table_create.set_cf(self.cf as i32);
        table_create.set_smallest(res.smallest);
        table_create.set_biggest(res.biggest);
        Ok((table_create, buf.freeze()))
    }
}

pub(crate) fn compact_tables(
    req: &CompactionRequest,
    fs: Arc<dyn dfs::DFS>,
    compression_lvl: i32,
) -> Result<Vec<pb::TableCreate>> {
    let tag = ShardTag::from_comp_req(req);
    info!(
        "{} compact req tops {:?}, bots {:?}",
        tag, &req.tops, &req.bottoms
    );
    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let top_files = load_table_files(&req.tops, fs.clone(), opts)?;
    let mut top_tables = in_mem_files_to_tables(top_files);
    top_tables.sort_by(|a, b| a.smallest().cmp(b.smallest()));
    let bot_files = load_table_files(&req.bottoms, fs.clone(), opts)?;
    let mut bot_tables = in_mem_files_to_tables(bot_files);
    bot_tables.sort_by(|a, b| a.smallest().cmp(b.smallest()));
    let top_iter = Box::new(ConcatIterator::new_with_tables(top_tables, false, false));
    let bot_iter = Box::new(ConcatIterator::new_with_tables(bot_tables, false, false));
    let mut iter = table::new_merge_iterator(vec![top_iter, bot_iter], false);
    iter.seek(&req.start);

    let mut last_key = BytesMut::new();
    let mut skip_key = BytesMut::new();
    let mut builder = sstable::Builder::new(0, req.block_size, req.compression_tp, compression_lvl);
    let mut id_idx = 0;
    let (tx, rx) = tikv_util::mpsc::bounded(req.file_ids.len());
    let mut reach_end = false;
    while iter.valid() && !reach_end {
        if id_idx >= req.file_ids.len() {
            panic!(
                "index out of bounds: the len is {} but the index is {}, req {:?}",
                req.file_ids.len(),
                id_idx,
                req
            );
        }
        let id = req.file_ids[id_idx];
        builder.reset(id);
        last_key.clear();
        while iter.valid() {
            let val = iter.value();
            let key = iter.key();
            let kv_size = val.encoded_size() + key.len();
            // See if we need to skip this key.
            if !skip_key.is_empty() {
                if key == skip_key {
                    iter.next_all_version();
                    continue;
                } else {
                    skip_key.clear();
                }
            }
            if key != last_key {
                if !last_key.is_empty() && builder.estimated_size() + kv_size > req.max_table_size {
                    break;
                }
                if key >= req.end.as_slice() {
                    reach_end = true;
                    break;
                }
                last_key.clear();
                last_key.extend_from_slice(key);
            }

            // Only consider the versions which are below the minReadTs, otherwise, we might end up discarding the
            // only valid version for a running transaction.
            if req.cf as usize == LOCK_CF || val.version <= req.safe_ts {
                // key is the latest readable version of this key, so we simply discard all the rest of the versions.
                skip_key.clear();
                skip_key.extend_from_slice(key);

                if val.is_deleted() {
                    // If this key range has overlap with lower levels, then keep the deletion
                    // marker with the latest version, discarding the rest. We have set skipKey,
                    // so the following key versions would be skipped. Otherwise discard the deletion marker.
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
                                // There may have old versions for this key, so convert to delete tombstone.
                                builder.add(key, table::Value::new_tombstone(val.version));
                            }
                            iter.next_all_version();
                            continue;
                        }
                    }
                }
            }
            builder.add(key, val);
            iter.next_all_version();
        }
        if builder.is_empty() {
            continue;
        }
        let mut buf = BytesMut::with_capacity(builder.estimated_size());
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
                afs.create(tbl_create.id, buf.freeze(), opts)
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
// Since we use txn ts as badger version, we only need to filter Delete, Rollback and Op_Lock.
// It is called for the first valid version before safe point, older versions are discarded automatically.
fn filter(safe_ts: u64, cf: usize, val: table::Value) -> Decision {
    let user_meta = val.user_meta();
    if cf == WRITE_CF {
        if user_meta.len() == USER_META_SIZE {
            let um = UserMeta::from_slice(user_meta);
            if um.commit_ts < safe_ts && val.get_value().is_empty() {
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
    // Older version are discarded automatically, we need to keep the first valid version.
    Decision::Keep
}

pub async fn handle_remote_compaction(
    dfs: Arc<dyn dfs::DFS>,
    req: hyper::Request<hyper::Body>,
    compression_lvl: i32,
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

    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let result = local_compact(dfs, &comp_req, compression_lvl);
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

fn local_compact(
    dfs: Arc<dyn dfs::DFS>,
    req: &CompactionRequest,
    compression_lvl: i32,
) -> Result<pb::ChangeSet> {
    let mut cs = pb::ChangeSet::new();
    cs.set_shard_id(req.shard_id);
    cs.set_shard_ver(req.shard_ver);
    let tag = ShardTag::from_comp_req(req);
    if req.destroy_range {
        info!("start destroying range for {}", tag);
        let tc = compact_destroy_range(req, dfs, compression_lvl)?;
        cs.set_destroy_range(tc);
        info!("finish destroying range for {}", tag);
        return Ok(cs);
    }

    if let Some(truncate_ts) = req.truncate_ts {
        info!("start truncate ts({}) for {}", truncate_ts, tag);
        let tc = compact_truncate_ts(req, dfs, compression_lvl)?;
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
        let tc = compact_trim_over_bound(req, dfs, compression_lvl)?;
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
        let tbls = compact_l0(req, dfs.clone(), compression_lvl)?;
        comp.set_table_creates(tbls.into());
        let bot_dels = req.multi_cf_bottoms.clone().into_iter().flatten().collect();
        comp.set_bottom_deletes(bot_dels);
        info!("finish compacting L0 for {}", tag);
    } else {
        info!("start compacting L{} CF{} for {}", req.level, req.cf, tag);
        let tbls = compact_tables(req, dfs.clone(), compression_lvl)?;
        comp.set_table_creates(tbls.into());
        comp.set_bottom_deletes(req.bottoms.clone());
        info!("finish compacting L{} CF{} for {}", req.level, req.cf, tag);
    }
    cs.set_compaction(comp);
    Ok(cs)
}

/// Compact files in place to remove data covered by delete prefixes.
fn compact_destroy_range(
    req: &CompactionRequest,
    dfs: Arc<dyn dfs::DFS>,
    compression_lvl: i32,
) -> Result<pb::TableChange> {
    assert!(
        req.destroy_range && !req.del_prefixes.is_empty() && !req.in_place_compact_files.is_empty()
    );
    assert_eq!(req.in_place_compact_files.len(), req.file_ids.len());

    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let mut files: HashMap<u64, InMemFile> = load_table_files(
        &req.in_place_compact_files
            .iter()
            .map(|(id, ..)| *id)
            .collect::<Vec<_>>(),
        dfs.clone(),
        opts,
    )?
    .into_iter()
    .map(|file| (file.id, file))
    .collect();

    let mut deletes = vec![];
    let mut creates = vec![];
    let del_prefixes = DeletePrefixes::unmarshal(&req.del_prefixes);
    let (tx, rx) = tikv_util::mpsc::bounded(req.file_ids.len());
    for (&(id, level, cf), &new_id) in req.in_place_compact_files.iter().zip(req.file_ids.iter()) {
        let file = files.remove(&id).unwrap();
        let (data, smallest, biggest) = if level == 0 {
            let t = sstable::L0Table::new(Arc::new(file), None, false).unwrap();
            let mut builder = L0Builder::new(new_id, req.block_size, t.version());
            for cf in 0..NUM_CFS {
                if let Some(cf_t) = t.get_cf(cf) {
                    let mut iter = cf_t.new_iterator(false, false);
                    iter.rewind();
                    while iter.valid() {
                        let key = iter.key();
                        if !del_prefixes.cover_prefix(key) {
                            builder.add(cf, key, iter.value());
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
            let t = sstable::SSTable::new(Arc::new(file), None, false).unwrap();
            let mut builder = sstable::Builder::new(
                new_id,
                req.block_size,
                t.compression_type(),
                compression_lvl,
            );
            let mut iter = t.new_iterator(false, false);
            iter.rewind();
            while iter.valid() {
                let key = iter.key();
                if !del_prefixes.cover_prefix(key) {
                    builder.add(key, iter.value());
                }
                iter.next_all_version();
            }
            if builder.is_empty() {
                continue;
            }
            let mut buf = BytesMut::with_capacity(builder.estimated_size());
            let res = builder.finish(0, &mut buf);
            (buf.freeze(), res.smallest, res.biggest)
        };
        let tx = tx.clone();
        let dfs_clone = dfs.clone();
        dfs.get_runtime().spawn(async move {
            tx.send(dfs_clone.create(new_id, data, opts).await).unwrap();
        });
        let mut delete = pb::TableDelete::new();
        delete.set_id(id);
        delete.set_level(level);
        delete.set_cf(cf);
        deletes.push(delete);
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
    req: &CompactionRequest,
    dfs: Arc<dyn dfs::DFS>,
    compression_lvl: i32,
) -> Result<pb::TableChange> {
    let truncate_ts = req.truncate_ts.unwrap();
    assert!(!req.in_place_compact_files.is_empty());

    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let mut files: HashMap<u64, InMemFile> = load_table_files(
        &req.in_place_compact_files
            .iter()
            .map(|(id, ..)| *id)
            .collect::<Vec<_>>(),
        dfs.clone(),
        opts,
    )?
    .into_iter()
    .map(|file| (file.id, file))
    .collect();

    let mut deletes = vec![];
    let mut creates = vec![];
    let (tx, rx) = tikv_util::mpsc::bounded(req.file_ids.len());
    for (&(id, level, cf), &new_id) in req.in_place_compact_files.iter().zip(req.file_ids.iter()) {
        let file = files.remove(&id).unwrap();

        let mut delete = pb::TableDelete::new();
        delete.set_id(id);
        delete.set_level(level);
        delete.set_cf(cf);
        deletes.push(delete);

        let (data, smallest, biggest) = if level == 0 {
            let t = sstable::L0Table::new(Arc::new(file), None, false).unwrap();
            let mut builder = L0Builder::new(new_id, req.block_size, t.version());
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
                            builder.add(cf, key, value);
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
            let t = sstable::SSTable::new(Arc::new(file), None, false).unwrap();
            let mut builder = sstable::Builder::new(
                new_id,
                req.block_size,
                t.compression_type(),
                compression_lvl,
            );
            let mut iter = t.new_iterator(false, false);
            iter.rewind();
            while iter.valid() {
                let key = iter.key();
                let value = iter.value();
                // Keep data in LOCK_CF. Locks would be resolved by TiDB.
                // TODO: handle async commit
                if cf as usize == LOCK_CF || value.version <= truncate_ts {
                    builder.add(key, value);
                }
                iter.next_all_version();
            }
            if builder.is_empty() {
                continue;
            }
            let mut buf = BytesMut::with_capacity(builder.estimated_size());
            let res = builder.finish(0, &mut buf);
            (buf.freeze(), res.smallest, res.biggest)
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
    req: &CompactionRequest,
    dfs: Arc<dyn dfs::DFS>,
    compression_lvl: i32,
) -> Result<pb::TableChange> {
    assert!(req.trim_over_bound && !req.in_place_compact_files.is_empty());
    assert_eq!(req.in_place_compact_files.len(), req.file_ids.len());

    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let mut files: HashMap<u64, InMemFile> = load_table_files(
        &req.in_place_compact_files
            .iter()
            .map(|(id, ..)| *id)
            .collect::<Vec<_>>(),
        dfs.clone(),
        opts,
    )?
    .into_iter()
    .map(|file| (file.id, file))
    .collect();

    let mut deletes = vec![];
    let mut creates = vec![];
    let (tx, rx) = tikv_util::mpsc::bounded(req.file_ids.len());
    for (&(id, level, cf), &new_id) in req.in_place_compact_files.iter().zip(req.file_ids.iter()) {
        let file = files.remove(&id).unwrap();

        let mut delete = pb::TableDelete::new();
        delete.set_id(id);
        delete.set_level(level);
        delete.set_cf(cf);
        deletes.push(delete);

        let (data, smallest, biggest) = if level == 0 {
            let t = sstable::L0Table::new(Arc::new(file), None, false).unwrap();
            let mut builder = L0Builder::new(new_id, req.block_size, t.version());
            for cf in 0..NUM_CFS {
                if let Some(cf_t) = t.get_cf(cf) {
                    let mut iter = cf_t.new_iterator(false, false);
                    iter.seek(&req.start);
                    while iter.valid() {
                        let key = iter.key();
                        if key >= req.end.as_slice() {
                            break;
                        }
                        builder.add(cf, key, iter.value());
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
            let t = sstable::SSTable::new(Arc::new(file), None, false).unwrap();
            let mut builder = sstable::Builder::new(
                new_id,
                req.block_size,
                t.compression_type(),
                compression_lvl,
            );
            let mut iter = t.new_iterator(false, false);
            iter.seek(&req.start);
            while iter.valid() {
                let key = iter.key();
                if key >= req.end.as_slice() {
                    break;
                }
                builder.add(key, iter.value());
                iter.next_all_version();
            }
            if builder.is_empty() {
                continue;
            }
            let mut buf = BytesMut::with_capacity(builder.estimated_size());
            let res = builder.finish(0, &mut buf);
            (buf.freeze(), res.smallest, res.biggest)
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

pub(crate) enum CompactMsg {
    /// The shard is ready to be compacted / destroyed range / truncated ts.
    Compact(IDVer),

    /// Compaction finished.
    Finish {
        task_id: u64,
        id_ver: IDVer,
        // This variant is too large(256 bytes). Box it to reduce size.
        result: Box<Option<Result<pb::ChangeSet>>>,
    },

    /// The compaction change set is applied to the engine, so the shard is ready
    /// for next compaction.
    Applied(IDVer),

    /// Clear message is sent when a shard changed its version or set to inactive.
    /// Then all the previous tasks will be discarded.
    /// This simplifies the logic, avoid race condition.
    Clear(IDVer),
}

pub(crate) struct CompactRunner {
    engine: Engine,
    rx: mpsc::Receiver<CompactMsg>,
    /// task_id is the identity of each compaction job.
    ///
    /// When a shard is set to inactive, all the compaction jobs of this shard should be discarded, then when
    /// it is set to active again, old result may arrive and conflict with the new tasks.
    /// So we use task_id to detect and discard obsolete tasks.
    task_id: u64,
    /// `running` contains shards' running compaction `task_id`s.
    running: HashMap<IDVer, u64 /* task_id */>,
    /// `notified` contains shards that have finished a compaction job and been notified to apply it.
    notified: HashSet<IDVer>,
    /// `pending` contains shards that want to do compaction but the job queue is full now.
    pending: HashSet<IDVer>,
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
            }
        }
    }

    fn compact(&mut self, id_ver: IDVer) {
        // The shard is compacting, and following compaction will be trigger after apply.
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
            let result = Box::new(engine.compact(id_ver));
            engine
                .compact_tx
                .send(CompactMsg::Finish {
                    task_id,
                    id_ver,
                    result,
                })
                .unwrap();
        });
    }

    fn compaction_finished(
        &mut self,
        id_ver: IDVer,
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
                // The compaction result can be none under complex situations. To avoid compaction
                // not making progress, we trigger it actively.
                if let Ok(shard) = self.engine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    self.engine.refresh_shard_states(&shard);
                }
                self.schedule_pending_compaction();
            }
        }
    }

    fn compaction_applied(&mut self, id_ver: IDVer) {
        // It's possible the shard is not being pending applied, because it may apply a compaction
        // from the former leader, so we use both `running` and `notified` to detect whether
        // it's compacting. It can avoid spawning more compaction jobs, e.g., a new leader spawns
        // one but not yet finished, and it applies the compaction from the former leader, then the
        // new leader spawns another one. However, it's possible the new compaction is finished and
        // not yet applied, it can result in duplicated compaction but no exceeding compaction
        // jobs.
        self.notified.remove(&id_ver);
    }

    fn clear(&mut self, id_ver: IDVer) {
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

    fn pick_highest_pri_pending_shard(&mut self) -> Option<IDVer> {
        let engine = self.engine.clone();
        self.pending
            .retain(|id_ver| engine.get_shard_with_ver(id_ver.id, id_ver.ver).is_ok());

        // Pick the shard that ready to destroy range / truncate ts / trim over bound.
        if let Some(id_ver) = self.pending.iter().find(|id_ver| {
            self.engine
                .get_shard_with_ver(id_ver.id, id_ver.ver)
                .map(|shard| {
                    shard.get_data().ready_to_destroy_range()
                        || shard.get_data().ready_to_truncate_ts()
                        || shard.get_data().ready_to_trim_over_bound()
                })
                .unwrap_or(false)
        }) {
            return Some(*id_ver);
        }

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

    fn is_compacting(&self, id_ver: IDVer) -> bool {
        self.running.contains_key(&id_ver) || self.notified.contains(&id_ver)
    }
}
