// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{mpsc, Arc, Mutex};

use collections::HashMap;
use engine_tiflash::FsStatsExt;
use kvproto::{
    import_sstpb::SstMeta,
    metapb::Region,
    raft_cmdpb::{
        AdminCmdType, AdminRequest, AdminResponse, ChangePeerRequest, CmdType, CommitMergeRequest,
        RaftCmdRequest, RaftCmdResponse, Request,
    },
    raft_serverpb::RaftApplyState,
};
use raft::{eraftpb, StateRole};
use sst_importer::SstImporter;
use tikv_util::{box_err, debug, error, info};
use yatp::{
    pool::{Builder, ThreadPool},
    task::future::TaskCell,
};

use crate::{
    coprocessor::{
        AdminObserver, ApplySnapshotObserver, BoxAdminObserver, BoxApplySnapshotObserver,
        BoxPdTaskObserver, BoxQueryObserver, BoxRegionChangeObserver, Cmd, Coprocessor,
        CoprocessorHost, ObserverContext, PdTaskObserver, QueryObserver, RegionChangeEvent,
        RegionChangeObserver, RegionState, StoreSizeInfo,
    },
    engine_store_ffi::{
        gen_engine_store_server_helper,
        interfaces::root::{DB as ffi_interfaces, DB::EngineStoreApplyRes},
        name_to_cf, ColumnFamilyType, EngineStoreServerHelper, RaftCmdHeader, RawCppPtr,
        TiFlashEngine, WriteCmdType, WriteCmds, CF_DEFAULT, CF_LOCK, CF_WRITE,
    },
    store::{check_sst_for_ingestion, SnapKey},
    Error, Result,
};

impl Into<engine_tiflash::FsStatsExt> for ffi_interfaces::StoreStats {
    fn into(self) -> FsStatsExt {
        FsStatsExt {
            available: self.fs_stats.avail_size,
            capacity: self.fs_stats.capacity_size,
            used: self.fs_stats.used_size,
        }
    }
}

pub struct TiFlashFFIHub {
    pub engine_store_server_helper: &'static EngineStoreServerHelper,
}
unsafe impl Send for TiFlashFFIHub {}
unsafe impl Sync for TiFlashFFIHub {}
impl engine_tiflash::FFIHubInner for TiFlashFFIHub {
    fn get_store_stats(&self) -> engine_tiflash::FsStatsExt {
        self.engine_store_server_helper
            .handle_compute_store_stats()
            .into()
    }
}

pub struct PtrWrapper(RawCppPtr);

unsafe impl Send for PtrWrapper {}
unsafe impl Sync for PtrWrapper {}

#[derive(Default, Debug)]
pub struct PrehandleContext {
    // tracer holds ptr of snapshot prehandled by TiFlash side.
    pub tracer: HashMap<SnapKey, Arc<PrehandleTask>>,
}

#[derive(Debug)]
pub struct PrehandleTask {
    pub recv: mpsc::Receiver<PtrWrapper>,
    pub peer_id: u64,
}

impl PrehandleTask {
    fn new(recv: mpsc::Receiver<PtrWrapper>, peer_id: u64) -> Self {
        PrehandleTask { recv, peer_id }
    }
}
unsafe impl Send for PrehandleTask {}
unsafe impl Sync for PrehandleTask {}

pub struct TiFlashObserver {
    pub peer_id: u64,
    pub engine_store_server_helper: &'static EngineStoreServerHelper,
    pub engine: TiFlashEngine,
    pub sst_importer: Arc<SstImporter>,
    pub pre_handle_snapshot_ctx: Arc<Mutex<PrehandleContext>>,
    pub snap_handle_pool_size: usize,
    pub apply_snap_pool: Option<Arc<ThreadPool<TaskCell>>>,
}

impl Clone for TiFlashObserver {
    fn clone(&self) -> Self {
        TiFlashObserver {
            peer_id: self.peer_id,
            engine_store_server_helper: self.engine_store_server_helper,
            engine: self.engine.clone(),
            sst_importer: self.sst_importer.clone(),
            pre_handle_snapshot_ctx: self.pre_handle_snapshot_ctx.clone(),
            snap_handle_pool_size: self.snap_handle_pool_size,
            apply_snap_pool: self.apply_snap_pool.clone(),
        }
    }
}

// TiFlash observer's priority should be higher than all other observers, to avoid being bypassed.
const TIFLASH_OBSERVER_PRIORITY: u32 = 0;

impl TiFlashObserver {
    pub fn new(
        peer_id: u64,
        engine: engine_tiflash::RocksEngine,
        sst_importer: Arc<SstImporter>,
        snap_handle_pool_size: usize,
    ) -> Self {
        let engine_store_server_helper =
            gen_engine_store_server_helper(engine.engine_store_server_helper);
        // TODO(tiflash) start thread pool
        // let snap_pool = Builder::new(tikv_util::thd_name!("region-task"))
        //     .max_thread_count(snap_handle_pool_size)
        //     .build_future_pool();
        TiFlashObserver {
            peer_id,
            engine_store_server_helper,
            engine,
            sst_importer,
            pre_handle_snapshot_ctx: Arc::new(Mutex::new(PrehandleContext::default())),
            snap_handle_pool_size,
            // apply_snap_pool: Some(Arc::new(snap_pool)),
            apply_snap_pool: None,
        }
    }

    // TODO(tiflash) open observers when TiKV merged.
    pub fn register_to<E: engine_traits::KvEngine>(
        &self,
        coprocessor_host: &mut CoprocessorHost<E>,
    ) {
        // If a observer is repeatedly registered, it can run repeated logic.
        coprocessor_host.registry.register_admin_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxAdminObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_query_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxQueryObserver::new(self.clone()),
        );
        // coprocessor_host.registry.register_apply_snapshot_observer(
        //     TIFLASH_OBSERVER_PRIORITY,
        //     BoxApplySnapshotObserver::new(self.clone()),
        // );
        coprocessor_host.registry.register_region_change_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxRegionChangeObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_pd_task_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxPdTaskObserver::new(self.clone()),
        );
    }

    fn handle_ingest_sst_for_engine_store(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        ssts: &Vec<engine_traits::SstMetaInfo>,
        index: u64,
        term: u64,
    ) -> EngineStoreApplyRes {
        let mut ssts_wrap = vec![];
        let mut sst_views = vec![];

        for sst in ssts {
            let sst = &sst.meta;
            if sst.get_cf_name() == engine_traits::CF_LOCK {
                panic!("should not ingest sst of lock cf");
            }

            // We still need this to filter error ssts.
            // TODO(tiflash) We will remove this as soon as TiKv publicize corresponding function.
            if let Err(e) = check_sst_for_ingestion(sst, &ob_ctx.region()) {
                error!(?e;
                 "proxy ingest fail";
                 "sst" => ?sst,
                 "region" => ?&ob_ctx.region(),
                );
                break;
            }

            ssts_wrap.push((
                self.sst_importer.get_path(sst),
                name_to_cf(sst.get_cf_name()),
            ));
        }

        for (path, cf) in &ssts_wrap {
            sst_views.push((path.to_str().unwrap().as_bytes(), *cf));
        }

        let res = self.engine_store_server_helper.handle_ingest_sst(
            sst_views,
            RaftCmdHeader::new(ob_ctx.region().get_id(), index, term),
        );
        res
    }

    fn handle_error_apply(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,

        region_state: &RegionState,
    ) -> bool {
        // We still need to pass a dummy cmd, to forward updates.
        let cmd_dummy = WriteCmds::new();
        let flash_res = self.engine_store_server_helper.handle_write_raft_cmd(
            &cmd_dummy,
            RaftCmdHeader::new(ob_ctx.region().get_id(), cmd.index, cmd.term),
        );
        match flash_res {
            EngineStoreApplyRes::None => false,
            EngineStoreApplyRes::Persist => !region_state.pending_remove,
            EngineStoreApplyRes::NotFound => false,
        }
    }
}

impl Coprocessor for TiFlashObserver {
    fn stop(&self) {
        // TODO(tiflash)
        // self.apply_snap_pool.as_ref().unwrap().shutdown();
    }
}

impl AdminObserver for TiFlashObserver {
    fn pre_exec_admin(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        req: &AdminRequest,
        index: u64,
        term: u64,
    ) -> bool {
        match req.get_cmd_type() {
            AdminCmdType::CompactLog => {
                if !self.engine_store_server_helper.try_flush_data(
                    ob_ctx.region().get_id(),
                    false,
                    index,
                    term,
                ) {
                    info!("can't flush data, should filter CompactLog";
                        "region" => ?ob_ctx.region(),
                        "req" => ?req,
                    );
                    return true;
                }
                // Otherwise, we can exec CompactLog, without later rolling back.
            }
            AdminCmdType::ComputeHash | AdminCmdType::VerifyHash => {
                // We can't support.
                return true;
            }
            AdminCmdType::TransferLeader => {
                error!("transfer leader won't exec";
                        "region" => ?ob_ctx.region(),
                        "req" => ?req,
                );
                return true;
            }
            _ => (),
        };
        false
    }

    fn post_exec_admin(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
        apply_state: &RaftApplyState,
        region_state: &RegionState,
    ) -> bool {
        fail::fail_point!("on_post_exec_admin", |e| {
            e.unwrap().parse::<bool>().unwrap()
        });
        let request = cmd.request.get_admin_request();
        let response = &cmd.response;
        let admin_reponse = response.get_admin_response();
        let cmd_type = request.get_cmd_type();

        if response.get_header().has_error() {
            info!(
                "error occurs when apply_raft_cmd, {:?}",
                response.get_header().get_error()
            );
            return self.handle_error_apply(ob_ctx, cmd, region_state);
        }

        match cmd_type {
            AdminCmdType::CompactLog | AdminCmdType::ComputeHash | AdminCmdType::VerifyHash => {
                info!(
                    "observe useless admin command";
                    "region_id" => ob_ctx.region().get_id(),
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                    "type" => ?cmd_type,
                );
            }
            _ => {
                info!(
                    "observe admin command";
                    "region_id" => ob_ctx.region().get_id(),
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                    "command" => ?request
                );
            }
        }

        // We wrap `modified_region` into `mut_split()`
        let mut new_response = None;
        match cmd_type {
            AdminCmdType::CommitMerge
            | AdminCmdType::PrepareMerge
            | AdminCmdType::RollbackMerge => {
                let mut r = AdminResponse::default();
                match region_state.modified_region.as_ref() {
                    Some(region) => r.mut_split().set_left(region.clone()),
                    None => {
                        error!("empty modified region";
                            "region_id" => ob_ctx.region().get_id(),
                            "peer_id" => region_state.peer_id,
                            "term" => cmd.term,
                            "index" => cmd.index,
                            "command" => ?request
                        );
                        panic!("empty modified region");
                    }
                }
                new_response = Some(r);
            }
            _ => (),
        }

        let flash_res = {
            match new_response {
                Some(r) => self.engine_store_server_helper.handle_admin_raft_cmd(
                    request,
                    &r,
                    RaftCmdHeader::new(ob_ctx.region().get_id(), cmd.index, cmd.term),
                ),
                None => self.engine_store_server_helper.handle_admin_raft_cmd(
                    request,
                    admin_reponse,
                    RaftCmdHeader::new(ob_ctx.region().get_id(), cmd.index, cmd.term),
                ),
            }
        };
        let persist = match flash_res {
            EngineStoreApplyRes::None => {
                if cmd_type == AdminCmdType::CompactLog {
                    // This could only happen in mock-engine-store when we perform some related tests.
                    // Formal code should never return None for CompactLog now.
                    // If CompactLog can't be done, the engine-store should return `false` in previous `try_flush_data`.
                    error!("applying CompactLog should not return None"; "region_id" => ob_ctx.region().get_id(),
                            "peer_id" => region_state.peer_id, "apply_state" => ?apply_state, "cmd" => ?cmd);
                }
                false
            }
            EngineStoreApplyRes::Persist => !region_state.pending_remove,
            EngineStoreApplyRes::NotFound => {
                error!(
                    "region not found in engine-store, maybe have exec `RemoveNode` first";
                    "region_id" => ob_ctx.region().get_id(),
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                );
                !region_state.pending_remove
            }
        };
        if persist {
            info!("should persist query"; "region_id" => ob_ctx.region().get_id(), "peer_id" => region_state.peer_id, "state" => ?apply_state);
        }
        persist
    }
}

impl QueryObserver for TiFlashObserver {
    fn on_empty_cmd(&self, ob_ctx: &mut ObserverContext<'_>, index: u64, term: u64) {
        fail::fail_point!("on_empty_cmd_normal", |_| {});
        debug!("encounter empty cmd, maybe due to leadership change";
            "region" => ?ob_ctx.region(),
            "index" => index,
            "term" => term,
        );
        // We still need to pass a dummy cmd, to forward updates.
        let cmd_dummy = WriteCmds::new();
        self.engine_store_server_helper.handle_write_raft_cmd(
            &cmd_dummy,
            RaftCmdHeader::new(ob_ctx.region().get_id(), index, term),
        );
    }

    fn post_exec_query(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
        apply_state: &RaftApplyState,
        region_state: &RegionState,
    ) -> bool {
        fail::fail_point!("on_post_exec_normal", |e| {
            e.unwrap().parse::<bool>().unwrap()
        });
        const NONE_STR: &str = "";
        let requests = cmd.request.get_requests();
        let response = &cmd.response;
        if response.get_header().has_error() {
            info!(
                "error occurs when apply_raft_cmd, {:?}",
                response.get_header().get_error()
            );
            return self.handle_error_apply(ob_ctx, cmd, region_state);
        }

        let mut ssts = vec![];
        let mut cmds = WriteCmds::with_capacity(requests.len());
        for req in requests {
            let cmd_type = req.get_cmd_type();
            match cmd_type {
                CmdType::Put => {
                    let put = req.get_put();
                    let cf = name_to_cf(put.get_cf());
                    let (key, value) = (put.get_key(), put.get_value());
                    cmds.push(key, value, WriteCmdType::Put, cf);
                }
                CmdType::Delete => {
                    let del = req.get_delete();
                    let cf = name_to_cf(del.get_cf());
                    let key = del.get_key();
                    cmds.push(key, NONE_STR.as_ref(), WriteCmdType::Del, cf);
                }
                CmdType::IngestSst => {
                    ssts.push(engine_traits::SstMetaInfo {
                        total_bytes: 0,
                        total_kvs: 0,
                        meta: req.get_ingest_sst().get_sst().clone(),
                    });
                }
                CmdType::Snap | CmdType::Get | CmdType::DeleteRange => {
                    // engine-store will drop table, no need DeleteRange
                    // We will filter delete range in engine_tiflash
                    continue;
                }
                CmdType::Prewrite | CmdType::Invalid | CmdType::ReadIndex => {
                    panic!("invalid cmd type, message maybe corrupted");
                }
            }
        }

        let persist = if !ssts.is_empty() {
            assert_eq!(cmds.len(), 0);
            match self.handle_ingest_sst_for_engine_store(ob_ctx, &ssts, cmd.index, cmd.term) {
                EngineStoreApplyRes::None => {
                    // Before, BR/Lightning may let ingest sst cmd contain only one cf,
                    // which may cause that TiFlash can not flush all region cache into column.
                    // so we have a optimization proxy@cee1f003.
                    // The optimization is to introduce a `pending_clean_ssts`,
                    // which holds ssts from being cleaned(by adding into `delete_ssts`),
                    // when engine-store returns None.
                    // However, since this is fixed in tiflash#1811,
                    // this optimization is no longer necessary.
                    // We will print a error here.
                    error!(
                        "should not skip persist for ingest sst";
                        "region_id" => ob_ctx.region().get_id(),
                        "peer_id" => region_state.peer_id,
                        "term" => cmd.term,
                        "index" => cmd.index,
                        "ssts_to_clean" => ?ssts,
                        "sst cf" => ssts.len(),
                    );
                    false
                }
                EngineStoreApplyRes::NotFound | EngineStoreApplyRes::Persist => {
                    info!(
                        "ingest sst success";
                        "region_id" => ob_ctx.region().get_id(),
                        "peer_id" => region_state.peer_id,
                        "term" => cmd.term,
                        "index" => cmd.index,
                        "ssts_to_clean" => ?ssts,
                        "sst cf" => ssts.len(),
                    );
                    !region_state.pending_remove
                }
            }
        } else {
            let flash_res = {
                self.engine_store_server_helper.handle_write_raft_cmd(
                    &cmds,
                    RaftCmdHeader::new(ob_ctx.region().get_id(), cmd.index, cmd.term),
                )
            };
            match flash_res {
                EngineStoreApplyRes::None => false,
                EngineStoreApplyRes::Persist => !region_state.pending_remove,
                EngineStoreApplyRes::NotFound => false,
            }
        };
        fail::fail_point!("on_post_exec_normal_end", |e| {
            e.unwrap().parse::<bool>().unwrap()
        });
        if persist {
            info!("should persist query"; "region_id" => ob_ctx.region().get_id(), "peer_id" => region_state.peer_id, "state" => ?apply_state);
        }
        persist
    }
}

impl RegionChangeObserver for TiFlashObserver {
    fn on_region_changed(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        e: RegionChangeEvent,
        _: StateRole,
    ) {
        if e == RegionChangeEvent::Destroy {
            self.engine_store_server_helper
                .handle_destroy(ob_ctx.region().get_id());
        }
    }
}

impl PdTaskObserver for TiFlashObserver {
    fn on_compute_engine_size(&self, store_size: &mut Option<StoreSizeInfo>) {
        let stats = self.engine_store_server_helper.handle_compute_store_stats();
        store_size.insert(StoreSizeInfo {
            capacity: stats.fs_stats.capacity_size,
            used: stats.fs_stats.used_size,
            avail: stats.fs_stats.avail_size,
        });
    }
}
