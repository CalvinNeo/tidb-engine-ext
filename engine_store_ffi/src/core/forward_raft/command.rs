// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use kvproto::raft_serverpb::RaftTruncatedState;
use raftstore::coprocessor::ObserverContext;

use crate::core::{common::*, ProxyForwarder};

impl<T: Transport + 'static, ER: RaftEngine> ProxyForwarder<T, ER> {
    fn handle_ingest_sst_for_engine_store(
        &self,
        ob_region: &Region,
        ssts: &Vec<engine_traits::SstMetaInfo>,
        index: u64,
        term: u64,
    ) -> EngineStoreApplyRes {
        let mut ssts_wrap = vec![];
        let mut sst_views = vec![];

        info!("begin handle ingest sst";
            "region" => ?ob_region,
            "index" => index,
            "term" => term,
        );

        for sst in ssts {
            let sst = &sst.meta;
            if sst.get_cf_name() == engine_traits::CF_LOCK {
                panic!("should not ingest sst of lock cf");
            }

            // We still need this to filter error ssts.
            if let Err(e) = check_sst_for_ingestion(sst, ob_region) {
                error!(?e;
                 "proxy ingest fail";
                 "sst" => ?sst,
                 "region" => ?ob_region,
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

        self.engine_store_server_helper.handle_ingest_sst(
            sst_views,
            RaftCmdHeader::new(ob_region.get_id(), index, term),
        )
    }

    fn handle_error_apply(
        &self,
        ob_region: &Region,
        cmd: &Cmd,
        region_state: &RegionState,
    ) -> bool {
        // We still need to pass a dummy cmd, to forward updates.
        let cmd_dummy = WriteCmds::new();
        let flash_res = self.engine_store_server_helper.handle_write_raft_cmd(
            &cmd_dummy,
            RaftCmdHeader::new(ob_region.get_id(), cmd.index, cmd.term),
        );
        match flash_res {
            EngineStoreApplyRes::None => false,
            EngineStoreApplyRes::Persist => !region_state.pending_remove,
            EngineStoreApplyRes::NotFound => false,
        }
    }

    pub fn pre_exec_admin(
        &self,
        ob_region: &Region,
        req: &AdminRequest,
        index: u64,
        term: u64,
    ) -> bool {
        match req.get_cmd_type() {
            AdminCmdType::CompactLog => {
                if !self.engine_store_server_helper.try_flush_data(
                    ob_region.get_id(),
                    false,
                    false,
                    index,
                    term,
                ) {
                    info!("can't flush data, filter CompactLog";
                        "region_id" => ?ob_region.get_id(),
                        "region_epoch" => ?ob_region.get_region_epoch(),
                        "index" => index,
                        "term" => term,
                        "compact_index" => req.get_compact_log().get_compact_index(),
                        "compact_term" => req.get_compact_log().get_compact_term(),
                    );
                    return true;
                }
                // Otherwise, we can exec CompactLog, without later rolling
                // back.
            }
            AdminCmdType::ComputeHash | AdminCmdType::VerifyHash => {
                // We can't support.
                return true;
            }
            AdminCmdType::TransferLeader => {
                error!("transfer leader won't exec";
                        "region" => ?ob_region,
                        "req" => ?req,
                );
                return true;
            }
            _ => (),
        };
        false
    }

    pub fn post_exec_admin(
        &self,
        ob_region: &Region,
        cmd: &Cmd,
        apply_state: &RaftApplyState,
        region_state: &RegionState,
        _: &mut ApplyCtxInfo<'_>,
    ) -> bool {
        fail::fail_point!("on_post_exec_admin", |e| {
            e.unwrap().parse::<bool>().unwrap()
        });
        let region_id = ob_region.get_id();
        let request = cmd.request.get_admin_request();
        let response = &cmd.response;
        let admin_reponse = response.get_admin_response();
        let cmd_type = request.get_cmd_type();

        if response.get_header().has_error() {
            info!(
                "error occurs when post_exec_admin, {:?}",
                response.get_header().get_error()
            );
            return self.handle_error_apply(ob_region, cmd, region_state);
        }

        match cmd_type {
            AdminCmdType::CompactLog | AdminCmdType::ComputeHash | AdminCmdType::VerifyHash => {
                info!(
                    "observe useless admin command";
                    "region_id" => region_id,
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                    "type" => ?cmd_type,
                );
            }
            _ => {
                info!(
                    "observe admin command";
                    "region_id" => region_id,
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
                            "region_id" => region_id,
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
                    RaftCmdHeader::new(region_id, cmd.index, cmd.term),
                ),
                None => self.engine_store_server_helper.handle_admin_raft_cmd(
                    request,
                    admin_reponse,
                    RaftCmdHeader::new(region_id, cmd.index, cmd.term),
                ),
            }
        };
        let persist = match flash_res {
            EngineStoreApplyRes::None => {
                if cmd_type == AdminCmdType::CompactLog {
                    // This could only happen in mock-engine-store when we perform some related
                    // tests. Formal code should never return None for
                    // CompactLog now. If CompactLog can't be done, the
                    // engine-store should return `false` in previous `try_flush_data`.
                    error!("applying CompactLog should not return None"; "region_id" => region_id,
                            "peer_id" => region_state.peer_id, "apply_state" => ?apply_state, "cmd" => ?cmd);
                }
                false
            }
            EngineStoreApplyRes::Persist => !region_state.pending_remove,
            EngineStoreApplyRes::NotFound => {
                error!(
                    "region not found in engine-store, maybe have exec `RemoveNode` first";
                    "region_id" => region_id,
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                );
                !region_state.pending_remove
            }
        };
        if persist {
            info!("should persist admin"; "region_id" => region_id, "peer_id" => region_state.peer_id, "state" => ?apply_state);
        }
        persist
    }

    pub fn on_empty_cmd(&self, ob_region: &Region, index: u64, term: u64) {
        let region_id = ob_region.get_id();
        fail::fail_point!("on_empty_cmd_normal", |_| {});
        debug!("encounter empty cmd, maybe due to leadership change";
            "region" => ?ob_region,
            "index" => index,
            "term" => term,
        );
        // We still need to pass a dummy cmd, to forward updates.
        let cmd_dummy = WriteCmds::new();
        self.engine_store_server_helper
            .handle_write_raft_cmd(&cmd_dummy, RaftCmdHeader::new(region_id, index, term));
    }

    pub fn pre_exec_query(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        req: &RaftCmdRequest,
        apply_state: &mut RaftApplyState,
        index: u64,
        term: u64,
        applied_term: u64,
        truncated_state: &mut RaftTruncatedState,
        first_index: &mut u64,
    ) -> bool {
        let cl = rlog::get_custom_log(req).unwrap();
        let tp = cl.get_type();
        let region_id = ob_ctx.region().get_id();

        debug!(
            "pre_exec_query";
            "region_id" => region_id,
            "index" => index,
            "term" => term,
            "applied_term" => applied_term,
            "type" => ?tp,
        );

        match tp {
            rlog::TYPE_ENGINE_META => {
                let cs = cl.get_change_set().unwrap();
                // Ingest file raft log will be handled in `post_exec_query`
                if !cs.has_ingest_files() {
                    let empty_cmds = WriteCmds::new();
                    self.engine_store_server_helper.handle_write_raft_cmd(
                        &empty_cmds,
                        RaftCmdHeader::new(region_id, index, term),
                    );
                    if apply_state.get_truncated_state().get_index() < apply_state.applied_index {
                        if !self.engine_store_server_helper.try_flush_data(
                            ob_ctx.region().get_id(),
                            false,
                            false,
                            apply_state.applied_index,
                            applied_term,
                        ) {
                            info!("can't flush data, filter CompactLog";
                                "region_id" => ?region_id,
                                "region_epoch" => ?ob_ctx.region().get_region_epoch(),
                                "index" => apply_state.applied_index,
                                "term" => applied_term,
                            );
                            return true;
                        }

                        truncated_state.set_index(apply_state.applied_index);
                        truncated_state.set_term(applied_term);

                        *first_index = entry_storage::first_index(apply_state);
                        apply_state.set_truncated_state(truncated_state.clone());
                    }
                    // Return true to skip the normal execution.
                    return true;
                }
            }
            _ => {}
        }
        false
    }

    fn commit_lock(
        &self,
        region_id: u64,
        write_cmds: &mut Vec<WriteCmd>,
        log_index: u64,
        k: &[u8],
        commit_ts: u64,
    ) {
        let lock_key = Key::from_raw(k).into_encoded();
        let lock_bin = self
            .engine_store_server_helper
            .get_lock_by_key(region_id, &lock_key);
        if lock_bin.is_empty() {
            warn!(
                "get empty lock, region id {} key {:?}, commit ts {}, log index {}",
                region_id, k, commit_ts, log_index,
            );
            return;
        }
        let mut lock = txn_types::Lock::parse(&lock_bin).unwrap();
        if lock.lock_type == LockType::Lock || lock.lock_type == LockType::Pessimistic {
            Self::del_lock(write_cmds, lock_key);
            return;
        }
        let short_value = lock.short_value.take().unwrap_or(vec![]);
        let write_type = if short_value.is_empty() {
            txn_types::WriteType::Delete
        } else {
            txn_types::WriteType::Put
        };
        let write_key = Key::from_raw(k).append_ts(commit_ts.into()).into_encoded();
        let write = txn_types::Write::new(write_type, lock.ts.into(), Some(short_value));
        let write_val = write.as_ref().to_bytes();
        write_cmds.push(WriteCmd::new(
            write_key,
            write_val,
            WriteCmdType::Put,
            ColumnFamilyType::Write,
        ));
        Self::del_lock(write_cmds, lock_key);
    }

    fn put_lock(write_cmds: &mut Vec<WriteCmd>, raw_key: &[u8], v: &[u8]) {
        write_cmds.push(WriteCmd::new(
            Key::from_raw(raw_key).into_encoded(),
            v.to_vec(),
            WriteCmdType::Put,
            ColumnFamilyType::Lock,
        ));
    }

    fn del_lock(write_cmds: &mut Vec<WriteCmd>, encoded_lock_key: Vec<u8>) {
        write_cmds.push(WriteCmd::new(
            encoded_lock_key,
            vec![],
            WriteCmdType::Del,
            ColumnFamilyType::Lock,
        ));
    }

    pub fn post_exec_query(
        &self,
        ob_region: &Region,
        cmd: &Cmd,
        apply_state: &mut RaftApplyState,
        region_state: &RegionState,
        apply_ctx_info: &mut ApplyCtxInfo<'_>,
    ) -> bool {
        let cl = rlog::get_custom_log(&cmd.request).unwrap();
        let tp = cl.get_type();
        let index = cmd.index;
        let term = cmd.term;

        debug!(
            "post_exec_query";
            "region_id" => ob_region.get_id(),
            "index" => index,
            "term" => term,
            "type" => ?tp,
        );

        let mut write_cmds = vec![];
        let region_id = ob_region.get_id();

        match tp {
            rlog::TYPE_PREWRITE => cl.iterate_lock(|k, v| {
                Self::put_lock(&mut write_cmds, k, v);
            }),
            rlog::TYPE_PESSIMISTIC_LOCK => cl.iterate_lock(|k, v| {
                Self::put_lock(&mut write_cmds, k, v);
            }),
            rlog::TYPE_COMMIT => cl.iterate_commit(|k, commit_ts| {
                self.commit_lock(region_id, &mut write_cmds, index, k, commit_ts);
            }),
            rlog::TYPE_ONE_PC => {
                cl.iterate_one_pc(|k, v, is_extra, del_lock, start_ts, commit_ts| {
                    if is_extra {
                        return;
                    }
                    let write_key = Key::from_raw(k).append_ts(commit_ts.into()).into_encoded();
                    let write_type = if v.len() == 0 {
                        txn_types::WriteType::Delete
                    } else {
                        txn_types::WriteType::Put
                    };
                    let write =
                        txn_types::Write::new(write_type, start_ts.into(), Some(v.to_vec()));
                    let write_val = write.as_ref().to_bytes();
                    write_cmds.push(WriteCmd::new(
                        write_key,
                        write_val,
                        WriteCmdType::Put,
                        ColumnFamilyType::Write,
                    ));
                    if del_lock {
                        Self::del_lock(&mut write_cmds, Key::from_raw(k).into_encoded());
                    }
                })
            }
            rlog::TYPE_ROLLBACK => cl.iterate_rollback(|k, start_ts, del_lock| {
                if del_lock {
                    Self::del_lock(&mut write_cmds, Key::from_raw(k).into_encoded());
                }
            }),
            rlog::TYPE_PESSIMISTIC_ROLLBACK => {
                cl.iterate_del_lock(|k| {
                    Self::del_lock(&mut write_cmds, Key::from_raw(k).into_encoded());
                });
            }
            rlog::TYPE_ENGINE_META => {
                let cs = cl.get_change_set().unwrap();
                if cs.has_ingest_files() {
                    let cs_bin = cs.write_to_bytes().unwrap();
                    let sst_views = vec![(cs_bin.as_slice(), ColumnFamilyType::Write)];
                    let header = RaftCmdHeader::new(region_id, index, term);
                    self.engine_store_server_helper
                        .handle_ingest_sst(sst_views, header);

                    // update apply_state
                    apply_state.set_applied_index(index);
                    return true;
                }
            }
            rlog::TYPE_RESOLVE_LOCK => cl.iterate_resolve_lock(|tp, k, ts, del_lock| match tp {
                rlog::TYPE_COMMIT => self.commit_lock(region_id, &mut write_cmds, index, k, ts),
                rlog::TYPE_ROLLBACK => {
                    if del_lock {
                        Self::del_lock(&mut write_cmds, Key::from_raw(k).into_encoded());
                    }
                }
                _ => unreachable!("unexpected custom log type: {:?}", tp),
            }),
            rlog::TYPE_SWITCH_MEM_TABLE => {}
            rlog::TYPE_TRIGGER_TRIM_OVER_BOUND => {}
            _ => panic!("unknown custom log type"),
        }

        let mut cmds = WriteCmds::with_capacity(write_cmds.len());
        for write_cmd in &write_cmds {
            cmds.push(
                &write_cmd.key,
                &write_cmd.val,
                write_cmd.cmd_type,
                write_cmd.cf,
            );
        }
        self.engine_store_server_helper
            .handle_write_raft_cmd(&cmds, RaftCmdHeader::new(ob_region.get_id(), index, term));

        // update apply_state
        apply_state.set_applied_index(index);

        // `false` means no need to persist apply_state
        false
    }

    pub fn on_raft_message(&self, msg: &RaftMessage) -> bool {
        !self.maybe_fast_path_tick(&msg)
    }
}
