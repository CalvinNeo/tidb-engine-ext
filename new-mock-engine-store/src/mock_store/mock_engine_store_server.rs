// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    pin::Pin,
    sync::{atomic::Ordering, Mutex},
    time::Duration,
};

use int_enum::IntEnum;

use crate::{
    common::*, interfaces_ffi::PageAndCppStrWithView, mock_cluster, mock_cluster::TiFlashEngine,
    mock_core::*, mock_page_storage::*, node::NodeCluster, server::ServerCluster,
};

pub struct EngineStoreServer {
    pub id: u64,
    pub engines: Option<Engines<TiFlashEngine, engine_rocks::RocksEngine>>,
    pub kvstore: HashMap<RegionId, Box<Region>>,
    pub proxy_compat: bool,
    pub mock_cfg: MockConfig,
    pub region_states: RefCell<HashMap<RegionId, RegionStats>>,
    pub page_storage: MockPageStorage,
}

impl EngineStoreServer {
    pub fn new(
        id: u64,
        engines: Option<Engines<TiFlashEngine, engine_rocks::RocksEngine>>,
    ) -> Self {
        EngineStoreServer {
            id,
            engines,
            kvstore: Default::default(),
            proxy_compat: false,
            mock_cfg: MockConfig::default(),
            region_states: RefCell::new(Default::default()),
            page_storage: Default::default(),
        }
    }

    pub fn mutate_region_states<F: Fn(&mut RegionStats)>(&self, region_id: RegionId, f: F) {
        let has = self.region_states.borrow().contains_key(&region_id);
        if !has {
            self.region_states
                .borrow_mut()
                .insert(region_id, Default::default());
        }
        f(self.region_states.borrow_mut().get_mut(&region_id).unwrap())
    }

    pub fn get_mem(
        &self,
        region_id: u64,
        cf: interfaces_ffi::ColumnFamilyType,
        key: &Vec<u8>,
    ) -> Option<&Vec<u8>> {
        match self.kvstore.get(&region_id) {
            Some(region) => {
                let bmap = &region.data[cf as usize];
                bmap.get(key)
            }
            None => None,
        }
    }

    pub fn stop(&mut self) {
        for (_, region) in self.kvstore.iter_mut() {
            for cf in region.pending_write.iter_mut() {
                cf.clear();
            }
            for cf in region.pending_delete.iter_mut() {
                cf.clear();
            }
            for cf in region.data.iter_mut() {
                cf.clear();
            }
            region.apply_state = Default::default();
            // We don't clear applied_term.
        }
    }

    // False alarm
    #[allow(clippy::needless_collect)]
    pub fn restore(&mut self) {
        // TODO We should actually read from engine store's persistence.
        // However, since mock engine store don't persist itself,
        // we read from proxy instead.
        unsafe {
            let region_ids: Vec<u64> = self.kvstore.keys().cloned().collect();
            for region_id in region_ids.into_iter() {
                load_from_db(self, region_id);
            }
        }
    }

    pub unsafe fn write_to_db_by_region_id(&mut self, region_id: u64, reason: String) {
        let kv = &mut self.engines.as_mut().unwrap().kv;
        let region = self.kvstore.get_mut(&region_id).unwrap();
        write_to_db_data_by_engine(self.id, kv, region, reason)
    }
}

pub struct EngineStoreServerWrap {
    pub engine_store_server: *mut EngineStoreServer,
    pub maybe_proxy_helper: std::option::Option<*mut RaftStoreProxyFFIHelper>,
    // Call `gen_cluster(cluster_ptr)`, and get which cluster this Server belong to.
    pub cluster_ptr: isize,
}

fn set_new_region_peer(new_region: &mut Region, store_id: u64) {
    if let Some(peer) = new_region
        .region
        .get_peers()
        .iter()
        .find(|&peer| peer.get_store_id() == store_id)
    {
        new_region.peer = peer.clone();
    } else {
        // This happens when region is not found.
    }
}

pub fn make_new_region(
    maybe_from_region: Option<kvproto::metapb::Region>,
    maybe_store_id: Option<u64>,
) -> Region {
    let mut region = Region {
        region: maybe_from_region.unwrap_or_default(),
        ..Default::default()
    };
    if let Some(store_id) = maybe_store_id {
        set_new_region_peer(&mut region, store_id);
    }
    region
        .apply_state
        .mut_truncated_state()
        .set_index(raftstore::store::RAFT_INIT_LOG_INDEX);
    region
        .apply_state
        .mut_truncated_state()
        .set_term(raftstore::store::RAFT_INIT_LOG_TERM);
    region.set_applied(
        raftstore::store::RAFT_INIT_LOG_INDEX,
        raftstore::store::RAFT_INIT_LOG_TERM,
    );
    region
}

pub fn write_kv_in_mem(region: &mut Region, cf_index: usize, k: &[u8], v: &[u8]) {
    let data = &mut region.data[cf_index];
    let pending_delete = &mut region.pending_delete[cf_index];
    let pending_write = &mut region.pending_write[cf_index];
    pending_delete.remove(k);
    data.insert(k.to_vec(), v.to_vec());
    pending_write.insert(k.to_vec(), v.to_vec());
}

fn delete_kv_in_mem(region: &mut Region, cf_index: usize, k: &[u8]) {
    let data = &mut region.data[cf_index];
    let pending_delete = &mut region.pending_delete[cf_index];
    pending_delete.insert(k.to_vec());
    data.remove(k);
}

unsafe fn load_data_from_db(store: &mut EngineStoreServer, region_id: u64) {
    let store_id = store.id;
    let engine = &mut store.engines.as_mut().unwrap().kv;
    let region = store.kvstore.get_mut(&region_id).unwrap();
    for cf in 0..3 {
        let cf_name = cf_to_name(cf.into());
        region.data[cf].clear();
        region.pending_delete[cf].clear();
        region.pending_write[cf].clear();
        let start = region.region.get_start_key().to_owned();
        let end = region.region.get_end_key().to_owned();
        engine
            .scan(cf_name, &start, &end, false, |k, v| {
                let origin_key = if keys::validate_data_key(k) {
                    keys::origin_key(k).to_vec()
                } else {
                    k.to_vec()
                };
                debug!("restored data";
                    "store" => store_id,
                    "region_id" => region_id,
                    "cf" => cf,
                    "k" => ?k,
                    "origin_key" => ?origin_key,
                );
                region.data[cf].insert(origin_key, v.to_vec());
                Ok(true)
            })
            .unwrap();
    }
}

pub(crate) unsafe fn load_from_db(store: &mut EngineStoreServer, region_id: u64) {
    let engine = &mut store.engines.as_mut().unwrap().kv;
    let apply_state: RaftApplyState = general_get_apply_state(engine, region_id).unwrap();
    let region_state: RegionLocalState = general_get_region_local_state(engine, region_id).unwrap();

    let region = store.kvstore.get_mut(&region_id).unwrap();
    region.apply_state = apply_state;
    region.region = region_state.get_region().clone();
    set_new_region_peer(region, store.id);

    load_data_from_db(store, region_id);
}

pub(crate) unsafe fn write_to_db_data(
    store: &mut EngineStoreServer,
    region: &mut Box<Region>,
    reason: String,
) {
    let kv = &mut store.engines.as_mut().unwrap().kv;
    write_to_db_data_by_engine(store.id, kv, region, reason)
}

pub(crate) unsafe fn write_to_db_data_by_engine(
    store_id: u64,
    kv: &TiFlashEngine,
    region: &mut Box<Region>,
    reason: String,
) {
    info!("mock flush to engine";
        "region" => ?region.region,
        "store_id" => store_id,
        "reason" => reason
    );
    for cf in 0..3 {
        let pending_write = std::mem::take(region.pending_write.as_mut().get_mut(cf).unwrap());
        let mut pending_remove =
            std::mem::take(region.pending_delete.as_mut().get_mut(cf).unwrap());
        for (k, v) in pending_write.into_iter() {
            let tikv_key = keys::data_key(k.as_slice());
            let cf_name = cf_to_name(cf.into());
            if !pending_remove.contains(&k) {
                kv.rocks.put_cf(cf_name, tikv_key.as_slice(), &v).unwrap();
            } else {
                pending_remove.remove(&k);
            }
        }
        let cf_name = cf_to_name(cf.into());
        for k in pending_remove.into_iter() {
            let tikv_key = keys::data_key(k.as_slice());
            kv.rocks.delete_cf(cf_name, &tikv_key).unwrap();
        }
    }
}

pub unsafe fn create_cpp_str_parts(
    s: Option<Vec<u8>>,
) -> (interfaces_ffi::RawCppPtr, interfaces_ffi::BaseBuffView) {
    match s {
        Some(s) => {
            let len = s.len() as u64;
            let ptr = Box::into_raw(Box::new(s)); // leak
            (
                interfaces_ffi::RawCppPtr {
                    ptr: ptr as RawVoidPtr,
                    type_: RawCppPtrTypeImpl::String.into(),
                },
                interfaces_ffi::BaseBuffView {
                    data: (*ptr).as_ptr() as *const _,
                    len,
                },
            )
        }
        None => (
            interfaces_ffi::RawCppPtr {
                ptr: std::ptr::null_mut(),
                type_: RawCppPtrTypeImpl::None.into(),
            },
            interfaces_ffi::BaseBuffView {
                data: std::ptr::null(),
                len: 0,
            },
        ),
    }
}

pub unsafe fn create_cpp_str(s: Option<Vec<u8>>) -> interfaces_ffi::CppStrWithView {
    let (p, v) = create_cpp_str_parts(s);
    interfaces_ffi::CppStrWithView { inner: p, view: v }
}

#[allow(clippy::single_element_loop)]
pub fn move_data_from(
    engine_store_server: &mut EngineStoreServer,
    old_region_id: u64,
    new_region_ids: &[u64],
) {
    let kvs = {
        let old_region = engine_store_server.kvstore.get_mut(&old_region_id).unwrap();
        let res = old_region.data.clone();
        old_region.data = Default::default();
        res
    };
    for new_region_id in new_region_ids {
        let new_region = engine_store_server.kvstore.get_mut(&new_region_id).unwrap();
        let new_region_meta = new_region.region.clone();
        let start_key = new_region_meta.get_start_key();
        let end_key = new_region_meta.get_end_key();
        for cf in &[interfaces_ffi::ColumnFamilyType::Default] {
            let cf = (*cf) as usize;
            for (k, v) in &kvs[cf] {
                let k = k.as_slice();
                let v = v.as_slice();
                match k {
                    keys::PREPARE_BOOTSTRAP_KEY | keys::STORE_IDENT_KEY => {}
                    _ => {
                        if k >= start_key && (end_key.is_empty() || k < end_key) {
                            debug!(
                                "move region data {:?} {:?} from {} to {}",
                                k, v, old_region_id, new_region_id
                            );
                            write_kv_in_mem(new_region, cf, k, v);
                        }
                    }
                };
            }
        }
    }
}

impl EngineStoreServerWrap {
    pub fn new(
        engine_store_server: *mut EngineStoreServer,
        maybe_proxy_helper: std::option::Option<*mut RaftStoreProxyFFIHelper>,
        cluster_ptr: isize,
    ) -> Self {
        Self {
            engine_store_server,
            maybe_proxy_helper,
            cluster_ptr,
        }
    }

    unsafe fn handle_admin_raft_cmd(
        &mut self,
        req: &kvproto::raft_cmdpb::AdminRequest,
        resp: &kvproto::raft_cmdpb::AdminResponse,
        header: interfaces_ffi::RaftCmdHeader,
    ) -> interfaces_ffi::EngineStoreApplyRes {
        let region_id = header.region_id;
        let node_id = (*self.engine_store_server).id;
        info!("handle_admin_raft_cmd";
            "node_id"=>node_id,
            "request"=>?req,
            "response"=>?resp,
            "header"=>?header,
            "region_id"=>header.region_id,
        );
        let do_handle_admin_raft_cmd =
            move |region: &mut Box<Region>, engine_store_server: &mut EngineStoreServer| {
                if region.apply_state.get_applied_index() >= header.index {
                    // If it is a old entry.
                    error!("obsolete admin index";
                    "apply_state"=>?region.apply_state,
                    "header"=>?header,
                    "node_id"=>node_id,
                    );
                    panic!("observe obsolete admin index");
                    // return interfaces_ffi::EngineStoreApplyRes::None;
                }
                match req.get_cmd_type() {
                    AdminCmdType::ChangePeer | AdminCmdType::ChangePeerV2 => {
                        let new_region_meta = resp.get_change_peer().get_region();
                        let old_peer_id = {
                            let old_region =
                                engine_store_server.kvstore.get_mut(&region_id).unwrap();
                            old_region.region = new_region_meta.clone();
                            region.set_applied(header.index, header.term);
                            old_region.peer.get_store_id()
                        };

                        let mut do_remove = true;
                        if old_peer_id != 0 {
                            for peer in new_region_meta.get_peers().iter() {
                                if peer.get_store_id() == old_peer_id {
                                    // Should not remove region
                                    do_remove = false;
                                }
                            }
                        } else {
                            // If old_peer_id is 0, seems old_region.peer is not set, just neglect
                            // for convenience.
                            do_remove = false;
                        }
                        if do_remove {
                            let removed = engine_store_server.kvstore.remove(&region_id);
                            // We need to also remove apply state, thus we need to know peer_id
                            debug!(
                                "Remove region {:?} peer_id {} at node {}, for new meta {:?}",
                                removed.unwrap().region,
                                old_peer_id,
                                node_id,
                                new_region_meta
                            );
                        }
                    }
                    AdminCmdType::BatchSplit => {
                        let regions = resp.get_splits().regions.as_ref();

                        for i in 0..regions.len() {
                            let region_meta = regions.get(i).unwrap();
                            if region_meta.id == region_id {
                                // This is the derived region
                                debug!(
                                    "region {} is derived by split at peer {} with meta {:?}",
                                    region_meta.id, node_id, region_meta
                                );
                                assert!(engine_store_server.kvstore.contains_key(&region_meta.id));
                                engine_store_server
                                    .kvstore
                                    .get_mut(&region_meta.id)
                                    .unwrap()
                                    .region = region_meta.clone();
                            } else {
                                // Should split data into new region
                                debug!(
                                    "new region {} generated by split at peer {} with meta {:?}",
                                    region_meta.id, node_id, region_meta
                                );
                                let new_region =
                                    make_new_region(Some(region_meta.clone()), Some(node_id));

                                // No need to split data because all KV are stored in the same
                                // RocksDB. TODO But we still need
                                // to clean all in-memory data.
                                // We can't assert `region_meta.id` is brand new here
                                engine_store_server
                                    .kvstore
                                    .insert(region_meta.id, Box::new(new_region));
                            }
                        }

                        {
                            // Move data
                            let region_ids =
                                regions.iter().map(|r| r.get_id()).collect::<Vec<u64>>();
                            move_data_from(engine_store_server, region_id, region_ids.as_slice());
                        }
                    }
                    AdminCmdType::PrepareMerge => {
                        let tikv_region = resp.get_split().get_left();

                        let _target = req.prepare_merge.as_ref().unwrap().target.as_ref();
                        let region_meta = &mut (engine_store_server
                            .kvstore
                            .get_mut(&region_id)
                            .unwrap()
                            .region);

                        // Increase self region conf version and version
                        let region_epoch = region_meta.region_epoch.as_mut().unwrap();
                        let new_version = region_epoch.version + 1;
                        region_epoch.set_version(new_version);
                        // TODO this check may fail
                        // assert_eq!(tikv_region.get_region_epoch().get_version(), new_version);
                        let conf_version = region_epoch.conf_ver + 1;
                        region_epoch.set_conf_ver(conf_version);
                        assert_eq!(tikv_region.get_region_epoch().get_conf_ver(), conf_version);

                        {
                            let region = engine_store_server.kvstore.get_mut(&region_id).unwrap();
                            region.set_applied(header.index, header.term);
                        }
                        // We don't handle MergeState and PeerState here
                    }
                    AdminCmdType::CommitMerge => {
                        fail::fail_point!("ffi_before_commit_merge", |_| {
                            return interfaces_ffi::EngineStoreApplyRes::Persist;
                        });
                        let (target_id, source_id) =
                            { (region_id, req.get_commit_merge().get_source().get_id()) };
                        {
                            let target_region =
                                &mut (engine_store_server.kvstore.get_mut(&region_id).unwrap());

                            let target_region_meta = &mut target_region.region;
                            let target_version =
                                target_region_meta.get_region_epoch().get_version();
                            let source_region = req.get_commit_merge().get_source();
                            let source_version = source_region.get_region_epoch().get_version();

                            let new_version = std::cmp::max(source_version, target_version) + 1;
                            target_region_meta
                                .mut_region_epoch()
                                .set_version(new_version);
                            assert_eq!(
                                target_region_meta.get_region_epoch().get_version(),
                                new_version
                            );

                            // No need to merge data
                            let source_at_left = if source_region.get_start_key().is_empty() {
                                true
                            } else if target_region_meta.get_start_key().is_empty() {
                                false
                            } else {
                                source_region
                                    .get_end_key()
                                    .cmp(target_region_meta.get_start_key())
                                    == std::cmp::Ordering::Equal
                            };

                            // The validation of applied result on TiFlash's side.
                            let tikv_target_region_meta = resp.get_split().get_left();
                            if source_at_left {
                                target_region_meta
                                    .set_start_key(source_region.get_start_key().to_vec());
                                assert_eq!(
                                    tikv_target_region_meta.get_start_key(),
                                    target_region_meta.get_start_key()
                                );
                            } else {
                                target_region_meta
                                    .set_end_key(source_region.get_end_key().to_vec());
                                assert_eq!(
                                    tikv_target_region_meta.get_end_key(),
                                    target_region_meta.get_end_key()
                                );
                            }
                            target_region.set_applied(header.index, header.term);
                        }
                        {
                            move_data_from(engine_store_server, source_id, &[target_id]);
                        }
                        let to_remove = req.get_commit_merge().get_source().get_id();
                        engine_store_server.kvstore.remove(&to_remove);
                    }
                    AdminCmdType::RollbackMerge => {
                        let region = engine_store_server.kvstore.get_mut(&region_id).unwrap();
                        let region_meta = &mut region.region;
                        let new_version = region_meta.get_region_epoch().get_version() + 1;
                        let region_epoch = region_meta.region_epoch.as_mut().unwrap();
                        region_epoch.set_version(new_version);

                        region.set_applied(header.index, header.term);
                    }
                    AdminCmdType::CompactLog => {
                        // We can always do compact, since a executed CompactLog must follow a
                        // successful persist.
                        let region = engine_store_server.kvstore.get_mut(&region_id).unwrap();
                        let state = &mut region.apply_state;
                        let compact_index = req.get_compact_log().get_compact_index();
                        let compact_term = req.get_compact_log().get_compact_term();
                        state.mut_truncated_state().set_index(compact_index);
                        state.mut_truncated_state().set_term(compact_term);
                        region.set_applied(header.index, header.term);
                    }
                    _ => {
                        region.set_applied(header.index, header.term);
                    }
                }
                // Do persist or not
                match req.get_cmd_type() {
                    AdminCmdType::CompactLog => {
                        fail::fail_point!("no_persist_compact_log", |_| {
                            // Persist data, but don't persist meta.
                            interfaces_ffi::EngineStoreApplyRes::None
                        });
                        interfaces_ffi::EngineStoreApplyRes::Persist
                    }
                    AdminCmdType::PrepareFlashback | AdminCmdType::FinishFlashback => {
                        fail::fail_point!("no_persist_flashback", |_| {
                            // Persist data, but don't persist meta.
                            interfaces_ffi::EngineStoreApplyRes::None
                        });
                        interfaces_ffi::EngineStoreApplyRes::Persist
                    }
                    _ => interfaces_ffi::EngineStoreApplyRes::Persist,
                }
            };

        let res = match (*self.engine_store_server).kvstore.entry(region_id) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                do_handle_admin_raft_cmd(o.get_mut(), &mut (*self.engine_store_server))
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                // Currently in tests, we don't handle commands like BatchSplit,
                // and sometimes we don't bootstrap region 1,
                // so it is normal if we find no region.
                warn!("region {} not found, create for {}", region_id, node_id);
                let new_region = v.insert(Default::default());
                assert!((*self.engine_store_server).kvstore.contains_key(&region_id));
                do_handle_admin_raft_cmd(new_region, &mut (*self.engine_store_server))
            }
        };

        let region = match (*self.engine_store_server).kvstore.get_mut(&region_id) {
            Some(r) => Some(r),
            None => {
                warn!(
                    "still can't find region {} for {}, may be remove due to confchange",
                    region_id, node_id
                );
                None
            }
        };
        if res == interfaces_ffi::EngineStoreApplyRes::Persist {
            // Persist tells ApplyDelegate to do a commit.
            // So we also need a persist of actual data on engine-store' side.
            if let Some(region) = region {
                if req.get_cmd_type() == AdminCmdType::CompactLog {
                    // We already persist when fn_try_flush_data.
                } else {
                    write_to_db_data(
                        &mut (*self.engine_store_server),
                        region,
                        format!("admin {:?}", req),
                    );
                }
            }
        }
        res
    }

    unsafe fn handle_write_raft_cmd(
        &mut self,
        cmds: interfaces_ffi::WriteCmdsView,
        header: interfaces_ffi::RaftCmdHeader,
    ) -> interfaces_ffi::EngineStoreApplyRes {
        let region_id = header.region_id;
        let server = &mut (*self.engine_store_server);
        let node_id = (*self.engine_store_server).id;
        let _kv = &mut (*self.engine_store_server).engines.as_mut().unwrap().kv;
        let proxy_compat = server.proxy_compat;
        let mut do_handle_write_raft_cmd = move |region: &mut Box<Region>| {
            if region.apply_state.get_applied_index() >= header.index {
                debug!("obsolete write index";
                "apply_state"=>?region.apply_state,
                "header"=>?header,
                "node_id"=>node_id,
                );
                panic!("observe obsolete write index");
                // return interfaces_ffi::EngineStoreApplyRes::None;
            }
            for i in 0..cmds.len {
                let key = &*cmds.keys.add(i as _);
                let val = &*cmds.vals.add(i as _);
                let k = &key.to_slice();
                let v = &val.to_slice();
                let tp = &*cmds.cmd_types.add(i as _);
                let cf = &*cmds.cmd_cf.add(i as _);
                let cf_index = (*cf) as u8;
                debug!(
                    "handle_write_raft_cmd with kv";
                    "k" => ?&k[..std::cmp::min(4usize, k.len())],
                    "v" => ?&v[..std::cmp::min(4usize, v.len())],
                    "region_id" => region_id,
                    "node_id" => server.id,
                    "header" => ?header,
                );
                let _data = &mut region.data[cf_index as usize];
                match tp {
                    interfaces_ffi::WriteCmdType::Put => {
                        write_kv_in_mem(region.as_mut(), cf_index as usize, k, v);
                    }
                    interfaces_ffi::WriteCmdType::Del => {
                        delete_kv_in_mem(region.as_mut(), cf_index as usize, k);
                    }
                }
            }
            // Advance apply index, but do not persist
            region.set_applied(header.index, header.term);
            if !proxy_compat {
                // If we don't support new proxy, we persist everytime.
                write_to_db_data(server, region, "write".to_string());
            }
            interfaces_ffi::EngineStoreApplyRes::None
        };

        match (*self.engine_store_server).kvstore.entry(region_id) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                do_handle_write_raft_cmd(o.get_mut())
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                warn!("region {} not found", region_id);
                do_handle_write_raft_cmd(v.insert(Default::default()))
            }
        }
    }
}

unsafe extern "C" fn ffi_set_pb_msg_by_bytes(
    type_: interfaces_ffi::MsgPBType,
    ptr: interfaces_ffi::RawVoidPtr,
    buff: interfaces_ffi::BaseBuffView,
) {
    match type_ {
        interfaces_ffi::MsgPBType::ReadIndexResponse => {
            let v = &mut *(ptr as *mut kvproto::kvrpcpb::ReadIndexResponse);
            v.merge_from_bytes(buff.to_slice()).unwrap();
        }
        interfaces_ffi::MsgPBType::ServerInfoResponse => {
            let v = &mut *(ptr as *mut kvproto::diagnosticspb::ServerInfoResponse);
            v.merge_from_bytes(buff.to_slice()).unwrap();
        }
        interfaces_ffi::MsgPBType::RegionLocalState => {
            let v = &mut *(ptr as *mut kvproto::raft_serverpb::RegionLocalState);
            v.merge_from_bytes(buff.to_slice()).unwrap();
        }
    }
}

pub fn gen_engine_store_server_helper(
    wrap: Pin<&EngineStoreServerWrap>,
) -> EngineStoreServerHelper {
    EngineStoreServerHelper {
        magic_number: interfaces_ffi::RAFT_STORE_PROXY_MAGIC_NUMBER,
        version: interfaces_ffi::RAFT_STORE_PROXY_VERSION,
        inner: &(*wrap) as *const EngineStoreServerWrap as *mut _,
        fn_gen_cpp_string: Some(ffi_gen_cpp_string),
        fn_handle_write_raft_cmd: Some(ffi_handle_write_raft_cmd),
        fn_handle_admin_raft_cmd: Some(ffi_handle_admin_raft_cmd),
        fn_need_flush_data: Some(ffi_need_flush_data),
        fn_try_flush_data: Some(ffi_try_flush_data),
        fn_atomic_update_proxy: Some(ffi_atomic_update_proxy),
        fn_handle_destroy: Some(ffi_handle_destroy),
        fn_handle_ingest_sst: Some(ffi_handle_ingest_sst),
        fn_handle_compute_store_stats: Some(ffi_handle_compute_store_stats),
        fn_handle_get_engine_store_server_status: None,
        fn_pre_handle_snapshot: Some(ffi_pre_handle_snapshot),
        fn_apply_pre_handled_snapshot: Some(ffi_apply_pre_handled_snapshot),
        fn_handle_http_request: None,
        fn_check_http_uri_available: None,
        fn_gc_raw_cpp_ptr: Some(ffi_gc_raw_cpp_ptr),
        fn_gc_raw_cpp_ptr_carr: Some(ffi_gc_raw_cpp_ptr_carr),
        fn_gc_special_raw_cpp_ptr: Some(ffi_gc_special_raw_cpp_ptr),
        fn_get_config: None,
        fn_set_store: None,
        fn_set_pb_msg_by_bytes: Some(ffi_set_pb_msg_by_bytes),
        fn_handle_safe_ts_update: Some(ffi_handle_safe_ts_update),
        fn_fast_add_peer: Some(ffi_fast_add_peer),
        fn_create_write_batch: Some(ffi_mockps_create_write_batch),
        fn_write_batch_put_page: Some(ffi_mockps_write_batch_put_page),
        fn_write_batch_del_page: Some(ffi_mockps_write_batch_del_page),
        fn_write_batch_size: Some(ffi_mockps_write_batch_size),
        fn_write_batch_is_empty: Some(ffi_mockps_write_batch_is_empty),
        fn_write_batch_merge: Some(ffi_mockps_write_batch_merge),
        fn_write_batch_clear: Some(ffi_mockps_write_batch_clear),
        fn_consume_write_batch: Some(ffi_mockps_consume_write_batch),
        fn_handle_read_page: Some(ffi_mockps_handle_read_page),
        fn_handle_purge_pagestorage: Some(ffi_mockps_handle_purge_pagestorage),
        fn_handle_scan_page: Some(ffi_mockps_handle_scan_page),
        fn_handle_seek_ps_key: Some(ffi_mockps_handle_seek_ps_key),
        fn_ps_is_empty: Some(ffi_mockps_ps_is_empty),
    }
}

pub unsafe fn into_engine_store_server_wrap(
    arg1: *const interfaces_ffi::EngineStoreServerWrap,
) -> &'static mut EngineStoreServerWrap {
    &mut *(arg1 as *mut EngineStoreServerWrap)
}

unsafe extern "C" fn ffi_handle_admin_raft_cmd(
    arg1: *const interfaces_ffi::EngineStoreServerWrap,
    arg2: interfaces_ffi::BaseBuffView,
    arg3: interfaces_ffi::BaseBuffView,
    arg4: interfaces_ffi::RaftCmdHeader,
) -> interfaces_ffi::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    let mut req = kvproto::raft_cmdpb::AdminRequest::default();
    let mut resp = kvproto::raft_cmdpb::AdminResponse::default();
    req.merge_from_bytes(arg2.to_slice()).unwrap();
    resp.merge_from_bytes(arg3.to_slice()).unwrap();
    store.handle_admin_raft_cmd(&req, &resp, arg4)
}

unsafe extern "C" fn ffi_handle_write_raft_cmd(
    arg1: *const interfaces_ffi::EngineStoreServerWrap,
    arg2: interfaces_ffi::WriteCmdsView,
    arg3: interfaces_ffi::RaftCmdHeader,
) -> interfaces_ffi::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    store.handle_write_raft_cmd(arg2, arg3)
}

#[repr(u32)]
#[derive(IntEnum, Copy, Clone)]
pub enum RawCppPtrTypeImpl {
    None = 0,
    String = 1,
    PreHandledSnapshotWithBlock = 11,
    WakerNotifier = 12,
    PSWriteBatch = 13,
    PSUniversalPage = 14,
    PSPageAndCppStr = 15,
}

impl From<RawCppPtrTypeImpl> for interfaces_ffi::RawCppPtrType {
    fn from(value: RawCppPtrTypeImpl) -> Self {
        assert_type_eq::assert_type_eq!(interfaces_ffi::RawCppPtrType, u32);
        value.int_value()
    }
}

impl From<interfaces_ffi::RawCppPtrType> for RawCppPtrTypeImpl {
    fn from(value: interfaces_ffi::RawCppPtrType) -> Self {
        if let Ok(s) = RawCppPtrTypeImpl::from_int(value) {
            s
        } else {
            panic!("unknown RawCppPtrType {:?}", value);
        }
    }
}

extern "C" fn ffi_need_flush_data(
    _arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    _region_id: u64,
) -> u8 {
    fail::fail_point!("need_flush_data", |e| e.unwrap().parse::<u8>().unwrap());
    true as u8
}

unsafe extern "C" fn ffi_try_flush_data(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    region_id: u64,
    _try_until_succeed: u8,
    index: u64,
    term: u64,
) -> u8 {
    let store = into_engine_store_server_wrap(arg1);
    let kvstore = &mut (*store.engine_store_server).kvstore;
    // If we can't find region here, we return true so proxy can trigger a
    // CompactLog. The triggered CompactLog will be handled by
    // `handleUselessAdminRaftCmd`, and result in a
    // `EngineStoreApplyRes::NotFound`. Proxy will print this message and
    // continue: `region not found in engine-store, maybe have exec `RemoveNode`
    // first`.
    let region = match kvstore.get_mut(&region_id) {
        Some(r) => r,
        None => {
            if (*store.engine_store_server)
                .mock_cfg
                .panic_when_flush_no_found
                .load(Ordering::SeqCst)
            {
                panic!(
                    "ffi_try_flush_data no found region {} [index {} term {}], store {}",
                    region_id,
                    index,
                    term,
                    (*store.engine_store_server).id
                );
            } else {
                return 1;
            }
        }
    };
    fail::fail_point!("try_flush_data", |e| {
        let b = e.unwrap().parse::<u8>().unwrap();
        if b == 1 {
            write_to_db_data(
                &mut (*store.engine_store_server),
                region,
                "fn_try_flush_data".to_string(),
            );
        }
        b
    });
    write_to_db_data(
        &mut (*store.engine_store_server),
        region,
        "fn_try_flush_data".to_string(),
    );
    true as u8
}

extern "C" fn ffi_gen_cpp_string(s: interfaces_ffi::BaseBuffView) -> interfaces_ffi::RawCppPtr {
    let str = Box::new(Vec::from(s.to_slice()));
    let ptr = Box::into_raw(str);
    interfaces_ffi::RawCppPtr {
        ptr: ptr as *mut _,
        type_: RawCppPtrTypeImpl::String.into(),
    }
}

pub struct RawCppStringPtrGuard(interfaces_ffi::RawCppStringPtr);

impl Default for RawCppStringPtrGuard {
    fn default() -> Self {
        Self(std::ptr::null_mut())
    }
}

impl std::convert::AsRef<interfaces_ffi::RawCppStringPtr> for RawCppStringPtrGuard {
    fn as_ref(&self) -> &interfaces_ffi::RawCppStringPtr {
        &self.0
    }
}
impl std::convert::AsMut<interfaces_ffi::RawCppStringPtr> for RawCppStringPtrGuard {
    fn as_mut(&mut self) -> &mut interfaces_ffi::RawCppStringPtr {
        &mut self.0
    }
}

impl Drop for RawCppStringPtrGuard {
    fn drop(&mut self) {
        interfaces_ffi::RawCppPtr {
            ptr: self.0 as *mut _,
            type_: RawCppPtrTypeImpl::String.into(),
        };
    }
}

impl RawCppStringPtrGuard {
    pub fn as_str(&self) -> &[u8] {
        let s = self.0 as *mut Vec<u8>;
        unsafe { &*s }
    }
}

pub struct ProxyNotifier {
    cv: std::sync::Condvar,
    mutex: Mutex<()>,
    // multi notifiers single receiver model. use another flag to avoid waiting until timeout.
    flag: std::sync::atomic::AtomicBool,
}

impl ProxyNotifier {
    pub fn blocked_wait_for(&self, timeout: Duration) {
        // if flag from false to false, wait for notification.
        // if flag from true to false, do nothing.
        if !self.flag.swap(false, std::sync::atomic::Ordering::AcqRel) {
            {
                let lock = self.mutex.lock().unwrap();
                if !self.flag.load(std::sync::atomic::Ordering::Acquire) {
                    let _cv = self.cv.wait_timeout(lock, timeout);
                }
            }
            self.flag.store(false, std::sync::atomic::Ordering::Release);
        }
    }

    pub fn wake(&self) {
        // if flag from false -> true, then wake up.
        // if flag from true -> true, do nothing.
        if !self.flag.swap(true, std::sync::atomic::Ordering::AcqRel) {
            let _lock = self.mutex.lock().unwrap();
            self.cv.notify_one();
        }
    }

    pub fn new_raw() -> RawCppPtr {
        let notifier = Box::new(Self {
            cv: Default::default(),
            mutex: Mutex::new(()),
            flag: std::sync::atomic::AtomicBool::new(false),
        });

        RawCppPtr {
            ptr: Box::into_raw(notifier) as _,
            type_: RawCppPtrTypeImpl::WakerNotifier.into(),
        }
    }
}

extern "C" fn ffi_gc_special_raw_cpp_ptr(
    ptr: interfaces_ffi::RawVoidPtr,
    hint_len: u64,
    tp: interfaces_ffi::SpecialCppPtrType,
) {
    match tp {
        interfaces_ffi::SpecialCppPtrType::None => (),
        interfaces_ffi::SpecialCppPtrType::TupleOfRawCppPtr => unsafe {
            let p = Box::from_raw(std::slice::from_raw_parts_mut(
                ptr as *mut RawCppPtr,
                hint_len as usize,
            ));
            drop(p);
        },
        interfaces_ffi::SpecialCppPtrType::ArrayOfRawCppPtr => unsafe {
            let p = Box::from_raw(std::slice::from_raw_parts_mut(
                ptr as *mut RawVoidPtr,
                hint_len as usize,
            ));
            drop(p);
        },
    }
}

extern "C" fn ffi_gc_raw_cpp_ptr(
    ptr: interfaces_ffi::RawVoidPtr,
    tp: interfaces_ffi::RawCppPtrType,
) {
    match tp.into() {
        RawCppPtrTypeImpl::None => {}
        RawCppPtrTypeImpl::String => unsafe {
            drop(Box::<Vec<u8>>::from_raw(ptr as *mut _));
        },
        RawCppPtrTypeImpl::PreHandledSnapshotWithBlock => unsafe {
            drop(Box::<PrehandledSnapshot>::from_raw(ptr as *mut _));
        },
        RawCppPtrTypeImpl::WakerNotifier => unsafe {
            drop(Box::from_raw(ptr as *mut ProxyNotifier));
        },
        RawCppPtrTypeImpl::PSWriteBatch => unsafe {
            drop(Box::from_raw(ptr as *mut MockPSWriteBatch));
        },
        RawCppPtrTypeImpl::PSUniversalPage => unsafe {
            drop(Box::from_raw(ptr as *mut MockPSUniversalPage));
        },
        _ => todo!(),
    }
}

extern "C" fn ffi_gc_raw_cpp_ptr_carr(
    ptr: interfaces_ffi::RawVoidPtr,
    tp: interfaces_ffi::RawCppPtrType,
    len: u64,
) {
    match tp.into() {
        RawCppPtrTypeImpl::String => unsafe {
            let p = Box::from_raw(std::slice::from_raw_parts_mut(
                ptr as *mut RawVoidPtr,
                len as usize,
            ));
            for i in 0..len {
                let i = i as usize;
                if !p[i].is_null() {
                    ffi_gc_raw_cpp_ptr(p[i], RawCppPtrTypeImpl::String.into());
                }
            }
            drop(p);
        },
        RawCppPtrTypeImpl::PSPageAndCppStr => unsafe {
            let p = Box::from_raw(std::slice::from_raw_parts_mut(
                ptr as *mut PageAndCppStrWithView,
                len as usize,
            ));
            drop(p)
        },
        _ => todo!(),
    }
}

unsafe extern "C" fn ffi_atomic_update_proxy(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    arg2: *mut interfaces_ffi::RaftStoreProxyFFIHelper,
) {
    let store = into_engine_store_server_wrap(arg1);
    store.maybe_proxy_helper = Some(&mut *(arg2 as *mut RaftStoreProxyFFIHelper));
}

unsafe extern "C" fn ffi_handle_destroy(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    arg2: u64,
) {
    let store = into_engine_store_server_wrap(arg1);
    debug!("ffi_handle_destroy {}", arg2);
    (*store.engine_store_server).kvstore.remove(&arg2);
}

type MockRaftProxyHelper = RaftStoreProxyFFIHelper;

#[derive(Debug)]
pub struct SSTReader<'a> {
    proxy_helper: &'a MockRaftProxyHelper,
    inner: interfaces_ffi::SSTReaderPtr,
    type_: interfaces_ffi::ColumnFamilyType,
}

impl<'a> Drop for SSTReader<'a> {
    fn drop(&mut self) {
        unsafe {
            (self.proxy_helper.sst_reader_interfaces.fn_gc.into_inner())(
                self.inner.clone(),
                self.type_,
            );
        }
    }
}

impl<'a> SSTReader<'a> {
    pub unsafe fn new(
        proxy_helper: &'a MockRaftProxyHelper,
        view: &'a interfaces_ffi::SSTView,
    ) -> Self {
        SSTReader {
            proxy_helper,
            inner: (proxy_helper
                .sst_reader_interfaces
                .fn_get_sst_reader
                .into_inner())(view.clone(), proxy_helper.proxy_ptr.clone()),
            type_: view.type_,
        }
    }

    pub unsafe fn remained(&mut self) -> bool {
        (self
            .proxy_helper
            .sst_reader_interfaces
            .fn_remained
            .into_inner())(self.inner.clone(), self.type_)
            != 0
    }

    pub unsafe fn key(&mut self) -> interfaces_ffi::BaseBuffView {
        (self.proxy_helper.sst_reader_interfaces.fn_key.into_inner())(
            self.inner.clone(),
            self.type_,
        )
    }

    pub unsafe fn value(&mut self) -> interfaces_ffi::BaseBuffView {
        (self
            .proxy_helper
            .sst_reader_interfaces
            .fn_value
            .into_inner())(self.inner.clone(), self.type_)
    }

    pub unsafe fn next(&mut self) {
        (self.proxy_helper.sst_reader_interfaces.fn_next.into_inner())(
            self.inner.clone(),
            self.type_,
        )
    }
}

struct PrehandledSnapshot {
    pub region: std::option::Option<Region>,
}

unsafe extern "C" fn ffi_pre_handle_snapshot(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    region_buff: interfaces_ffi::BaseBuffView,
    peer_id: u64,
    snaps: interfaces_ffi::SSTViewVec,
    index: u64,
    term: u64,
) -> interfaces_ffi::RawCppPtr {
    let store = into_engine_store_server_wrap(arg1);
    let proxy_helper = &mut *(store.maybe_proxy_helper.unwrap());
    let _kvstore = &mut (*store.engine_store_server).kvstore;
    let node_id = (*store.engine_store_server).id;

    let mut region_meta = kvproto::metapb::Region::default();
    assert_ne!(region_buff.data, std::ptr::null());
    assert_ne!(region_buff.len, 0);
    region_meta
        .merge_from_bytes(region_buff.to_slice())
        .unwrap();

    let mut region = Box::new(Region::new(region_meta));
    debug!(
        "pre handle snaps";
        "peer_id" => peer_id,
        "store_id" => node_id,
        "index" => index,
        "term" => term,
        "region" => ?region.region,
        "snap len" => snaps.len,
    );

    (*store.engine_store_server).mutate_region_states(
        region.region.get_id(),
        |e: &mut RegionStats| {
            e.pre_handle_count.fetch_add(1, Ordering::SeqCst);
        },
    );

    for i in 0..snaps.len {
        let snapshot = snaps.views.add(i as usize);
        let view = &*(snapshot as *mut interfaces_ffi::SSTView);
        let mut sst_reader = SSTReader::new(proxy_helper, view);

        while sst_reader.remained() {
            let key = sst_reader.key();
            let value = sst_reader.value();

            let cf_index = (*snapshot).type_ as u8;
            write_kv_in_mem(
                region.as_mut(),
                cf_index as usize,
                key.to_slice(),
                value.to_slice(),
            );

            sst_reader.next();
        }
    }
    {
        region.set_applied(index, term);
        region.apply_state.mut_truncated_state().set_index(index);
        region.apply_state.mut_truncated_state().set_term(term);
    }
    interfaces_ffi::RawCppPtr {
        ptr: Box::into_raw(Box::new(PrehandledSnapshot {
            region: Some(*region),
        })) as *const Region as interfaces_ffi::RawVoidPtr,
        type_: RawCppPtrTypeImpl::PreHandledSnapshotWithBlock.into(),
    }
}

unsafe extern "C" fn ffi_handle_safe_ts_update(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    _region_id: u64,
    self_safe_ts: u64,
    leader_safe_ts: u64,
) {
    let store = into_engine_store_server_wrap(arg1);
    let cluster = store.cluster_ptr as *const mock_cluster::Cluster<ServerCluster>;
    assert_eq!(self_safe_ts, (*cluster).test_data.expected_self_safe_ts);
    assert_eq!(leader_safe_ts, (*cluster).test_data.expected_leader_safe_ts);
}

unsafe extern "C" fn ffi_apply_pre_handled_snapshot(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    arg2: interfaces_ffi::RawVoidPtr,
    _arg3: interfaces_ffi::RawCppPtrType,
) {
    let store = into_engine_store_server_wrap(arg1);
    let region_meta = &mut *(arg2 as *mut PrehandledSnapshot);
    let node_id = (*store.engine_store_server).id;

    let region_id = region_meta.region.as_ref().unwrap().region.id;

    let _ = &(*store.engine_store_server)
        .kvstore
        .insert(region_id, Box::new(region_meta.region.take().unwrap()));

    let region = (*store.engine_store_server)
        .kvstore
        .get_mut(&region_id)
        .unwrap();

    debug!(
        "apply prehandled snap";
        "store_id" => node_id,
        "region" => ?region.region,
    );
    write_to_db_data(
        &mut (*store.engine_store_server),
        region,
        String::from("prehandle-snap"),
    );
}

unsafe extern "C" fn ffi_handle_ingest_sst(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    snaps: interfaces_ffi::SSTViewVec,
    header: interfaces_ffi::RaftCmdHeader,
) -> interfaces_ffi::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    let node_id = (*store.engine_store_server).id;
    let proxy_helper = &mut *(store.maybe_proxy_helper.unwrap());

    let region_id = header.region_id;
    let kvstore = &mut (*store.engine_store_server).kvstore;
    let _kv = &mut (*store.engine_store_server).engines.as_mut().unwrap().kv;

    match kvstore.entry(region_id) {
        std::collections::hash_map::Entry::Occupied(_o) => {}
        std::collections::hash_map::Entry::Vacant(v) => {
            // When we remove hacked code in handle_raft_entry_normal during migration,
            // some tests in handle_raft_entry_normal may fail, since it can observe a empty
            // cmd, thus creating region.
            warn!(
                "region {} not found when ingest, create for {}",
                region_id, node_id
            );
            let _ = v.insert(Default::default());
        }
    }
    let region = kvstore.get_mut(&region_id).unwrap();

    let index = header.index;
    let term = header.term;
    debug!("handle ingest sst";
        "header" => ?header,
        "region_id" => region_id,
        "snap len" => snaps.len,
    );

    for i in 0..snaps.len {
        let snapshot = snaps.views.add(i as usize);
        // let _path = std::str::from_utf8_unchecked((*snapshot).path.to_slice());
        let mut sst_reader =
            SSTReader::new(proxy_helper, &*(snapshot as *mut interfaces_ffi::SSTView));
        while sst_reader.remained() {
            let key = sst_reader.key();
            let value = sst_reader.value();
            let cf_index = (*snapshot).type_ as usize;
            write_kv_in_mem(region.as_mut(), cf_index, key.to_slice(), value.to_slice());
            sst_reader.next();
        }
    }

    {
        region.set_applied(header.index, header.term);
        region.apply_state.mut_truncated_state().set_index(index);
        region.apply_state.mut_truncated_state().set_term(term);
    }

    fail::fail_point!("on_handle_ingest_sst_return", |_e| {
        interfaces_ffi::EngineStoreApplyRes::None
    });
    write_to_db_data(
        &mut (*store.engine_store_server),
        region,
        String::from("ingest-sst"),
    );
    interfaces_ffi::EngineStoreApplyRes::Persist
}

unsafe extern "C" fn ffi_handle_compute_store_stats(
    _arg1: *mut interfaces_ffi::EngineStoreServerWrap,
) -> interfaces_ffi::StoreStats {
    interfaces_ffi::StoreStats {
        fs_stats: interfaces_ffi::FsStats {
            capacity_size: 444444,
            used_size: 111111,
            avail_size: 333333,
            ok: 1,
        },
        engine_bytes_written: 0,
        engine_keys_written: 0,
        engine_bytes_read: 0,
        engine_keys_read: 0,
    }
}

pub fn copy_meta_from<EK: engine_traits::KvEngine, ER: RaftEngine + engine_traits::Peekable>(
    source_engines: &Engines<EK, ER>,
    target_engines: &Engines<EK, ER>,
    source: &Region,
    target: &mut Region,
    new_region_meta: kvproto::metapb::Region,
    copy_region_state: bool,
    copy_apply_state: bool,
    copy_raft_state: bool,
) -> raftstore::Result<()> {
    let region_id = source.region.get_id();

    let mut wb = target_engines.kv.write_batch();

    // Can't copy this key, otherwise will cause a bootstrap.
    // box_try!(wb.put_msg(keys::PREPARE_BOOTSTRAP_KEY, &source.region));

    // region local state
    if copy_region_state {
        let mut state = RegionLocalState::default();
        state.set_region(new_region_meta);
        box_try!(wb.put_msg_cf(CF_RAFT, &keys::region_state_key(region_id), &state));
    }

    // apply state
    if copy_apply_state {
        let apply_state: RaftApplyState =
            match general_get_apply_state(&source_engines.kv, region_id) {
                Some(x) => x,
                None => return Err(box_err!("bad RaftApplyState")),
            };
        wb.put_msg_cf(CF_RAFT, &keys::apply_state_key(region_id), &apply_state)?;
        target.apply_state = apply_state.clone();
        target.applied_term = source.applied_term;
    }

    wb.write()?;
    target_engines.sync_kv()?;

    let mut raft_wb = target_engines.raft.log_batch(1024);
    // raft state
    if copy_raft_state {
        let raft_state = match get_raft_local_state(&source_engines.raft, region_id) {
            Some(x) => x,
            None => return Err(box_err!("bad RaftLocalState")),
        };
        raft_wb.put_raft_state(region_id, &raft_state)?;
    };

    box_try!(target_engines.raft.consume(&mut raft_wb, true));
    Ok(())
}

pub fn copy_data_from(
    source_engines: &Engines<impl KvEngine, impl RaftEngine + engine_traits::Peekable>,
    target_engines: &Engines<impl KvEngine, impl RaftEngine>,
    source: &Region,
    target: &mut Region,
) -> raftstore::Result<()> {
    let region_id = source.region.get_id();

    // kv data in memory
    for cf in 0..3 {
        for (k, v) in &source.data[cf] {
            // debug!("copy_data_from region {} {:?} {:?}", region_id, k, v);
            write_kv_in_mem(target, cf, k.as_slice(), v.as_slice());
        }
    }

    // raft log
    let mut raft_wb = target_engines.raft.log_batch(1024);
    let mut entries: Vec<raft::eraftpb::Entry> = Default::default();
    source_engines
        .raft
        .get_all_entries_to(region_id, &mut entries)
        .unwrap();
    debug!("copy raft log {:?}", entries);

    raft_wb.append(region_id, None, entries)?;
    box_try!(target_engines.raft.consume(&mut raft_wb, true));
    Ok(())
}

#[allow(clippy::redundant_closure_call)]
unsafe extern "C" fn ffi_fast_add_peer(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    region_id: u64,
    new_peer_id: u64,
) -> interfaces_ffi::FastAddPeerRes {
    let store = into_engine_store_server_wrap(arg1);
    let cluster = &*(store.cluster_ptr as *const mock_cluster::Cluster<NodeCluster>);
    let store_id = (*store.engine_store_server).id;
    (*store.engine_store_server).mutate_region_states(region_id, |e: &mut RegionStats| {
        e.fast_add_peer_count.fetch_add(1, Ordering::SeqCst);
    });

    let failed_add_peer_res =
        |status: interfaces_ffi::FastAddPeerStatus| interfaces_ffi::FastAddPeerRes {
            status,
            apply_state: create_cpp_str(None),
            region: create_cpp_str(None),
        };
    let from_store = (|| {
        fail::fail_point!("fap_mock_add_peer_from_id", |t| {
            let t = t.unwrap().parse::<u64>().unwrap();
            t
        });
        1
    })();
    let block_wait: bool = (|| {
        fail::fail_point!("fap_mock_block_wait", |t| {
            let t = t.unwrap().parse::<u64>().unwrap();
            t
        });
        0
    })() != 0;
    let fail_after_write: bool = (|| {
        fail::fail_point!("fap_mock_fail_after_write", |t| {
            let t = t.unwrap().parse::<u64>().unwrap();
            t
        });
        0
    })() != 0;
    debug!("recover from remote peer: enter from {} to {}", from_store, store_id; "region_id" => region_id);

    for retry in 0..300 {
        let mut ret: Option<interfaces_ffi::FastAddPeerRes> = None;
        if retry > 0 {
            std::thread::sleep(std::time::Duration::from_millis(30));
        }
        cluster.access_ffi_helpers(&mut |guard: &mut HashMap<u64, crate::mock_cluster::FFIHelperSet>| {
            debug!("recover from remote peer: preparing from {} to {}, persist and check source", from_store, store_id; "region_id" => region_id);
            let source_server = match guard.get_mut(&from_store) {
                Some(s) => &mut s.engine_store_server,
                None => {
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::NoSuitable));
                    return;
                }
            };
            let source_engines = match source_server.engines.clone() {
                Some(s) => s,
                None => {
                    error!("recover from remote peer: failed get source engine"; "region_id" => region_id);
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::BadData));
                    return
                }
            };
            // TODO We must ask the remote peer to persist before get a snapshot.
            let source_region = match source_server.kvstore.get(&region_id) {
                Some(s) => s,
                None => {
                    error!("recover from remote peer: failed read source region info"; "region_id" => region_id);
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::BadData));
                    return;
                }
            };
            let region_local_state: RegionLocalState = match general_get_region_local_state(
                &source_engines.kv,
                region_id,
            ) {
                Some(x) => x,
                None => {
                    debug!("recover from remote peer: preparing from {} to {}:{}, not region state", from_store, store_id, new_peer_id; "region_id" => region_id);
                    // We don't return BadData here, since the data may not be persisted.
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::WaitForData));
                    return;
                }
            };
            let new_region_meta = region_local_state.get_region();
            let peer_state = region_local_state.get_state();
            // Validation
            match peer_state {
                PeerState::Tombstone | PeerState::Applying => {
                    // Note in real implementation, we will avoid selecting this peer.
                    error!("recover from remote peer: preparing from {} to {}:{}, error peer state {:?}", from_store, store_id, new_peer_id, peer_state; "region_id" => region_id);
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::BadData));
                    return;
                }
                _ => {
                    info!("recover from remote peer: preparing from {} to {}:{}, ok peer state {:?}", from_store, store_id, new_peer_id, peer_state; "region_id" => region_id);
                }
            };
            if !engine_store_ffi::core::validate_remote_peer_region(
                new_region_meta,
                store_id,
                new_peer_id,
            ) {
                debug!("recover from remote peer: preparing from {} to {}, not applied conf change {}", from_store, store_id, new_peer_id; "region_id" => region_id);
                ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::WaitForData));
                return;
            }
            // TODO check commit_index and applied_index here
            debug!("recover from remote peer: preparing from {} to {}, check target", from_store, store_id; "region_id" => region_id);
            let new_region = make_new_region(
                Some(new_region_meta.clone()),
                Some((*store.engine_store_server).id),
            );
            (*store.engine_store_server)
                .kvstore
                .insert(region_id, Box::new(new_region));
            let target_engines = match (*store.engine_store_server).engines.clone() {
                Some(s) => s,
                None => {
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::OtherError));
                    return;
                }
            };
            let target_region = match (*store.engine_store_server).kvstore.get_mut(&region_id) {
                Some(s) => s,
                None => {
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::BadData));
                    return;
                }
            };
            debug!("recover from remote peer: meta from {} to {}", from_store, store_id; "region_id" => region_id);
            // Must first dump meta then data, otherwise data may lag behind.
            // We can see a raft log hole at applied_index otherwise.
            let apply_state: RaftApplyState = match general_get_apply_state(
                &source_engines.kv,
                region_id,
            ) {
                Some(x) => x,
                None => {
                    error!("recover from remote peer: failed read apply state"; "region_id" => region_id);
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::BadData));
                    return;
                }
            };
            debug!("recover from remote peer: begin data from {} to {}", from_store, store_id; 
                "region_id" => region_id,
                "apply_state" => ?apply_state,
            );
            // TODO In TiFlash we should take care of write batch size
            if let Err(e) = copy_data_from(
                &source_engines,
                &target_engines,
                &source_region,
                target_region,
            ) {
                error!("recover from remote peer: inject error {:?}", e; "region_id" => region_id);
                ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::FailedInject));
                return;
            }
            if fail_after_write {
                let mut raft_wb = target_engines.raft.log_batch(1024);
                let mut entries: Vec<raft::eraftpb::Entry> = Default::default();
                target_engines
                    .raft
                    .get_all_entries_to(region_id, &mut entries)
                    .unwrap();
                let l = entries.len();
                // Manually delete one raft log
                // let from = entries.get(l - 2).unwrap().get_index();
                let from = 7;
                let to = entries.get(l - 1).unwrap().get_index() + 1;
                debug!("recover from remote peer: simulate error from {} to {}", from_store, store_id;
                    "region_id" => region_id,
                    "from" => from,
                    "to" => to,
                );
                // raft_wb.cut_logs(region_id, from, to);
                target_engines.raft.gc(region_id, from, to, &mut raft_wb).unwrap();
                target_engines.raft.consume(&mut raft_wb, true).unwrap();
            }
            let apply_state_bytes = apply_state.write_to_bytes().unwrap();
            let region_bytes = region_local_state.get_region().write_to_bytes().unwrap();
            let apply_state_ptr = create_cpp_str(Some(apply_state_bytes));
            let region_ptr = create_cpp_str(Some(region_bytes));
            // Check if we have commit_index.
            debug!("recover from remote peer: ok from {} to {}", from_store, store_id; "region_id" => region_id);
            ret = Some(interfaces_ffi::FastAddPeerRes {
                status: interfaces_ffi::FastAddPeerStatus::Ok,
                apply_state: apply_state_ptr,
                region: region_ptr,
            });
        });
        if let Some(r) = ret {
            match r.status {
                interfaces_ffi::FastAddPeerStatus::WaitForData => {
                    if block_wait {
                        continue;
                    } else {
                        return r;
                    }
                }
                _ => return r,
            }
        }
    }
    error!("recover from remote peer: failed after retry"; "region_id" => region_id);
    failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::BadData)
}
