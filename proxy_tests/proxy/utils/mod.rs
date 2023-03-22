// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub use std::{
    collections::HashMap,
    io::Write,
    ops::DerefMut,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{atomic::Ordering, mpsc, Arc, RwLock},
};

pub use collections::HashSet;
pub use engine_store_ffi::ffi::{interfaces_ffi::KVGetStatus, RaftStoreProxyFFI};
pub use engine_traits::{
    MiscExt, Mutable, Peekable, RaftEngineDebug, RaftLogBatch, WriteBatch, CF_DEFAULT, CF_LOCK,
    CF_RAFT, CF_WRITE,
};
pub use kvproto::{
    import_sstpb::SstMeta,
    metapb,
    metapb::RegionEpoch,
    raft_cmdpb::{AdminCmdType, AdminRequest, CmdType, Request},
    raft_serverpb::{PeerState, RaftApplyState, RaftLocalState, RegionLocalState, StoreIdent},
};
pub use mock_engine_store::{
    general_get_apply_state, general_get_raft_local_state, general_get_region_local_state,
    make_new_region,
    mock_cluster::{
        config::MixedClusterConfig,
        test_utils::*,
        v1::{
            node::NodeCluster,
            transport_simulate::{
                CloneFilterFactory, CollectSnapshotFilter, Direction, RegionPacketFilter,
            },
            Cluster, Simulator,
        },
        ClusterExt, FFIHelperSet, ProxyConfig,
    },
    write_kv_in_mem, RegionStats,
};
pub use pd_client::PdClient;
pub use raft::eraftpb::{ConfChangeType, MessageType};
pub use raftstore::coprocessor::ConsistencyCheckMethod;
pub use test_pd_client::TestPdClient;
pub use tikv_util::{
    box_err, box_try,
    config::{ReadableDuration, ReadableSize},
    store::{find_peer, find_peer_by_id},
    time::Duration,
    HandyRwLock,
};

pub fn new_compute_hash_request() -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::ComputeHash);
    req.mut_compute_hash()
        .set_context(vec![ConsistencyCheckMethod::Raw as u8]);
    req
}

pub fn new_verify_hash_request(hash: Vec<u8>, index: u64) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::VerifyHash);
    req.mut_verify_hash().set_hash(hash);
    req.mut_verify_hash().set_index(index);
    req
}

pub fn new_mock_cluster(id: u64, count: usize) -> (Cluster<NodeCluster>, Arc<TestPdClient>) {
    tikv_util::set_panic_hook(true, "./");
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = Cluster::new(id, count, sim, pd_client.clone(), ProxyConfig::default());
    // Compat new proxy
    cluster.cfg.mock_cfg.proxy_compat = true;

    #[cfg(feature = "enable-pagestorage")]
    {
        cluster.cfg.proxy_cfg.engine_store.enable_unips = true;
    }
    #[cfg(not(feature = "enable-pagestorage"))]
    {
        cluster.cfg.proxy_cfg.engine_store.enable_unips = false;
    }
    (cluster, pd_client)
}

pub fn new_mock_cluster_snap(id: u64, count: usize) -> (Cluster<NodeCluster>, Arc<TestPdClient>) {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut proxy_config = ProxyConfig::default();
    proxy_config.raft_store.snap_handle_pool_size = 2;
    let mut cluster = Cluster::new(id, count, sim, pd_client.clone(), proxy_config);
    // Compat new proxy
    cluster.cfg.mock_cfg.proxy_compat = true;

    #[cfg(feature = "enable-pagestorage")]
    {
        cluster.cfg.proxy_cfg.engine_store.enable_unips = true;
    }
    #[cfg(not(feature = "enable-pagestorage"))]
    {
        cluster.cfg.proxy_cfg.engine_store.enable_unips = false;
    }
    (cluster, pd_client)
}

pub fn must_put_and_check_key_with_generator<F: Fn(u64) -> (String, String)>(
    cluster: &mut impl MixedCluster,
    gen: F,
    from: u64,
    to: u64,
    in_mem: Option<bool>,
    in_disk: Option<bool>,
    engines: Option<Vec<u64>>,
) {
    for i in from..to {
        let (k, v) = gen(i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }
    for i in from..to {
        let (k, v) = gen(i);
        check_key(
            cluster,
            k.as_bytes(),
            v.as_bytes(),
            in_mem,
            in_disk,
            engines.clone(),
        );
    }
}

pub fn must_put_and_check_key(
    cluster: &mut impl MixedCluster,
    from: u64,
    to: u64,
    in_mem: Option<bool>,
    in_disk: Option<bool>,
    engines: Option<Vec<u64>>,
) {
    must_put_and_check_key_with_generator(
        cluster,
        |i: u64| {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            (k, v)
        },
        from,
        to,
        in_mem,
        in_disk,
        engines.clone(),
    );
}

pub fn check_key(
    cluster: &impl MixedCluster,
    k: &[u8],
    v: &[u8],
    in_mem: Option<bool>,
    in_disk: Option<bool>,
    engines: Option<Vec<u64>>,
) {
    let region_id = cluster.get_region(k).get_id();
    let engine_keys = {
        match engines {
            Some(e) => e.to_vec(),
            None => cluster.get_all_store_ids(),
        }
    };
    for id in engine_keys {
        let engine = cluster.get_engine(id);
        match in_disk {
            Some(b) => {
                if b {
                    cluster.must_get(id, k, Some(v));
                } else {
                    cluster.must_get(id, k, None);
                }
            }
            None => (),
        };
        match in_mem {
            Some(b) => {
                if b {
                    must_get_mem(cluster.get_cluster_ext(), id, region_id, k, Some(v));
                } else {
                    must_get_mem(cluster.get_cluster_ext(), id, region_id, k, None);
                }
            }
            None => (),
        };
    }
}

pub fn disable_auto_gen_compact_log(cluster: &mut Cluster<NodeCluster>) {
    // Disable AUTO generated compact log.
    // This will not totally disable, so we use some failpoints later.
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(1000);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100000);
    cluster.cfg.raft_store.snap_apply_batch_size = ReadableSize(500000);
    cluster.cfg.raft_store.raft_log_gc_threshold = 10000;
}

pub fn force_compact_log(
    cluster: &mut impl MixedCluster,
    key: &[u8],
    use_nodes: Option<Vec<u64>>,
) -> u64 {
    let region = cluster.get_region(key);
    let region_id = region.get_id();
    let prev_states = maybe_collect_states(cluster.get_cluster_ext(), region_id, None);

    let (compact_index, compact_term) = get_valid_compact_index_by(&prev_states, use_nodes);
    let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
    let _ = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    return compact_index;
}

pub fn stop_tiflash_node(cluster: &mut impl MixedCluster, node_id: u64) {
    info!("stop node {}", node_id);
    {
        cluster.stop_node(node_id);
    }
    {
        iter_ffi_helpers(
            cluster,
            Some(vec![node_id]),
            &mut |_, ffi: &mut FFIHelperSet| {
                let server = &mut ffi.engine_store_server;
                server.stop();
            },
        );
    }
}

pub fn restart_tiflash_node(cluster: &mut Cluster<NodeCluster>, node_id: u64) {
    info!("restored node {}", node_id);
    {
        iter_ffi_helpers(
            cluster,
            Some(vec![node_id]),
            &mut |_, ffi: &mut FFIHelperSet| {
                let server = &mut ffi.engine_store_server;
                server.restore();
            },
        );
    }
    cluster.run_node(node_id).unwrap();
}

pub fn must_not_merged(pd_client: Arc<TestPdClient>, from: u64, duration: Duration) {
    let timer = tikv_util::time::Instant::now();
    loop {
        let region = futures::executor::block_on(pd_client.get_region_by_id(from)).unwrap();
        if let Some(_) = region {
            if timer.saturating_elapsed() > duration {
                return;
            }
        } else {
            panic!("region {} is merged.", from);
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}
