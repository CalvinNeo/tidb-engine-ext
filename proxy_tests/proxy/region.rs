// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::iter::FromIterator;

use collections::HashSet;
use raft::eraftpb::Entry;

use crate::proxy::*;

#[test]
fn test_handle_destroy() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

    disable_auto_gen_compact_log(&mut cluster);

    // Disable default max peer count check.
    pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");
    let eng_ids = cluster
        .engines
        .iter()
        .map(|e| e.0.to_owned())
        .collect::<Vec<_>>();

    let region = cluster.get_region(b"k1");
    let region_id = region.get_id();
    let peer_1 = find_peer(&region, eng_ids[0]).cloned().unwrap();
    let peer_2 = find_peer(&region, eng_ids[1]).cloned().unwrap();
    cluster.must_transfer_leader(region_id, peer_1);

    iter_ffi_helpers(
        &cluster,
        Some(vec![eng_ids[1]]),
        &mut |_, _, ffi: &mut FFIHelperSet| {
            let server = &ffi.engine_store_server;
            assert!(server.kvstore.contains_key(&region_id));
        },
    );

    pd_client.must_remove_peer(region_id, peer_2);

    check_key(
        &cluster,
        b"k1",
        b"v2",
        Some(false),
        None,
        Some(vec![eng_ids[1]]),
    );

    std::thread::sleep(std::time::Duration::from_millis(100));
    // Region removed in server.
    iter_ffi_helpers(
        &cluster,
        Some(vec![eng_ids[1]]),
        &mut |_, _, ffi: &mut FFIHelperSet| {
            let server = &ffi.engine_store_server;
            assert!(!server.kvstore.contains_key(&region_id));
        },
    );

    cluster.shutdown();
}

#[test]
fn test_get_region_local_state() {
    let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

    cluster.run();

    let k = b"k1";
    let v = b"v1";
    cluster.must_put(k, v);
    check_key(&cluster, k, v, Some(true), None, None);
    let region_id = cluster.get_region(k).get_id();

    // Get RegionLocalState through ffi
    unsafe {
        iter_ffi_helpers(
            &cluster,
            None,
            &mut |_id: u64, _, ffi_set: &mut FFIHelperSet| {
                let f = ffi_set.proxy_helper.fn_get_region_local_state.unwrap();
                let mut state = kvproto::raft_serverpb::RegionLocalState::default();
                let mut error_msg = new_mock_engine_store::RawCppStringPtrGuard::default();

                assert_eq!(
                    f(
                        ffi_set.proxy_helper.proxy_ptr,
                        region_id,
                        &mut state as *mut _ as _,
                        error_msg.as_mut(),
                    ),
                    KVGetStatus::Ok
                );
                assert!(state.has_region());
                assert_eq!(state.get_state(), kvproto::raft_serverpb::PeerState::Normal);
                assert!(error_msg.as_ref().is_null());

                let mut state = kvproto::raft_serverpb::RegionLocalState::default();
                assert_eq!(
                    f(
                        ffi_set.proxy_helper.proxy_ptr,
                        0, // not exist
                        &mut state as *mut _ as _,
                        error_msg.as_mut(),
                    ),
                    KVGetStatus::NotFound
                );
                assert!(!state.has_region());
                assert!(error_msg.as_ref().is_null());

                ffi_set
                    .proxy
                    .get_value_cf("none_cf", "123".as_bytes(), |value| {
                        let msg = value.unwrap_err();
                        assert_eq!(msg, "Storage Engine Status { code: IoError, sub_code: None, sev: NoError, state: \"cf none_cf not found\" }");
                    });
                ffi_set
                    .proxy
                    .get_value_cf("raft", "123".as_bytes(), |value| {
                        let res = value.unwrap();
                        assert!(res.is_none());
                    });

                // If we have no kv engine.
                ffi_set.proxy.set_kv_engine(None);
                let res = ffi_set.proxy_helper.fn_get_region_local_state.unwrap()(
                    ffi_set.proxy_helper.proxy_ptr,
                    region_id,
                    &mut state as *mut _ as _,
                    error_msg.as_mut(),
                );
                assert_eq!(res, KVGetStatus::Error);
                assert!(!error_msg.as_ref().is_null());
                assert_eq!(
                    error_msg.as_str(),
                    "KV engine is not initialized".as_bytes()
                );
            },
        );
    }

    cluster.shutdown();
}

/// This test is very important.
/// If make sure we can add learner peer for a store which is not started
/// actually.
#[test]
fn test_add_absent_learner_peer_by_simple() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    disable_auto_gen_compact_log(&mut cluster);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let _ = cluster.run();
    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));

    pd_client.must_add_peer(1, new_learner_peer(4, 4));

    cluster.must_put(b"k3", b"v3");
    check_key(&cluster, b"k3", b"v3", Some(true), None, None);
    let new_states = collect_all_states(&cluster, 1);
    assert_eq!(new_states.len(), 3);
    for i in new_states.keys() {
        assert_eq!(
            new_states
                .get(i)
                .unwrap()
                .in_disk_region_state
                .get_region()
                .get_peers()
                .len(),
            3 + 1 // Learner
        );
    }

    cluster.shutdown();
}

/// This test is very important.
/// If make sure we can add learner peer for a store which is not started
/// actually.
#[test]
fn test_add_absent_learner_peer_by_joint() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    disable_auto_gen_compact_log(&mut cluster);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let _ = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));

    pd_client.must_joint_confchange(
        1,
        vec![
            (ConfChangeType::AddNode, new_peer(2, 2)),
            (ConfChangeType::AddNode, new_peer(3, 3)),
            (ConfChangeType::AddLearnerNode, new_learner_peer(4, 4)),
            (ConfChangeType::AddLearnerNode, new_learner_peer(5, 5)),
        ],
    );
    assert!(pd_client.is_in_joint(1));
    pd_client.must_leave_joint(1);

    cluster.must_put(b"k3", b"v3");
    check_key(&cluster, b"k3", b"v3", Some(true), None, None);
    let new_states = collect_all_states(&cluster, 1);
    assert_eq!(new_states.len(), 3);
    for i in new_states.keys() {
        assert_eq!(
            new_states
                .get(i)
                .unwrap()
                .in_disk_region_state
                .get_region()
                .get_peers()
                .len(),
            1 + 2 /* AddPeer */ + 2 // Learner
        );
    }

    cluster.shutdown();
}

use engine_traits::{Engines, KvEngine, RaftEngine};
use raftstore::store::{write_initial_apply_state, write_initial_raft_state, RAFT_INIT_LOG_INDEX};

pub fn prepare_bootstrap_cluster_with(
    engines: &Engines<impl KvEngine, impl RaftEngine>,
    region: &metapb::Region,
) -> raftstore::Result<()> {
    let mut state = RegionLocalState::default();
    state.set_region(region.clone());

    let mut wb = engines.kv.write_batch();
    // box_try!(wb.put_msg(keys::PREPARE_BOOTSTRAP_KEY, region));
    box_try!(wb.put_msg_cf(CF_RAFT, &keys::region_state_key(region.get_id()), &state));
    write_initial_apply_state(&mut wb, region.get_id())?;
    wb.write()?;
    engines.sync_kv()?;

    let mut raft_wb = engines.raft.log_batch(1024);
    write_initial_raft_state(&mut raft_wb, region.get_id())?;
    box_try!(engines.raft.consume(&mut raft_wb, true));
    Ok(())
}

fn new_later_add_learner_cluster<F: Fn(&mut Cluster<NodeCluster>)>(
    initer: F,
    learner: Vec<u64>,
) -> (Cluster<NodeCluster>, Arc<TestPdClient>) {
    let (mut cluster, pd_client) = new_mock_cluster(0, 5);
    // Make sure we persist before generate snapshot.
    fail::cfg("on_pre_persist_with_finish", "return").unwrap();

    cluster.cfg.proxy_compat = false;
    disable_auto_gen_compact_log(&mut cluster);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let _ = cluster.run_conf_change_no_start();
    let _ = cluster.start_with(HashSet::from_iter(
        vec![3, 4].into_iter().map(|x| x as usize),
    ));
    initer(&mut cluster);

    let mut peers = vec![
        (ConfChangeType::AddNode, new_peer(2, 2)),
        (ConfChangeType::AddNode, new_peer(3, 3)),
    ];
    let mut learner_peers: Vec<(ConfChangeType, kvproto::metapb::Peer)> = learner
        .iter()
        .map(|i| (ConfChangeType::AddLearnerNode, new_learner_peer(*i, *i)))
        .collect();
    peers.append(&mut learner_peers);
    pd_client.must_joint_confchange(1, peers);
    assert!(pd_client.is_in_joint(1));
    pd_client.must_leave_joint(1);

    (cluster, pd_client)
}

fn later_bootstrap_learner_peer(
    cluster: &mut Cluster<NodeCluster>,
    peers: Vec<u64>,
    already_learner_count: usize,
) {
    // Check if the voters has correct learner peer.
    let new_states = maybe_collect_states(&cluster, 1, Some(vec![1, 2, 3]));
    assert_eq!(new_states.len(), 3);
    for i in new_states.keys() {
        assert_eq!(
            new_states
                .get(i)
                .unwrap()
                .in_disk_region_state
                .get_region()
                .get_peers()
                .len(),
            1 + 2 /* AddPeer */ + already_learner_count // Learner
        );
    }

    let region = new_states
        .get(&1)
        .unwrap()
        .in_disk_region_state
        .get_region();
    // Explicitly bootstrap region.
    for id in peers {
        let engines = cluster.get_engines(id);
        assert!(prepare_bootstrap_cluster_with(engines, region).is_ok());
    }
}

#[test]
fn test_add_delayed_started_learner_by_joint() {
    let (mut cluster, pd_client) = new_later_add_learner_cluster(
        |c: &mut Cluster<NodeCluster>| {
            c.must_put(b"k1", b"v1");
            check_key(c, b"k1", b"v1", Some(true), None, Some(vec![1]));
        },
        vec![4, 5],
    );

    cluster.must_put(b"k2", b"v2");
    check_key(
        &cluster,
        b"k2",
        b"v2",
        Some(true),
        None,
        Some(vec![1, 2, 3]),
    );

    later_bootstrap_learner_peer(&mut cluster, vec![4, 5], 2);
    cluster
        .start_with(HashSet::from_iter(
            vec![0, 1, 2].into_iter().map(|x| x as usize),
        ))
        .unwrap();

    cluster.must_put(b"k4", b"v4");
    check_key(&cluster, b"k4", b"v4", Some(true), None, None);

    let new_states = maybe_collect_states(&cluster, 1, None);
    assert_eq!(new_states.len(), 5);
    for i in new_states.keys() {
        assert_eq!(
            new_states
                .get(i)
                .unwrap()
                .in_disk_region_state
                .get_region()
                .get_peers()
                .len(),
            1 + 2 /* AddPeer */ + 2 // Learner
        );
    }

    fail::remove("on_pre_persist_with_finish");
    cluster.shutdown();
}

pub fn copy_meta_from(
    source_engines: &Engines<
        impl KvEngine,
        impl RaftEngine + engine_traits::Peekable + RaftEngineDebug,
    >,
    target_engines: &Engines<impl KvEngine, impl RaftEngine>,
    source: &Box<new_mock_engine_store::Region>,
    target: &mut Box<new_mock_engine_store::Region>,
    new_region_meta: kvproto::metapb::Region,
) -> raftstore::Result<()> {
    let region_id = source.region.get_id();

    let mut wb = target_engines.kv.write_batch();
    let mut raft_wb = target_engines.raft.log_batch(1024);

    // box_try!(wb.put_msg(keys::PREPARE_BOOTSTRAP_KEY, &source.region));

    // region local state
    let mut state = RegionLocalState::default();
    state.set_region(new_region_meta);
    box_try!(wb.put_msg_cf(CF_RAFT, &keys::region_state_key(region_id), &state));

    // apply state
    {
        let key = keys::apply_state_key(region_id);
        let apply_state: RaftApplyState = source_engines
            .kv
            .get_msg_cf(CF_RAFT, &key)
            .unwrap()
            .unwrap();
        wb.put_msg_cf(CF_RAFT, &keys::apply_state_key(region_id), &apply_state)?;
        target.apply_state = apply_state.clone();
        target.applied_term = source.applied_term;
    }

    wb.write()?;
    target_engines.sync_kv()?;

    // raft state
    {
        let key = keys::raft_state_key(region_id);
        let raft_state = source_engines
            .raft
            .get_msg_cf(CF_DEFAULT, &key)
            .unwrap()
            .unwrap();
        raft_wb.put_raft_state(region_id, &raft_state)?;
    };

    // raft log
    let mut entries: Vec<Entry> = Default::default();
    source_engines
        .raft
        .scan_entries(region_id, |e| {
            debug!("copy raft log"; "e" => ?e);
            entries.push(e.clone());
            Ok(true)
        })
        .unwrap();

    raft_wb.append(region_id, entries)?;
    box_try!(target_engines.raft.consume(&mut raft_wb, true));

    Ok(())
}

fn fork_from_peer(
    cluster: &Cluster<NodeCluster>,
    new_peer: &metapb::Peer,
    from: u64,
    to: u64,
    region_id: u64,
) {
    debug!("fork_from_peer"; "from" => from, "to" => to, "region_id" => region_id);
    let source_region = cluster
        .ffi_helper_set
        .lock()
        .unwrap()
        .get_mut(&from)
        .unwrap()
        .engine_store_server
        .kvstore
        .get(&region_id)
        .unwrap()
        .clone();

    let mut new_region_meta = source_region.region.clone();
    new_region_meta.mut_peers().push(new_peer.clone());

    // Copy all node `from`'s data to node `to`
    iter_ffi_helpers(
        cluster,
        Some(vec![to]),
        &mut |id: u64, engine: &engine_rocks::RocksEngine, ffi: &mut FFIHelperSet| {
            let server = &mut ffi.engine_store_server;
            assert!(server.kvstore.get(&region_id).is_none());

            let new_region = make_new_region(Some(source_region.region.clone()), Some(id));
            server
                .kvstore
                .insert(source_region.region.get_id(), Box::new(new_region));
            if let Some(region) = server.kvstore.get_mut(&region_id) {
                for cf in 0..3 {
                    for (k, v) in &source_region.data[cf] {
                        write_kv_in_mem(region, cf, k.as_slice(), v.as_slice());
                    }
                }
                let source_engines = cluster.get_engines(from);
                let target_engines = cluster.get_engines(to);
                copy_meta_from(
                    source_engines,
                    target_engines,
                    &source_region,
                    region,
                    new_region_meta.clone(),
                )
                .unwrap();
            } else {
                panic!("error");
            }
        },
    );
}

fn recover_from_peer(cluster: &Cluster<NodeCluster>, from: u64, to: u64, region_id: u64) {
    fork_from_peer(cluster, &new_learner_peer(to, to), from, to, region_id);
    {
        let prev_states = maybe_collect_states(cluster, region_id, None);
        assert_eq!(
            prev_states.get(&from).unwrap().in_disk_apply_state,
            prev_states.get(&to).unwrap().in_disk_apply_state
        );
    }
}

fn force_compact_log(
    cluster: &mut Cluster<NodeCluster>,
    key: &[u8],
    use_nodes: Option<Vec<u64>>,
) -> u64 {
    let region = cluster.get_region(key);
    let region_id = region.get_id();
    let prev_states = maybe_collect_states(&cluster, region_id, None);

    let (compact_index, compact_term) = get_valid_compact_index_by(&prev_states, use_nodes);
    let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
    let _ = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    return compact_index;
}

#[test]
fn test_add_delayed_started_learner_no_snapshot() {
    // fail::cfg("before_tiflash_check_double_write", "return").unwrap();
    // fail::cfg("before_tiflash_do_write", "return").unwrap();
    let (mut cluster, pd_client) = new_later_add_learner_cluster(
        |c: &mut Cluster<NodeCluster>| {
            c.must_put(b"k1", b"v1");
            check_key(c, b"k1", b"v1", Some(true), None, Some(vec![1]));
        },
        vec![4],
    );

    // Start Leader store 4.
    cluster
        .start_with(HashSet::from_iter(
            vec![0, 1, 2, 4].into_iter().map(|x| x as usize),
        ))
        .unwrap();

    must_put_and_check_key_with_generator(
        &mut cluster,
        |i: u64| (format!("k{}", i), (0..1024).map(|_| "X").collect()),
        10,
        20,
        Some(true),
        None,
        Some(vec![1, 2, 3, 4]),
    );
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Force a compact log, so the leader have to send snapshot if peer 5 not catch
    // up.
    {
        assert!(force_compact_log(&mut cluster, b"k1", None) > 15);
    }

    // Simulate 4 is lost, recover its data to node 5.
    cluster.stop_node(4);

    later_bootstrap_learner_peer(&mut cluster, vec![5], 1);
    // After that, we manually compose data, to avoid snapshot sending.
    recover_from_peer(&cluster, 4, 5, 1);
    // Add node 5 to cluster.
    pd_client.must_add_peer(1, new_learner_peer(5, 5));

    fail::cfg("apply_on_handle_snapshot_finish_1_1", "panic").unwrap();
    // Start store 5.
    cluster
        .start_with(HashSet::from_iter(
            vec![0, 1, 2, 3].into_iter().map(|x| x as usize),
        ))
        .unwrap();

    cluster.must_put(b"z1", b"v1");
    check_key(
        &cluster,
        b"z1",
        b"v1",
        Some(true),
        None,
        Some(vec![1, 2, 3, 5]),
    );

    // Check if every node has the correct configuation.
    let new_states = maybe_collect_states(&cluster, 1, Some(vec![1, 2, 3, 5]));
    assert_eq!(new_states.len(), 4);
    for i in new_states.keys() {
        assert_eq!(
            new_states
                .get(i)
                .unwrap()
                .in_disk_region_state
                .get_region()
                .get_peers()
                .len(),
            1 + 2 /* AddPeer */ + 2 // Learner
        );
    }

    fail::remove("apply_on_handle_snapshot_finish_1_1");
    fail::remove("on_pre_persist_with_finish");
    cluster.shutdown();
    // fail::remove("before_tiflash_check_double_write");
    // fail::remove("before_tiflash_do_write");
}

#[test]
fn test_add_delayed_started_learner_snapshot() {
    let (mut cluster, pd_client) = new_later_add_learner_cluster(
        |c: &mut Cluster<NodeCluster>| {
            c.must_put(b"k1", b"v1");
            check_key(c, b"k1", b"v1", Some(true), None, Some(vec![1]));
        },
        vec![4],
    );

    // Start Leader store 4.
    cluster
        .start_with(HashSet::from_iter(
            vec![0, 1, 2, 4].into_iter().map(|x| x as usize),
        ))
        .unwrap();

    must_put_and_check_key_with_generator(
        &mut cluster,
        |i: u64| (format!("k{}", i), (0..1024).map(|_| "X").collect()),
        10,
        20,
        Some(true),
        None,
        Some(vec![1, 2, 3, 4]),
    );
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Simulate 4 is lost, recover its data to node 5.
    cluster.stop_node(4);

    // Force a compact log, so the leader have to send snapshot if peer 5 not catch
    // up.
    {
        must_put_and_check_key(&mut cluster, 21, 25, Some(true), None, Some(vec![1, 2, 3]));
        let prev_states = maybe_collect_states(&cluster, 1, Some(vec![4]));
        assert!(
            force_compact_log(&mut cluster, b"k1", Some(vec![1, 2, 3]))
                > prev_states
                    .get(&4)
                    .unwrap()
                    .in_disk_apply_state
                    .get_applied_index()
        );
    }

    later_bootstrap_learner_peer(&mut cluster, vec![5], 1);
    // After that, we manually compose data, to avoid snapshot sending.
    recover_from_peer(&cluster, 4, 5, 1);
    // Add node 5 to cluster.
    pd_client.must_add_peer(1, new_learner_peer(5, 5));

    // Start store 5.
    cluster
        .start_with(HashSet::from_iter(
            vec![0, 1, 2, 3].into_iter().map(|x| x as usize),
        ))
        .unwrap();

    cluster.must_put(b"z1", b"v1");
    check_key(
        &cluster,
        b"z1",
        b"v1",
        Some(true),
        None,
        Some(vec![1, 2, 3, 5]),
    );

    // Check if every node has the correct configuation.
    let new_states = maybe_collect_states(&cluster, 1, Some(vec![1, 2, 3, 5]));
    assert_eq!(new_states.len(), 4);
    for i in new_states.keys() {
        assert_eq!(
            new_states
                .get(i)
                .unwrap()
                .in_disk_region_state
                .get_region()
                .get_peers()
                .len(),
            1 + 2 /* AddPeer */ + 2 // Learner
        );
    }

    iter_ffi_helpers(
        &cluster,
        Some(vec![5]),
        &mut |id: u64, engine: &engine_rocks::RocksEngine, ffi: &mut FFIHelperSet| {
            (*ffi.engine_store_server).mutate_region_states(1, |e: &mut RegionStats| {
                assert_eq!(e.pre_handle_count.load(Ordering::SeqCst), 1);
            });
        },
    );

    fail::remove("on_pre_persist_with_finish");
    cluster.shutdown();
}

use std::cell::Cell;
struct NodeDebugStruct {
    pub cluster_ptr: Cell<usize>,
}

impl NodeDebugStruct {}

unsafe impl Send for NodeDebugStruct {}
unsafe impl Sync for NodeDebugStruct {}

impl engine_store_ffi::observer::DebugStruct for NodeDebugStruct {
    fn pre_replicate_peer(&self, store_id: u64, region_id: u64, peer: &metapb::Peer) {
        let cluster = unsafe {
            let cluster = self.cluster_ptr.get() as *const Cluster<NodeCluster>;
            &*cluster
        };
        if store_id == 5 {
            fork_from_peer(&cluster, peer, 4, 5, 1);
        }
    }
}

#[test]
fn test_fast_add_peer() {
    let debug = Arc::from(NodeDebugStruct {
        cluster_ptr: Cell::new(0),
    });
    let (mut cluster, pd_client) = new_mock_cluster_debug(0, 5, debug.clone());
    debug.cluster_ptr.set(&cluster as *const _ as usize);

    fail::cfg("on_pre_persist_with_finish", "return").unwrap();
    fail::cfg("try_flush_data", "return(1)").unwrap();
    disable_auto_gen_compact_log(&mut cluster);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let _ = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));

    pd_client.must_joint_confchange(
        1,
        vec![
            (ConfChangeType::AddNode, new_peer(2, 2)),
            (ConfChangeType::AddNode, new_peer(3, 3)),
            (ConfChangeType::AddLearnerNode, new_learner_peer(4, 4)),
        ],
    );
    pd_client.must_leave_joint(1);

    must_put_and_check_key(
        &mut cluster,
        2,
        10,
        Some(true),
        None,
        Some(vec![1, 2, 3, 4]),
    );

    assert!(force_compact_log(&mut cluster, b"k1", Some(vec![1, 2, 3, 4])) > 15);

    // Ensure node 4 has persisted.
    must_wait_until_cond_node(&cluster, 1, Some(vec![4]), &|new: &States| {
        new.in_disk_apply_state.get_applied_index() == new.in_memory_apply_state.get_applied_index()
    });

    pd_client.add_peer(1, new_learner_peer(5, 5));

    // recover_from_peer(&cluster, 4, 5, 1);

    cluster.must_put(b"z1", b"v1");

    check_key(
        &cluster,
        b"z1",
        b"v1",
        Some(true),
        None,
        Some(vec![1, 2, 3, 4, 5]),
    );

    fail::remove("try_flush_data");
    fail::remove("on_pre_persist_with_finish");
    cluster.shutdown();
}
