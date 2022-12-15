// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use crate::proxy::*;

#[derive(PartialEq, Eq)]
enum SourceType {
    Leader,
    Learner,
    DelayedLearner,
    InvalidSource,
}

enum PauseType {
    None,
    Build,
    ApplySnapshot,
}

#[test]
fn basic_fast_add_peer() {
    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 2);
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    // fail::cfg("on_pre_persist_with_finish", "return").unwrap();
    fail::cfg("before_tiflash_check_double_write", "return").unwrap();
    disable_auto_gen_compact_log(&mut cluster);
    // Disable auto generate peer.
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();

    cluster.must_put(b"k0", b"v0");
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1, 2]));

    cluster.shutdown();
}

fn simple_fast_add_peer(source_type: SourceType, block_wait: bool, pause: PauseType) {
    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    // fail::cfg("on_pre_persist_with_finish", "return").unwrap();
    fail::cfg("before_tiflash_check_double_write", "return").unwrap();
    if block_wait {
        fail::cfg("ffi_fast_add_peer_block_wait", "return(1)").unwrap();
    }
    disable_auto_gen_compact_log(&mut cluster);
    // Disable auto generate peer.
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();

    // If we don't write here, we will have the first MsgAppend with (6,6), which
    // will cause "fast-forwarded commit to snapshot".
    cluster.must_put(b"k0", b"v0");

    // Add learner 2 from leader 1
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    // std::thread::sleep(std::time::Duration::from_millis(2000));
    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1, 2]));

    // Getting (k1,v1) not necessarily means peer 2 is ready.
    must_wait_until_cond_node(&cluster, 1, Some(vec![2]), &|states: &States| -> bool {
        find_peer_by_id(states.in_disk_region_state.get_region(), 2).is_some()
    });

    // Add learner 3 according to source_type
    match source_type {
        SourceType::Learner | SourceType::DelayedLearner => {
            fail::cfg("ffi_fast_add_peer_from_id", "return(2)").unwrap();
        }
        SourceType::InvalidSource => {
            fail::cfg("ffi_fast_add_peer_from_id", "return(100)").unwrap();
        }
        _ => (),
    };

    match pause {
        PauseType::Build => fail::cfg("ffi_fast_add_peer_pause", "pause").unwrap(),
        PauseType::ApplySnapshot => fail::cfg("on_can_apply_snapshot", "return(false)").unwrap(),
        _ => (),
    }

    // Add peer 3
    pd_client.must_add_peer(1, new_learner_peer(3, 3));
    cluster.must_put(b"k2", b"v2");

    match source_type {
        SourceType::DelayedLearner => {
            // Make sure conf change is applied in peer 2.
            check_key(&cluster, b"k2", b"v2", Some(true), None, Some(vec![1, 2]));
            cluster.add_send_filter(CloneFilterFactory(
                RegionPacketFilter::new(1, 2)
                    .msg_type(MessageType::MsgAppend)
                    .msg_type(MessageType::MsgSnapshot)
                    .direction(Direction::Recv),
            ));
            cluster.must_put(b"k3", b"v3");
        }
        _ => (),
    };

    match pause {
        PauseType::Build => {
            std::thread::sleep(std::time::Duration::from_millis(3000));
            fail::remove("ffi_fast_add_peer_pause");
        }
        PauseType::ApplySnapshot => {
            std::thread::sleep(std::time::Duration::from_millis(4000));
            fail::remove("on_can_apply_snapshot");
            fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5000));
        }
        _ => (),
    }

    match source_type {
        SourceType::DelayedLearner => {
            check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![1, 3]));
            check_key(&cluster, b"k3", b"v3", Some(false), None, Some(vec![2]));
        }
        SourceType::Learner => {
            check_key(
                &cluster,
                b"k2",
                b"v2",
                Some(true),
                None,
                Some(vec![1, 2, 3]),
            );
        }
        _ => {
            check_key(
                &cluster,
                b"k2",
                b"v2",
                Some(true),
                None,
                Some(vec![1, 2, 3]),
            );
        }
    };
    must_wait_until_cond_node(&cluster, 1, Some(vec![3]), &|states: &States| -> bool {
        find_peer_by_id(states.in_disk_region_state.get_region(), 3).is_some()
    });

    match pause {
        PauseType::ApplySnapshot => {
            iter_ffi_helpers(
                &cluster,
                Some(vec![3]),
                &mut |_, _, ffi: &mut FFIHelperSet| {
                    let server = &ffi.engine_store_server;
                    (*ffi.engine_store_server).mutate_region_states(1, |e: &mut RegionStats| {
                        assert_eq!(1, e.fast_add_peer_count.load(Ordering::SeqCst));
                    });
                },
            );
        }
        _ => (),
    }

    match source_type {
        SourceType::DelayedLearner => {
            cluster.clear_send_filters();
        }
        _ => (),
    };

    // Destroy peer
    pd_client.must_remove_peer(1, new_learner_peer(3, 3));
    must_wait_until_cond_node(&cluster, 1, Some(vec![1]), &|states: &States| -> bool {
        find_peer_by_id(states.in_disk_region_state.get_region(), 3).is_none()
    });
    std::thread::sleep(std::time::Duration::from_millis(1000));
    iter_ffi_helpers(
        &cluster,
        Some(vec![3]),
        &mut |_, _, ffi: &mut FFIHelperSet| {
            let server = &ffi.engine_store_server;
            assert!(!server.kvstore.contains_key(&1));
            (*ffi.engine_store_server).mutate_region_states(1, |e: &mut RegionStats| {
                e.fast_add_peer_count.store(0, Ordering::SeqCst);
            });
        },
    );
    cluster.must_put(b"k5", b"v5");
    // These failpoints make sure we will cause again a fast path.
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(3, 4));
    // Wait until Learner has applied ConfChange
    std::thread::sleep(std::time::Duration::from_millis(1000));
    must_wait_until_cond_node(&cluster, 1, Some(vec![3]), &|states: &States| -> bool {
        find_peer_by_id(states.in_disk_region_state.get_region(), 4).is_some()
    });
    // If we re-add peer, we can still go fast path.
    iter_ffi_helpers(
        &cluster,
        Some(vec![3]),
        &mut |id: u64, engine: &engine_rocks::RocksEngine, ffi: &mut FFIHelperSet| {
            (*ffi.engine_store_server).mutate_region_states(1, |e: &mut RegionStats| {
                assert!(e.fast_add_peer_count.load(Ordering::SeqCst) > 0);
            });
        },
    );
    cluster.must_put(b"k6", b"v6");
    check_key(
        &cluster,
        b"k6",
        b"v6",
        Some(true),
        None,
        Some(vec![1, 2, 3]),
    );
    fail::remove("fallback_to_slow_path_not_allow");
    fail::remove("fast_path_is_not_first");

    fail::remove("on_can_apply_snapshot");
    fail::remove("ffi_fast_add_peer_from_id");
    fail::remove("on_pre_persist_with_finish");
    fail::remove("ffi_fast_add_peer_block_wait");
    cluster.shutdown();
}

#[test]
fn test_fast_add_peer_from_leader() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::Leader, false, PauseType::None);
    fail::remove("fallback_to_slow_path_not_allow");
}

/// Fast path by learner snapshot.
#[test]
fn test_fast_add_peer_from_learner() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::Learner, false, PauseType::None);
    fail::remove("fallback_to_slow_path_not_allow");
}

/// If a learner is delayed, but already applied ConfChange.
#[test]
fn test_fast_add_peer_from_delayed_learner() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::DelayedLearner, false, PauseType::None);
    fail::remove("fallback_to_slow_path_not_allow");
}

/// If we select a wrong source, or we can't run fast path, we can fallback to
/// normal.
#[test]
fn test_fast_add_peer_from_invalid_source() {
    simple_fast_add_peer(SourceType::InvalidSource, false, PauseType::None);
}

#[test]
fn test_fast_add_peer_from_learner_blocked() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::Learner, true, PauseType::None);
    fail::remove("fallback_to_slow_path_not_allow");
}

#[test]
fn test_fast_add_peer_from_delayed_learner_blocked() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::DelayedLearner, true, PauseType::None);
    fail::remove("fallback_to_slow_path_not_allow");
}

#[test]
fn test_fast_add_peer_from_learner_blocked_paused_build() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::Learner, true, PauseType::Build);
    fail::remove("fallback_to_slow_path_not_allow");
}

#[test]
fn test_fast_add_peer_from_delayed_learner_blocked_paused_build() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::DelayedLearner, true, PauseType::Build);
    fail::remove("fallback_to_slow_path_not_allow");
}

#[test]
fn test_fast_add_peer_from_learner_blocked_paused_apply() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::Learner, true, PauseType::ApplySnapshot);
    fail::remove("fallback_to_slow_path_not_allow");
}

#[test]
fn test_fast_add_peer_from_delayed_learner_blocked_paused_apply() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::DelayedLearner, true, PauseType::ApplySnapshot);
    fail::remove("fallback_to_slow_path_not_allow");
}

#[test]
fn test_existing_peer() {
    fail::cfg("before_tiflash_check_double_write", "return").unwrap();

    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 2);
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    // fail::cfg("on_pre_persist_with_finish", "return").unwrap();
    disable_auto_gen_compact_log(&mut cluster);
    // Disable auto generate peer.
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();
    must_put_and_check_key(&mut cluster, 1, 2, Some(true), None, Some(vec![1]));

    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    must_put_and_check_key(&mut cluster, 3, 4, Some(true), None, None);
    fail::remove("fallback_to_slow_path_not_allow");

    stop_tiflash_node(&mut cluster, 2);
    fail::cfg("go_fast_path_not_allow", "panic").unwrap();
    restart_tiflash_node(&mut cluster, 2);
    must_put_and_check_key(&mut cluster, 5, 6, Some(true), None, None);

    cluster.shutdown();
    fail::remove("go_fast_path_not_allow");
    fail::remove("before_tiflash_check_double_write");
}

#[test]
fn test_apply_snapshot() {
    fail::cfg("before_tiflash_check_double_write", "return").unwrap();

    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    // fail::cfg("on_pre_persist_with_finish", "return").unwrap();
    disable_auto_gen_compact_log(&mut cluster);
    // Disable auto generate peer.
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();

    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    must_put_and_check_key(&mut cluster, 1, 2, Some(true), None, Some(vec![1]));

    // We add peer 3, it will be paused before fetching peer 2's data.
    // However, peer 2 will apply conf change.
    fail::cfg("ffi_fast_add_peer_from_id", "return(2)").unwrap();
    fail::cfg("ffi_fast_add_peer_pause", "pause").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(3, 3));
    std::thread::sleep(std::time::Duration::from_millis(1000));
    must_put_and_check_key(&mut cluster, 2, 3, Some(true), None, Some(vec![1, 2]));
    must_wait_until_cond_node(&cluster, 1, Some(vec![2]), &|states: &States| -> bool {
        find_peer_by_id(states.in_disk_region_state.get_region(), 3).is_some()
    });

    // peer 2 can't apply new kvs.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .msg_type(MessageType::MsgAppend)
            .direction(Direction::Recv),
    ));
    cluster.must_put(b"k3", b"v3");
    cluster.must_put(b"k4", b"v4");
    force_compact_log(&mut cluster, b"k2", Some(vec![1]));
    // Log compacted, peer 2 will get snapshot, however, we pause when applying
    // snapshot.
    fail::cfg("on_ob_post_apply_snapshot", "pause").unwrap();
    // Trigger a snapshot to 2.
    cluster.clear_send_filters();

    std::thread::sleep(std::time::Duration::from_millis(300));
    // Now if we continue fast path, peer 2 will be in Applying state.
    // We will end up going slow path.
    fail::remove("ffi_fast_add_peer_pause");
    fail::cfg("go_fast_path_succeed", "panic").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(300));
    // Resume applying snapshot
    fail::remove("on_ob_post_apply_snapshot");
    check_key(&cluster, b"k4", b"v4", Some(true), None, Some(vec![1, 3]));
    cluster.shutdown();
    fail::remove("go_fast_path_succeed");
    fail::remove("ffi_fast_add_peer_from_id");
    fail::remove("before_tiflash_check_double_write");
}

#[test]
fn test_split_merge() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    pd_client.disable_default_operator();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;

    tikv_util::set_panic_hook(true, "./");
    // Can always apply snapshot immediately
    fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
    cluster.cfg.raft_store.right_derive_when_split = true;

    let _ = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));
    check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![1]));

    let r1 = cluster.get_region(b"k1");
    let r3 = cluster.get_region(b"k3");
    assert_eq!(r1.get_id(), r3.get_id());

    cluster.must_split(&r1, b"k2");
    let r1_new = cluster.get_region(b"k1"); // 1000
    let r3_new = cluster.get_region(b"k3"); // 1

    let r1_id = r1_new.get_id();
    let r3_id = r3_new.get_id();
    debug!("r1_new {} r3_new {}", r1_id, r3_id);

    // Test add peer after split
    pd_client.must_add_peer(r1_id, new_learner_peer(2, 2001));
    std::thread::sleep(std::time::Duration::from_millis(1000));
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![2]));
    check_key(&cluster, b"k3", b"v3", Some(false), None, Some(vec![2]));
    pd_client.must_add_peer(r3_id, new_learner_peer(2, 2003));
    std::thread::sleep(std::time::Duration::from_millis(1000));
    check_key(&cluster, b"k1", b"v1", Some(false), None, Some(vec![2]));
    check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![2]));

    // Test merge
    pd_client.must_add_peer(r3_id, new_learner_peer(3, 3003));
    pd_client.merge_region(r1_id, r3_id);
    must_not_merged(pd_client.clone(), r1_id, Duration::from_millis(1000));
    pd_client.must_add_peer(r1_id, new_learner_peer(3, 3001));
    pd_client.must_merge(r1_id, r3_id);
    check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![3]));
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![3]));

    fail::remove("on_can_apply_snapshot");
    cluster.shutdown();
}
