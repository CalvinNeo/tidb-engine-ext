// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    env,
    iter::Iterator,
    ops::Deref,
    path::Path,
    rc::Rc,
    sync::{atomic::AtomicU64, Arc},
    thread,
    time::Duration,
    vec,
};

use bytes::{Buf, Bytes};
use cloud_encryption::MasterKey;
use file_system::IoRateLimiter;
use kvenginepb as pb;
use tempfile::TempDir;
use tikv_util::{mpsc, time::Instant};

use crate::{
    dfs::InMemFs,
    table::{
        memtable::CfTable,
        sstable::{InMemFile, SsTable},
        InnerKey, BIT_DELETE,
    },
    *,
};

macro_rules! unwrap_or_return {
    ($e:expr, $m:expr) => {
        match $e {
            Ok(x) => x,
            Err(y) => {
                error!("{:?} {:?}", y, $m);
                return;
            }
        }
    };
}

const DEF_BLOCK_SIZE: usize = 4 << 10;

fn new_test_engine() -> (Engine, mpsc::Sender<ApplyTask>) {
    new_test_engine_opt(false, DEF_BLOCK_SIZE)
}

fn new_test_engine_opt(
    enable_inner_key_off: bool,
    block_size: usize,
) -> (Engine, mpsc::Sender<ApplyTask>) {
    let (listener_tx, listener_rx) = mpsc::unbounded();
    let tester = EngineTester::new(enable_inner_key_off, block_size);
    let meta_change_listener = Box::new(TestMetaChangeListener {
        sender: listener_tx,
    });
    let rate_limiter = Arc::new(IoRateLimiter::new_for_test());
    let mut meta_iter = tester.clone();
    let engine = Engine::open(
        tester.fs.clone(),
        tester.opts.clone(),
        tester.config.clone(),
        &mut meta_iter,
        tester.clone(),
        tester.core.clone(),
        meta_change_listener,
        rate_limiter,
        None,
        MasterKey::new(&[1u8; 32]),
    )
    .unwrap();
    {
        let shard = engine.get_shard(1).unwrap();
        store_bool(&shard.active, true);
    }
    let (applier_tx, applier_rx) = mpsc::unbounded();
    let meta_listener = MetaListener::new(listener_rx, applier_tx.clone());
    thread::spawn(move || {
        meta_listener.run();
    });
    let applier = Applier::new(engine.clone(), applier_rx);
    thread::spawn(move || {
        applier.run();
    });
    (engine, applier_tx)
}

#[test]
fn test_engine() {
    init_logger();
    let (engine, applier_tx) = new_test_engine();
    // FIXME(youjiali1995): split has bugs.
    //
    // let mut keys = vec![];
    // for i in &[1000, 3000, 6000, 9000] {
    // keys.push(i_to_key(*i,
    // engine.opts.blob_table_build_options.min_blob_size)); }
    // let mut splitter = Splitter::new(keys.clone(), applier_tx.clone());
    // let handle = thread::spawn(move || {
    // splitter.run();
    // });
    let (begin, end) = (0, 10000);
    load_data(
        begin,
        end,
        1,
        applier_tx,
        engine.opts.blob_table_build_options.min_blob_size,
    );
    // handle.join().unwrap();
    check_get(
        begin,
        end,
        2,
        &[0, 1, 2],
        &engine,
        true,
        None,
        engine.opts.blob_table_build_options.min_blob_size,
    );
    check_iterater(begin, end, &engine);
}

#[test]
fn test_destroy_range() {
    init_logger();
    let (engine, applier_tx) = new_test_engine();
    let mem_table_count = engine.get_shard_stat(1).mem_table_count;
    load_data(
        10,
        50,
        1,
        applier_tx.clone(),
        engine.opts.blob_table_build_options.min_blob_size,
    );
    // Unsafe destroy keys [10, 30).
    for prefix in [10, 20] {
        let mut wb = WriteBatch::new(1, 0);
        let key = i_to_key(prefix, engine.opts.blob_table_build_options.min_blob_size);
        wb.set_property(DEL_PREFIXES_KEY, key[..key.len() - 1].as_bytes());
        write_data(wb, &applier_tx);
    }
    assert!(!engine.get_shard(1).unwrap().get_del_prefixes().is_empty());
    // Memtable is switched because it contains data covered by the delete-prefixes.
    let stats = engine.get_shard_stat(1);
    assert_eq!(
        stats.mem_table_count + stats.l0_table_count + stats.blob_table_count,
        mem_table_count + 1
    );
    let wait_for_destroying_range = || {
        for _ in 0..30 {
            let shard = engine.get_shard(1).unwrap();
            let data = shard.get_data();
            let del_prefixes = shard.get_del_prefixes();
            if del_prefixes.is_empty() {
                break;
            }
            if Shard::ready_to_destroy_range(&del_prefixes, &data) {
                engine.trigger_compact(shard.id_ver());
            }
            thread::sleep(Duration::from_millis(100));
        }
        let shard = engine.get_shard(1).unwrap();
        let length = shard
            .get_property(DEL_PREFIXES_KEY)
            .map(|v| v.len())
            .unwrap_or_default();
        // Delete-prefixes is cleaned up.
        assert_eq!(length, 0);
    };
    wait_for_destroying_range();
    // After destroying range, key [10, 30) should be removed.
    check_get(
        10,
        30,
        2,
        &[0, 1, 2],
        &engine,
        false,
        None,
        engine.opts.blob_table_build_options.min_blob_size,
    );
    check_get(
        30,
        50,
        2,
        &[0, 1, 2],
        &engine,
        true,
        None,
        engine.opts.blob_table_build_options.min_blob_size,
    );
    check_iterater(30, 50, &engine);

    // Trigger L0 compaction.
    for i in 1..=10 {
        load_data(
            50 + (i - 1) * 10,
            50 + i * 10,
            1,
            applier_tx.clone(),
            engine.opts.blob_table_build_options.min_blob_size,
        );
        let mut wb = WriteBatch::new(1, 0);
        wb.set_switch_mem_table();
        write_data(wb, &applier_tx);
    }
    // Waiting for L0 compaction.
    for _ in 0..30 {
        if engine.get_shard_stat(1).l0_table_count == 0 {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(engine.get_shard_stat(1).l0_table_count < 10);
    // Unsafe destroy keys [100, 150).
    let mut wb = WriteBatch::new(1, 0);
    let key = i_to_key(100, engine.opts.blob_table_build_options.min_blob_size);
    wb.set_property(DEL_PREFIXES_KEY, key[..key.len() - 2].as_bytes());
    write_data(wb, &applier_tx);
    wait_for_destroying_range();
    check_get(
        100,
        150,
        2,
        &[0, 1, 2],
        &engine,
        false,
        None,
        engine.opts.blob_table_build_options.min_blob_size,
    );
    check_get(
        50,
        100,
        2,
        &[0, 1, 2],
        &engine,
        true,
        None,
        engine.opts.blob_table_build_options.min_blob_size,
    );

    // Clean all data.
    let mut wb = WriteBatch::new(1, 0);
    wb.set_property(DEL_PREFIXES_KEY, b"key");
    write_data(wb, &applier_tx);
    wait_for_destroying_range();
    check_get(
        10,
        150,
        2,
        &[0, 1, 2],
        &engine,
        false,
        None,
        engine.opts.blob_table_build_options.min_blob_size,
    );

    // No data exists and delete-prefixes can be cleaned too.
    let mut wb = WriteBatch::new(1, 0);
    wb.set_property(DEL_PREFIXES_KEY, b"key");
    write_data(wb, &applier_tx);
    wait_for_destroying_range();
}

#[test]
fn test_truncate_ts_request() {
    init_logger();
    // TODO: disable compaction, otherwise this case would be unstable.
    let (engine, applier_tx) = new_test_engine();
    let version = 1000;
    load_data(
        10,
        50,
        version,
        applier_tx.clone(),
        engine.opts.blob_table_build_options.min_blob_size,
    );
    // In case auto truncate_ts compaction finishes too fast.
    engine.get_shard(1).unwrap().set_active(false);
    // truncate ts.
    let mut wb = WriteBatch::new(1, 0);
    let truncate_ts = TruncateTs::from(version + 10);
    wb.set_property(TRUNCATE_TS_KEY, truncate_ts.marshal().as_slice());
    write_data(wb, &applier_tx);
    assert_eq!(
        Some(truncate_ts),
        engine.get_shard(1).unwrap().get_truncate_ts()
    );
    assert_eq!(
        truncate_ts.marshal().as_slice(),
        engine
            .get_shard(1)
            .unwrap()
            .get_property(TRUNCATE_TS_KEY)
            .unwrap()
    );
    let mut cs = pb::ChangeSet::new();
    cs.set_shard_id(1);
    cs.set_shard_ver(1);
    cs.set_sequence(2);
    let tc = pb::TableChange::new();
    cs.set_truncate_ts(tc);
    cs.set_property_key(TRUNCATE_TS_KEY.to_owned());

    // truncated_ts is larger than current truncate ts, don't change it.
    let truncated_ts = TruncateTs::from(version + 20);
    cs.set_property_value(truncated_ts.marshal().to_vec());
    let ret = engine.apply_change_set(apply::ChangeSet::new(cs.clone()));
    ret.unwrap();
    assert_eq!(
        Some(truncate_ts),
        engine.get_shard(1).unwrap().get_truncate_ts()
    );

    // truncated_ts is equal than current truncate ts, remove truncate ts in shard.
    let truncated_ts = TruncateTs::from(version + 10);
    cs.set_sequence(3);
    cs.set_property_value(truncated_ts.marshal().to_vec());
    let ret = engine.apply_change_set(apply::ChangeSet::new(cs));
    ret.unwrap();
    assert_eq!(None, engine.get_shard(1).unwrap().get_truncate_ts());
}

#[test]
fn test_truncate_ts() {
    init_logger();
    let (engine, applier_tx) = new_test_engine();

    // `tolerate_none`: set `true` when there is no data to be truncated.
    // In this scene, truncate ts may have been done before we check
    // `ShardData.truncate_ts`.
    let set_truncate_ts = |ts: u64, tolerate_none: bool| {
        let mut wb = WriteBatch::new(1, 0);
        let truncate_ts = TruncateTs::from(ts);
        wb.set_property(TRUNCATE_TS_KEY, truncate_ts.marshal().as_slice());
        write_data(wb, &applier_tx);

        let res = engine.get_shard(1).unwrap().get_truncate_ts();
        if res.is_none() && tolerate_none {
            return;
        }
        assert_eq!(Some(truncate_ts), res);

        let res_bin = engine
            .get_shard(1)
            .unwrap()
            .get_property(TRUNCATE_TS_KEY)
            .unwrap();
        if res_bin.is_empty() && tolerate_none {
            return;
        }
        assert_eq!(truncate_ts.marshal().as_slice(), res_bin);
    };

    let wait_for_truncate_ts = || {
        let ok = try_wait(
            || engine.get_shard(1).unwrap().get_truncate_ts().is_none(),
            10,
        );
        assert!(
            ok,
            "wait_for_truncate_ts timeout, shard:{:?}",
            engine.get_shard_stat(1)
        );
        let shard = engine.get_shard(1).unwrap();
        let length = shard
            .get_property(TRUNCATE_TS_KEY)
            .map(|v| v.len())
            .unwrap_or_default();
        assert_eq!(length, 0);
    };

    load_data(
        0,
        300,
        1000,
        applier_tx.clone(),
        engine.opts.blob_table_build_options.min_blob_size,
    );
    load_data(
        100,
        400,
        2000,
        applier_tx.clone(),
        engine.opts.blob_table_build_options.min_blob_size,
    );
    load_data(
        200,
        500,
        3000,
        applier_tx.clone(),
        engine.opts.blob_table_build_options.min_blob_size,
    );

    const ALL_CFS: &[usize] = &[0, 1, 2];
    const WRT_EXT_CFS: &[usize] = &[0, 2];

    {
        // No truncate.
        set_truncate_ts(3000, true);
        thread::sleep(Duration::from_secs(1));
        wait_for_truncate_ts();
        check_get(
            0,
            100,
            u64::MAX,
            ALL_CFS,
            &engine,
            true,
            Some(1000),
            engine.opts.blob_table_build_options.min_blob_size,
        );
        check_get(
            100,
            300,
            1000,
            ALL_CFS,
            &engine,
            true,
            Some(1000),
            engine.opts.blob_table_build_options.min_blob_size,
        );
        check_get(
            100,
            200,
            u64::MAX,
            ALL_CFS,
            &engine,
            true,
            Some(2000),
            engine.opts.blob_table_build_options.min_blob_size,
        );
        check_get(
            200,
            400,
            2000,
            ALL_CFS,
            &engine,
            true,
            Some(2000),
            engine.opts.blob_table_build_options.min_blob_size,
        );
        check_get(
            200,
            500,
            u64::MAX,
            ALL_CFS,
            &engine,
            true,
            Some(3000),
            engine.opts.blob_table_build_options.min_blob_size,
        );
    }

    for (i, truncate_ts) in [2999, 2000].iter().enumerate() {
        set_truncate_ts(*truncate_ts as u64, i > 0);
        wait_for_truncate_ts();
        check_get(
            0,
            100,
            u64::MAX,
            ALL_CFS,
            &engine,
            true,
            Some(1000),
            engine.opts.blob_table_build_options.min_blob_size,
        );
        check_get(
            100,
            300,
            1000,
            ALL_CFS,
            &engine,
            true,
            Some(1000),
            engine.opts.blob_table_build_options.min_blob_size,
        );
        check_get(
            100,
            400,
            u64::MAX,
            ALL_CFS,
            &engine,
            true,
            Some(2000),
            engine.opts.blob_table_build_options.min_blob_size,
        );
        check_get(
            400,
            500,
            u64::MAX,
            WRT_EXT_CFS,
            &engine,
            false,
            None,
            engine.opts.blob_table_build_options.min_blob_size,
        );
    }

    for (i, truncate_ts) in [1999, 1000].iter().enumerate() {
        set_truncate_ts(*truncate_ts as u64, i > 0);
        wait_for_truncate_ts();
        check_get(
            0,
            300,
            u64::MAX,
            ALL_CFS,
            &engine,
            true,
            Some(1000),
            engine.opts.blob_table_build_options.min_blob_size,
        );
        check_get(
            300,
            500,
            u64::MAX,
            WRT_EXT_CFS,
            &engine,
            false,
            None,
            engine.opts.blob_table_build_options.min_blob_size,
        );
    }

    {
        // Truncate all.
        set_truncate_ts(999, false);
        wait_for_truncate_ts();
        check_get(
            0,
            500,
            u64::MAX,
            WRT_EXT_CFS,
            &engine,
            false,
            None,
            engine.opts.blob_table_build_options.min_blob_size,
        );
    }
}

// This test case construct a LSM tree and trigger a particular compaction to
// verify deleted entry will not reappear after the compaction.
// In the LSM true, L3 contains data, L2 contains tombstone, L1 contains data.
// The range for overlap check should use both L1 and L2 instead of only L1.
// The tombstone entries will be discarded when the compaction is
// non-overlapping. If only use L1's range for overlap check, some of the L2's
// tombstone will be lost, cause deleted entries reappear.
#[test]
fn test_lost_tombstone_issue() {
    init_logger();
    let (engine, _) = new_test_engine();
    let shard = engine.get_shard(1).unwrap();

    let mut cf_builder = ShardCfBuilder::new(0);
    let mut saved_vals: Vec<Rc<String>> = Vec::new();

    cf_builder.add_table(
        new_table(&engine, 11, 0, 100, 101, false, &mut saved_vals),
        3,
    );
    cf_builder.add_table(
        new_table(&engine, 12, 50, 150, 102, true, &mut saved_vals),
        2,
    );
    cf_builder.add_table(
        new_table(&engine, 13, 120, 200, 103, false, &mut saved_vals),
        1,
    );
    let data = ShardData::new(
        shard.range.clone(),
        vec![CfTable::new()],
        vec![],
        Arc::new(HashMap::default()),
        [cf_builder.build(), ShardCf::new(1), ShardCf::new(2)],
        HashMap::new(),
    );
    shard.set_data(data);
    let pri = CompactionPriority::L1Plus {
        cf: 0,
        score: 2.0,
        level: 1,
    };
    let mut guard = shard.compaction_priority.write().unwrap();
    *guard = Some(pri);
    drop(guard);
    engine.update_managed_safe_ts(104);
    engine.trigger_compact(IdVer::new(1, 1));
    thread::sleep(Duration::from_secs(1));
    check_get(50, 100, 104, &[0], &engine, false, None, 0);
}

// This test case consturct a three level LSM tree and with different version
// and add tombstone with latest version. Iterator should return all versions of
// the key, excluding the tombstoned key.
#[test]
fn test_read_iterator_all_versions() {
    init_logger();
    let (engine, _) = new_test_engine();
    let shard = engine.get_shard(1).unwrap();
    let cf = 0;

    let mut cf_builder = ShardCfBuilder::new(cf);
    let mut saved_vals: Vec<Rc<String>> = Vec::new();

    // 0..50 has only version 101
    // 50..70 has version 101 and 102
    // 70..90 has version 103 marked deleted, and older version 101 and 102
    // 90..100 has version 101 and 102
    // 100..150 has version 102
    cf_builder.add_table(
        new_table(&engine, 11, 0, 100, 101, false, &mut saved_vals),
        3,
    );
    cf_builder.add_table(
        new_table(&engine, 12, 50, 150, 102, false, &mut saved_vals),
        2,
    );
    cf_builder.add_table(
        new_table(&engine, 13, 70, 90, 103, true, &mut saved_vals),
        1,
    );

    let data = ShardData::new(
        shard.range.clone(),
        vec![CfTable::new()],
        vec![],
        Arc::new(HashMap::default()),
        [cf_builder.build(), ShardCf::new(1), ShardCf::new(2)],
        HashMap::new(),
    );
    shard.set_data(data);

    let snap = SnapAccess::new(&shard);
    let mut iter = snap.new_iterator(cf, false, true, None, false);
    iter.seek(shard.outer_start.chunk());

    let mut expected_keys = Vec::with_capacity(160);
    for i in 0..50 {
        expected_keys.push(i);
    }
    for i in 50..70 {
        expected_keys.push(i);
        expected_keys.push(i);
    }

    // 70..90 should not returned

    for i in 90..100 {
        expected_keys.push(i);
        expected_keys.push(i);
    }
    for i in 100..150 {
        expected_keys.push(i);
    }
    let mut expected_keys_iter = expected_keys.iter();

    while iter.valid() {
        let key = i_to_key(expected_keys_iter.next().unwrap().to_owned(), 0);
        assert_eq!(iter.key(), key.as_bytes());
        assert_eq!(iter.val(), key.repeat(2).as_bytes());
        iter.next();
    }
}

#[test]
fn test_level_overlapping_tables() {
    init_logger();
    test_level_overlapping_tables_impl(false);
    test_level_overlapping_tables_impl(true);
}

fn test_level_overlapping_tables_impl(enable_inner_key_off: bool) {
    let (engine, _) = new_test_engine_opt(enable_inner_key_off, DEF_BLOCK_SIZE);
    let shard = engine.get_shard(1).unwrap();

    let mut cf_builder = ShardCfBuilder::new(0);
    let mut saved_vals: Vec<Rc<String>> = Vec::new();

    let table_ranges: Vec<(usize, usize)> = vec![(10, 20), (50, 100), (120, 200)];
    for (i, (start, end)) in table_ranges.into_iter().enumerate() {
        cf_builder.add_table(
            new_table(
                &engine,
                i as u64,
                start,
                end,
                i as u64 + 100,
                false,
                &mut saved_vals,
            ),
            1, // level
        );
    }
    let data = ShardData::new(
        shard.range.clone(),
        vec![CfTable::new()],
        vec![],
        Arc::new(HashMap::default()),
        [cf_builder.build(), ShardCf::new(1), ShardCf::new(2)],
        HashMap::new(),
    );

    let cf0 = data.get_cf(0);
    let level1 = cf0.get_level(1);
    let get_overlapping_tables = |start: i32, end: i32| -> (usize, usize) {
        level1.overlapping_tables_exclusive_end(
            InnerKey::from_inner_buf(&i_to_key(start, 0).into_bytes()),
            InnerKey::from_inner_buf(&i_to_key(end, 0).into_bytes()),
        )
    };

    assert_eq!(get_overlapping_tables(0, 10), (0, 0));
    assert_eq!(get_overlapping_tables(0, 20), (0, 1));
    assert_eq!(get_overlapping_tables(0, 50), (0, 1));
    assert_eq!(get_overlapping_tables(0, 100), (0, 2));
    assert_eq!(get_overlapping_tables(0, 120), (0, 2));
    assert_eq!(get_overlapping_tables(0, 200), (0, 3));
    assert_eq!(get_overlapping_tables(0, 300), (0, 3));

    assert_eq!(get_overlapping_tables(10, 20), (0, 1));
    assert_eq!(get_overlapping_tables(10, 50), (0, 1));
    assert_eq!(get_overlapping_tables(10, 100), (0, 2));
    assert_eq!(get_overlapping_tables(10, 120), (0, 2));
    assert_eq!(get_overlapping_tables(10, 200), (0, 3));
    assert_eq!(get_overlapping_tables(10, 300), (0, 3));

    assert_eq!(get_overlapping_tables(20, 50), (1, 1));
    assert_eq!(get_overlapping_tables(20, 100), (1, 2));
    assert_eq!(get_overlapping_tables(20, 120), (1, 2));
    assert_eq!(get_overlapping_tables(20, 200), (1, 3));
    assert_eq!(get_overlapping_tables(20, 300), (1, 3));

    assert_eq!(get_overlapping_tables(50, 100), (1, 2));
    assert_eq!(get_overlapping_tables(50, 120), (1, 2));
    assert_eq!(get_overlapping_tables(50, 200), (1, 3));
    assert_eq!(get_overlapping_tables(50, 300), (1, 3));

    assert_eq!(get_overlapping_tables(100, 120), (2, 2));
    assert_eq!(get_overlapping_tables(100, 200), (2, 3));
    assert_eq!(get_overlapping_tables(100, 300), (2, 3));

    assert_eq!(get_overlapping_tables(120, 200), (2, 3));
    assert_eq!(get_overlapping_tables(120, 300), (2, 3));

    assert_eq!(get_overlapping_tables(200, 300), (3, 3));
}

#[test]
fn test_get_suggest_split_key() {
    init_logger();
    test_get_suggest_split_key_impl(false);
    test_get_suggest_split_key_impl(true);
}

fn test_get_suggest_split_key_impl(enable_inner_key_off: bool) {
    let (engine, _) = new_test_engine_opt(enable_inner_key_off, 10);
    let shard = engine.get_shard(1).unwrap();

    let range = ShardRange::new(
        &i_to_key(100, 0).into_bytes(),
        &i_to_key(200, 0).into_bytes(),
        shard.inner_key_off,
    );
    let shard = Shard::new(
        shard.engine_id,
        &shard.properties.to_pb(shard.id),
        shard.ver,
        range.clone(),
        engine.opts.clone(),
        &engine.master_key,
    );

    let cases: Vec<(
        Vec<(i32, i32)>, // table ranges
        Option<i32>,     // expected suggest split key,
    )> = vec![
        (vec![(0, 20), (20, 50), (50, 100)], None),
        (vec![(0, 20), (20, 50), (100, 150), (150, 180)], Some(150)),
        (
            vec![
                (20, 50),
                (100, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
                (300, 400),
                (400, 500),
                (500, 600),
                (700, 800),
            ],
            Some(180),
        ),
        (
            vec![
                (20, 50),
                (100, 150), // in range, key in block
                (300, 400),
                (400, 500),
                (500, 600),
                (700, 800),
            ],
            Some(125),
        ),
        (
            vec![
                (20, 50),
                (100, 200), // in range, key in block
                (300, 400),
                (400, 500),
                (500, 600),
                (700, 800),
            ],
            Some(150),
        ),
        (
            vec![
                (20, 50),
                (100, 500), // would be 366 if shard range is not considered
                (500, 600),
                (700, 800),
            ],
            Some(150),
        ),
        (
            vec![
                (20, 50),
                (150, 151), // only one block
                (500, 600),
                (700, 800),
            ],
            None,
        ),
        (
            vec![(20, 50), (197, 200), (500, 600), (700, 800)],
            Some(199),
        ),
        (
            vec![
                (20, 50),
                (198, 201), // will be 200 if shard range is not considered
                (500, 600),
                (700, 800),
            ],
            Some(199),
        ),
    ];

    for (idx, (table_ranges, expect_split_key)) in cases.into_iter().enumerate() {
        let mut cf_builder = ShardCfBuilder::new(0);
        let mut saved_vals: Vec<Rc<String>> = Vec::new();

        for (i, (start, end)) in table_ranges.into_iter().enumerate() {
            cf_builder.add_table(
                new_table(
                    &engine,
                    i as u64 + 1,
                    start as usize,
                    end as usize,
                    100 + i as u64,
                    false,
                    &mut saved_vals,
                ),
                1,
            );
        }

        let data = ShardData::new(
            range.clone(),
            vec![CfTable::new()],
            vec![],
            Arc::new(HashMap::default()),
            [cf_builder.build(), ShardCf::new(1), ShardCf::new(2)],
            HashMap::new(),
        );
        shard.set_data(data);

        let key = shard.get_suggest_split_key();
        assert_eq!(
            key.map(|k| k.to_vec()),
            expect_split_key.map(|i| i_to_key(i, 0).into_bytes()),
            "case {}",
            idx
        );
    }
}

#[test]
fn test_get_evenly_split_keys() {
    init_logger();
    test_get_evenly_split_keys_impl(false);
    test_get_evenly_split_keys_impl(true);
}

fn test_get_evenly_split_keys_impl(enable_inner_key_off: bool) {
    let (engine, _) = new_test_engine_opt(enable_inner_key_off, 10);
    let shard = engine.get_shard(1).unwrap();

    let range = ShardRange::new(
        &i_to_key(100, 0).into_bytes(),
        &i_to_key(200, 0).into_bytes(),
        shard.inner_key_off,
    );
    let shard = Shard::new(
        shard.engine_id,
        &shard.properties.to_pb(shard.id),
        shard.ver,
        range.clone(),
        engine.opts.clone(),
        &engine.master_key,
    );

    let cases: Vec<(
        Vec<(i32, i32)>,  // table ranges
        usize,            // split count
        Option<Vec<i32>>, // expected evenly split keys,
    )> = vec![
        (vec![(0, 20), (20, 50), (50, 100)], 1, None),
        (vec![(0, 20), (20, 50), (50, 100)], 2, None),
        (
            vec![(0, 20), (20, 50), (100, 150), (150, 180)],
            2,
            Some(vec![150]),
        ),
        (
            vec![
                (20, 50),
                (100, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
                (300, 400),
            ],
            1,
            None,
        ),
        (
            vec![
                (20, 50),
                (100, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
                (300, 400),
            ],
            2,
            Some(vec![180]),
        ),
        (
            vec![
                (20, 50),
                (100, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
                (300, 400),
            ],
            3,
            Some(vec![150, 180]),
        ),
        (
            vec![
                (20, 50),
                (100, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
                (300, 400),
            ],
            4,
            Some(vec![125, 150, 180]),
        ),
        (
            vec![
                (20, 50),
                (100, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
                (300, 400),
            ],
            5,
            Some(vec![125, 150, 165, 180]),
        ),
        (
            vec![
                (20, 50),
                (100, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
                (300, 400),
            ],
            6,
            Some(vec![125, 150, 165, 180, 190]),
        ),
    ];

    for (idx, (table_ranges, split_count, expect_split_keys)) in cases.into_iter().enumerate() {
        let mut cf_builder = ShardCfBuilder::new(0);
        let mut saved_vals: Vec<Rc<String>> = Vec::new();

        for (i, (start, end)) in table_ranges.into_iter().enumerate() {
            cf_builder.add_table(
                new_table(
                    &engine,
                    i as u64 + 1,
                    start as usize,
                    end as usize,
                    100 + i as u64,
                    false,
                    &mut saved_vals,
                ),
                1, // level
            );
        }

        let data = ShardData::new(
            range.clone(),
            vec![CfTable::new()],
            vec![],
            Arc::new(HashMap::default()),
            [cf_builder.build(), ShardCf::new(1), ShardCf::new(2)],
            HashMap::new(),
        );
        shard.set_data(data);

        let split_keys = shard.get_evenly_split_keys(split_count);
        assert_eq!(
            split_keys.map(|keys| keys.into_iter().map(|k| k.to_vec()).collect::<Vec<_>>()),
            expect_split_keys.map(|keys| keys
                .into_iter()
                .map(|i| i_to_key(i, 0).into_bytes())
                .collect::<Vec<_>>()),
            "case {}",
            idx
        );
    }
}

#[test]
fn test_overlap_ingest_files() {
    init_logger();
    let (engine, _) = new_test_engine();
    let shard = engine.get_shard(1).unwrap();
    let cf = WRITE_CF;

    let mut cf_builder = ShardCfBuilder::new(cf);
    let mut saved_vals: Vec<Rc<String>> = Vec::new();

    let tables = vec![
        (10, 10, 20), // file_id, begin, end (exclusive)
        (20, 20, 30),
        (40, 40, 50),
    ];
    tables.into_iter().for_each(|(id, begin, end)| {
        cf_builder.add_table(
            new_table(&engine, id, begin, end, 100, false, &mut saved_vals),
            3, // ingest to level 3 only.
        )
    });
    let data = ShardData::new(
        shard.range.clone(),
        vec![CfTable::new()],
        vec![],
        Arc::new(HashMap::default()),
        [cf_builder.build(), ShardCf::new(1), ShardCf::new(2)],
        HashMap::new(),
    );
    shard.set_data(data);

    let cases = vec![
        // overlapped cases.
        (vec![(1, 9, 9)], false), // Vec<(file_id, smallest, biggest)>, is_overlap
        (vec![(1, 9, 10)], true),
        (vec![(1, 9, 20)], true),
        (vec![(1, 9, 30)], true),
        (vec![(1, 9, 40)], true),
        (vec![(1, 9, 50)], true),
        (vec![(1, 9, 99)], true),
        (vec![(1, 10, 10)], true),
        (vec![(1, 10, 20)], true),
        (vec![(1, 10, 30)], true),
        (vec![(1, 10, 40)], true),
        (vec![(1, 10, 50)], true),
        (vec![(1, 20, 20)], true),
        (vec![(1, 20, 30)], true),
        (vec![(1, 20, 40)], true),
        (vec![(1, 20, 50)], true),
        (vec![(1, 30, 30)], false),
        (vec![(1, 30, 40)], true),
        (vec![(1, 30, 50)], true),
        (vec![(1, 40, 40)], true),
        (vec![(1, 40, 50)], true),
        (vec![(1, 50, 50)], false),
        // no overlapped cases.
        (
            vec![
                (1, 1, 9),
                (10, 10, 19),
                (20, 20, 29),
                (30, 30, 39),
                (40, 40, 49),
                (50, 50, 99),
            ],
            false,
        ),
    ];

    let snap = SnapAccess::new(&shard);
    for (idx, (tbls, is_overlap)) in cases.into_iter().enumerate() {
        let mut ingest_files = pb::IngestFiles::default();
        for tbl in &tbls {
            let tbl_create = pb::TableCreate {
                id: tbl.0,
                level: 3,
                cf: cf as i32,
                smallest: i_to_key(tbl.1, 0).into_bytes(),
                biggest: i_to_key(tbl.2, 0).into_bytes(),
                ..Default::default()
            };
            ingest_files.mut_table_creates().push(tbl_create);
        }
        assert_eq!(
            snap.overlap_ingest_files(&ingest_files),
            is_overlap,
            "case {}: {:?}",
            idx,
            tbls
        );
    }
}

#[derive(Clone)]
struct TestMetaChangeListener {
    sender: mpsc::Sender<pb::ChangeSet>,
}

impl MetaChangeListener for TestMetaChangeListener {
    fn on_change_set(&self, cs: pb::ChangeSet) {
        info!("on meta change listener");
        self.sender.send(cs).unwrap();
    }
}

#[derive(Clone)]
struct EngineTester {
    core: Arc<EngineTesterCore>,
}

impl Deref for EngineTester {
    type Target = EngineTesterCore;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl EngineTester {
    fn new(enable_inner_key_off: bool, block_size: usize) -> Self {
        let initial_cs = new_initial_cs();
        let initial_meta = ShardMeta::new(1, &initial_cs);
        let metas = dashmap::DashMap::new();
        metas.insert(1, Arc::new(initial_meta));
        let tmp_dir = TempDir::new().unwrap();
        let opts = new_test_options(tmp_dir.path(), enable_inner_key_off, block_size);
        let config = KvEngineConfig::default();

        Self {
            core: Arc::new(EngineTesterCore {
                _tmp_dir: tmp_dir,
                metas,
                fs: Arc::new(InMemFs::new()),
                opts: Arc::new(opts),
                config,
                id: AtomicU64::new(0),
            }),
        }
    }
}

struct EngineTesterCore {
    _tmp_dir: TempDir,
    metas: dashmap::DashMap<u64, Arc<ShardMeta>>,
    fs: Arc<dfs::InMemFs>,
    opts: Arc<Options>,
    config: KvEngineConfig,
    id: AtomicU64,
}

impl MetaIterator for EngineTester {
    fn iterate<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(kvenginepb::ChangeSet),
    {
        for meta in &self.metas {
            f(meta.value().to_change_set())
        }
        Ok(())
    }

    fn engine_id(&self) -> u64 {
        1
    }
}

impl RecoverHandler for EngineTester {
    fn recover(&self, _engine: &Engine, _shard: &Arc<Shard>, _info: &ShardMeta) -> Result<()> {
        Ok(())
    }
}

impl IdAllocator for EngineTesterCore {
    fn alloc_id(&self, count: usize) -> Result<Vec<u64>> {
        let start_id = self
            .id
            .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let end_id = start_id + count as u64;
        let mut ids = Vec::with_capacity(count);
        for id in start_id..end_id {
            ids.push(id);
        }
        Ok(ids)
    }
}

struct MetaListener {
    applier_tx: mpsc::Sender<ApplyTask>,
    meta_rx: mpsc::Receiver<pb::ChangeSet>,
}

impl MetaListener {
    fn new(meta_rx: mpsc::Receiver<pb::ChangeSet>, applier_tx: mpsc::Sender<ApplyTask>) -> Self {
        Self {
            meta_rx,
            applier_tx,
        }
    }

    fn run(&self) {
        loop {
            let cs = unwrap_or_return!(self.meta_rx.recv(), "meta_listener_a");
            let (tx, rx) = mpsc::bounded(1);
            let task = ApplyTask::new_cs(cs, tx);
            self.applier_tx.send(task).unwrap();
            let res = unwrap_or_return!(rx.recv(), "meta_listener_b");
            unwrap_or_return!(res, "meta_listener_c");
        }
    }
}

struct Applier {
    engine: Engine,
    task_rx: mpsc::Receiver<ApplyTask>,
}

impl Applier {
    fn new(engine: Engine, task_rx: mpsc::Receiver<ApplyTask>) -> Self {
        Self { engine, task_rx }
    }

    fn run(&self) {
        let mut seq = 2;
        loop {
            let mut task = unwrap_or_return!(self.task_rx.recv(), "apply recv task");
            seq += 1;
            if let Some(wb) = task.wb.as_mut() {
                wb.set_sequence(seq);
                self.engine.write(wb);
            }
            if let Some(mut cs) = task.cs.take() {
                cs.set_sequence(seq);
                if cs.has_split() {
                    let mut ids = vec![];
                    for new_shard in cs.get_split().get_new_shards() {
                        ids.push(new_shard.shard_id);
                    }
                    unwrap_or_return!(self.engine.split(cs, 1), "apply split");
                    for id in ids {
                        let shard = self.engine.get_shard(id).unwrap();
                        shard.set_active(true);
                    }
                    info!("applier executed split");
                } else {
                    self.engine.meta_committed(&cs, false);
                    unwrap_or_return!(
                        self.engine.apply_change_set(
                            self.engine.prepare_change_set(cs, false, None).unwrap()
                        ),
                        "applier apply changeset"
                    );
                }
            }
            task.result_tx.send(Ok(())).unwrap();
        }
    }
}

struct ApplyTask {
    wb: Option<WriteBatch>,
    cs: Option<pb::ChangeSet>,
    result_tx: mpsc::Sender<Result<()>>,
}

impl ApplyTask {
    fn new_cs(cs: pb::ChangeSet, result_tx: mpsc::Sender<Result<()>>) -> Self {
        Self {
            wb: None,
            cs: Some(cs),
            result_tx,
        }
    }

    fn new_wb(wb: WriteBatch, result_tx: mpsc::Sender<Result<()>>) -> Self {
        Self {
            wb: Some(wb),
            cs: None,
            result_tx,
        }
    }
}

struct Splitter {
    apply_sender: mpsc::Sender<ApplyTask>,
    keys: Vec<Vec<u8>>,
    shard_ver: u64,
    new_id: u64,
}

#[allow(dead_code)]
impl Splitter {
    fn new(keys: Vec<Vec<u8>>, apply_sender: mpsc::Sender<ApplyTask>) -> Self {
        Self {
            keys,
            apply_sender,
            shard_ver: 1,
            new_id: 1,
        }
    }

    fn run(&mut self) {
        let keys = self.keys.clone();
        for key in keys {
            thread::sleep(Duration::from_millis(200));
            self.new_id += 1;
            self.split(key.clone(), vec![self.new_id, 1]);
        }
    }

    fn send_task(&mut self, cs: pb::ChangeSet) {
        let (tx, rx) = mpsc::bounded(1);
        let task = ApplyTask {
            cs: Some(cs),
            wb: None,
            result_tx: tx,
        };
        self.apply_sender.send(task).unwrap();
        let res = unwrap_or_return!(rx.recv(), "splitter recv");
        res.unwrap();
    }

    fn split(&mut self, key: Vec<u8>, new_ids: Vec<u64>) {
        let mut cs = pb::ChangeSet::new();
        cs.set_shard_id(1);
        cs.set_shard_ver(self.shard_ver);
        let mut finish_split = pb::Split::new();
        finish_split.set_keys(protobuf::RepeatedField::from_vec(vec![key.clone()]));
        let mut new_shards = Vec::new();
        for new_id in &new_ids {
            let mut new_shard = pb::Properties::new();
            new_shard.set_shard_id(*new_id);
            new_shards.push(new_shard);
        }
        finish_split.set_new_shards(protobuf::RepeatedField::from_vec(new_shards));
        cs.set_split(finish_split);
        self.send_task(cs);
        info!(
            "splitter sent split task to applier, ids {:?} key {}",
            new_ids,
            String::from_utf8_lossy(key.as_slice())
        );
        self.shard_ver += 1;
    }
}

fn new_initial_cs() -> pb::ChangeSet {
    let mut cs = pb::ChangeSet::new();
    cs.set_shard_id(1);
    cs.set_shard_ver(1);
    cs.set_sequence(1);
    let mut snap = pb::Snapshot::new();
    snap.set_base_version(1);
    snap.set_outer_end(GLOBAL_SHARD_END_KEY.to_vec());
    let props = snap.mut_properties();
    props.shard_id = 1;
    cs.set_snapshot(snap);
    cs
}

fn new_test_options(
    path: impl AsRef<Path>,
    enable_inner_key_off: bool,
    block_size: usize,
) -> Options {
    let min_blob_size: u32 = match env::var("MIN_BLOB_SIZE") {
        Ok(val) => match val.trim().parse() {
            Ok(n) => n,
            Err(e) => {
                warn!("MIN_BLOB_SIZE=<number>, got {}", e);
                1024
            }
        },
        Err(_) => 1024,
    };
    info!("MIN_BLOB_SIZE={}", min_blob_size);
    let mut opts = Options::default();
    opts.local_dir = path.as_ref().to_path_buf();
    opts.base_size = 64 << 10;
    opts.table_builder_options.block_size = block_size;
    opts.table_builder_options.max_table_size = 16 << 10;
    opts.max_mem_table_size = 16 << 10;
    opts.num_compactors = 2;
    opts.blob_table_build_options.min_blob_size = min_blob_size;
    opts.max_del_range_delay = Duration::from_secs(1);
    opts.enable_inner_key_offset = enable_inner_key_off;
    opts
}

fn i_to_key(i: i32, min_blob_size: u32) -> String {
    if min_blob_size > 0 {
        // 3 -> strlen("key")
        format!("key{:0>1$}", i, min_blob_size as usize - 3)
    } else {
        format!("key{:0>1$}", i, 6)
    }
}

fn new_table(
    engine: &Engine,
    id: u64,
    begin: usize,
    end: usize,
    version: u64,
    del: bool,
    saved_vals: &mut Vec<Rc<String>>,
) -> SsTable {
    let block_size = engine.opts.table_builder_options.block_size;
    let comp_tp = engine.opts.table_builder_options.compression_tps[0];
    let comp_lvl = engine.opts.table_builder_options.compression_lvl;
    let fs = engine.fs.clone();

    let mut builder =
        table::sstable::builder::Builder::new(id, block_size, comp_tp, comp_lvl, None);
    for i in begin..end {
        let key = i_to_key(i as i32, 0);
        let val = if del {
            table::Value::new_with_meta_version(BIT_DELETE, version, 0, &[])
        } else {
            let val = Rc::new(key.repeat(2));
            // Save the val with Rc, make sure the val_rc stay valid during the iterator
            // liftcycle.
            saved_vals.push(val.clone());
            table::Value::new_with_meta_version(0, version, 0, val.as_bytes())
        };
        builder.add(InnerKey::from_inner_buf(key.as_bytes()), &val, None);
    }
    let mut data_buf = Vec::new();
    builder.finish(0, &mut data_buf);
    let data = Bytes::from(data_buf);
    let opts = dfs::Options::new(1, 1);
    let runtime = fs.get_runtime();
    runtime.block_on(fs.create(id, data.clone(), opts)).unwrap();
    let file = InMemFile::new(id, data);
    SsTable::new(Arc::new(file), None, true, None).unwrap()
}

fn load_data(
    begin: usize,
    end: usize,
    version: u64,
    tx: mpsc::Sender<ApplyTask>,
    min_blob_size: u32,
) {
    let mut wb = WriteBatch::new(1, 0);
    for i in begin..end {
        let key = i_to_key(i as i32, min_blob_size);
        for cf in 0..3 {
            let val = key.repeat(cf + 2);
            let version = if cf == 1 { 0 } else { version };
            wb.put(cf, key.as_bytes(), val.as_bytes(), 0, &[], version);
        }
        if i % 100 == 99 {
            info!("load data {}:{}", i - 99, i);
            write_data(wb, &tx);
            wb = WriteBatch::new(1, 0);
            thread::sleep(Duration::from_millis(10));
        }
    }
    if wb.num_entries() > 0 {
        write_data(wb, &tx);
    }
}

fn write_data(wb: WriteBatch, applier_tx: &mpsc::Sender<ApplyTask>) {
    let (result_tx, result_rx) = mpsc::bounded(1);
    let task = ApplyTask::new_wb(wb, result_tx);
    if let Err(err) = applier_tx.send(task) {
        panic!("{:?}", err);
    }
    result_rx.recv().unwrap().unwrap();
}

fn check_get(
    begin: usize,
    end: usize,
    version: u64,
    cfs: &[usize],
    en: &Engine,
    exist: bool,
    check_version: Option<u64>,
    min_blob_size: u32,
) {
    for i in begin..end {
        let key = i_to_key(i as i32, min_blob_size);
        let shard = get_shard_for_key(key.as_bytes(), en);
        let snap = SnapAccess::new(&shard);
        for &cf in cfs {
            let version = if cf == 1 { 0 } else { version };
            let item = snap.get(cf, key.as_bytes(), version);
            if item.is_valid() {
                if !exist {
                    if item.is_deleted() {
                        continue;
                    }
                    let shard_stats = shard.get_stats();
                    panic!(
                        "got key {}, shard {}:{}, cf {}, stats {:?}",
                        key, shard.id, shard.ver, cf, shard_stats,
                    );
                }
                assert_eq!(item.get_value(), key.repeat(cf + 2).as_bytes());
                if cf != 1 && check_version.is_some() {
                    assert_eq!(item.version, check_version.unwrap());
                }
            } else if exist {
                let shard_stats = shard.get_stats();
                panic!(
                    "failed to get key {}, shard {}, stats {:?}",
                    key,
                    shard.tag(),
                    shard_stats,
                );
            }
        }
    }
}

fn check_iterater(begin: usize, end: usize, en: &Engine) {
    thread::sleep(Duration::from_secs(1));
    for cf in 0..3 {
        let mut i = begin;
        // let ids = vec![2, 3, 4, 5, 1];
        let ids = vec![1];
        for id in ids {
            let shard = en.get_shard(id).unwrap();
            let snap = SnapAccess::new(&shard);
            let mut iter = snap.new_iterator(cf, false, false, None, true);
            iter.seek(shard.outer_start.chunk());
            while iter.valid() {
                if iter.key.chunk() >= shard.outer_end.chunk() {
                    break;
                }
                let key = i_to_key(i as i32, en.opts.blob_table_build_options.min_blob_size);
                assert_eq!(iter.key(), key.as_bytes());
                assert_eq!(iter.val(), key.repeat(cf + 2).as_bytes());
                i += 1;
                iter.next();
            }
        }
        assert_eq!(i, end);
    }
}

fn get_shard_for_key(key: &[u8], en: &Engine) -> Arc<Shard> {
    for id in 1_u64..=5 {
        if let Some(shard) = en.get_shard(id) {
            if shard.overlap_key(InnerKey::from_inner_buf(key)) {
                return shard;
            }
        }
    }
    en.get_shard(1).unwrap()
}

pub(crate) fn init_logger() {
    use slog::Drain;
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build();
    let drain = std::sync::Mutex::new(drain).fuse();
    let logger = slog::Logger::root(drain, o!());
    slog_global::set_global(logger);
}

fn try_wait<F>(f: F, seconds: usize) -> bool
where
    F: Fn() -> bool,
{
    let begin = Instant::now_coarse();
    let timeout = Duration::from_secs(seconds as u64);
    while begin.saturating_elapsed() < timeout {
        if f() {
            return true;
        }
        thread::sleep(Duration::from_millis(100))
    }
    false
}
