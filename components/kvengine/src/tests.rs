// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::Deref,
    path::Path,
    sync::{atomic::AtomicU64, Arc},
    thread,
    time::Duration,
    vec,
};

use bytes::Buf;
use file_system::IoRateLimiter;
use kvenginepb as pb;
use tempfile::TempDir;
use tikv_util::{mpsc, time::Instant};

use crate::{dfs::InMemFS, *};

macro_rules! unwrap_or_return {
    ( $e:expr, $m:expr ) => {
        match $e {
            Ok(x) => x,
            Err(y) => {
                error!("{:?} {:?}", y, $m);
                return;
            }
        }
    };
}

fn new_test_engine() -> (Engine, mpsc::Sender<ApplyTask>) {
    let (listener_tx, listener_rx) = mpsc::unbounded();
    let tester = EngineTester::new();
    let meta_change_listener = Box::new(TestMetaChangeListener {
        sender: listener_tx,
    });
    let rate_limiter = Arc::new(IoRateLimiter::new_for_test());
    let engine = Engine::open(
        tester.fs.clone(),
        tester.opts.clone(),
        tester.clone(),
        tester.clone(),
        tester.core.clone(),
        meta_change_listener,
        rate_limiter,
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
    // keys.push(i_to_key(*i));
    // }
    // let mut splitter = Splitter::new(keys.clone(), applier_tx.clone());
    // let handle = thread::spawn(move || {
    // splitter.run();
    // });
    let (begin, end) = (0, 10000);
    load_data(begin, end, 1, applier_tx);
    // handle.join().unwrap();
    check_get(begin, end, 2, &[0, 1, 2], &engine, true, None);
    check_iterater(begin, end, &engine);
}

#[test]
fn test_destroy_range() {
    init_logger();
    let (engine, applier_tx) = new_test_engine();
    load_data(10, 50, 1, applier_tx.clone());
    let mem_table_count = engine.get_shard_stat(1).mem_table_count;
    // Unsafe destroy keys [10, 30).
    for prefix in [10, 20] {
        let mut wb = WriteBatch::new(1);
        let key = i_to_key(prefix);
        wb.set_property(DEL_PREFIXES_KEY, &key[..key.len() - 1]);
        write_data(wb, &applier_tx);
    }
    assert!(
        !engine
            .get_shard(1)
            .unwrap()
            .get_data()
            .del_prefixes
            .is_empty()
    );
    // Memtable is switched because it contains data covered by the delete-prefixes.
    let stats = engine.get_shard_stat(1);
    assert_eq!(
        stats.mem_table_count + stats.l0_table_count,
        mem_table_count + 1
    );
    let wait_for_destroying_range = || {
        for _ in 0..30 {
            if engine
                .get_shard(1)
                .unwrap()
                .get_data()
                .del_prefixes
                .is_empty()
            {
                break;
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
    check_get(10, 30, 2, &[0, 1, 2], &engine, false, None);
    check_get(30, 50, 2, &[0, 1, 2], &engine, true, None);
    check_iterater(30, 50, &engine);

    // Trigger L0 compaction.
    for i in 1..=10 {
        load_data(50 + (i - 1) * 10, 50 + i * 10, 1, applier_tx.clone());
        let mut wb = WriteBatch::new(1);
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
    let mut wb = WriteBatch::new(1);
    let key = i_to_key(100);
    wb.set_property(DEL_PREFIXES_KEY, &key[..key.len() - 2]);
    write_data(wb, &applier_tx);
    wait_for_destroying_range();
    check_get(100, 150, 2, &[0, 1, 2], &engine, false, None);
    check_get(50, 100, 2, &[0, 1, 2], &engine, true, None);

    // Clean all data.
    let mut wb = WriteBatch::new(1);
    wb.set_property(DEL_PREFIXES_KEY, b"key");
    write_data(wb, &applier_tx);
    wait_for_destroying_range();
    check_get(10, 150, 2, &[0, 1, 2], &engine, false, None);

    // No data exists and delete-prefixes can be cleaned too.
    let mut wb = WriteBatch::new(1);
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
    load_data(10, 50, version, applier_tx.clone());
    // truncate ts.
    let mut wb = WriteBatch::new(1);
    let truncate_ts = TruncateTs::from(version + 10);
    wb.set_property(TRUNCATE_TS_KEY, truncate_ts.marshal().as_slice());
    write_data(wb, &applier_tx);
    assert_eq!(
        Some(truncate_ts),
        engine.get_shard(1).unwrap().get_data().truncate_ts
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
    assert!(ret.is_ok());
    assert_eq!(
        Some(truncate_ts),
        engine.get_shard(1).unwrap().get_data().truncate_ts
    );

    // truncated_ts is equal than current truncate ts, remove truncate ts in shard.
    let truncated_ts = TruncateTs::from(version + 10);
    cs.set_sequence(3);
    cs.set_property_value(truncated_ts.marshal().to_vec());
    let ret = engine.apply_change_set(apply::ChangeSet::new(cs));
    assert!(ret.is_ok());
    assert_eq!(None, engine.get_shard(1).unwrap().get_data().truncate_ts);
}

#[test]
fn test_truncate_ts() {
    init_logger();
    let (engine, applier_tx) = new_test_engine();

    let set_truncate_ts = |ts: u64| {
        let mut wb = WriteBatch::new(1);
        let truncate_ts = TruncateTs::from(ts);
        wb.set_property(TRUNCATE_TS_KEY, truncate_ts.marshal().as_slice());
        write_data(wb, &applier_tx);
        assert_eq!(
            Some(truncate_ts),
            engine.get_shard(1).unwrap().get_data().truncate_ts
        );
        assert_eq!(
            truncate_ts.marshal().as_slice(),
            engine
                .get_shard(1)
                .unwrap()
                .get_property(TRUNCATE_TS_KEY)
                .unwrap()
        );
    };

    let wait_for_truncate_ts = || {
        let ok = try_wait(
            || {
                engine
                    .get_shard(1)
                    .unwrap()
                    .get_data()
                    .truncate_ts
                    .is_none()
            },
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

    load_data(0, 300, 1000, applier_tx.clone());
    load_data(100, 400, 2000, applier_tx.clone());
    load_data(200, 500, 3000, applier_tx.clone());

    const ALL_CFS: &[usize] = &[0, 1, 2];
    const WRT_EXT_CFS: &[usize] = &[0, 2];

    {
        // No truncate.
        set_truncate_ts(3000);
        thread::sleep(Duration::from_secs(1));
        wait_for_truncate_ts();
        check_get(0, 100, u64::MAX, ALL_CFS, &engine, true, Some(1000));
        check_get(100, 300, 1000, ALL_CFS, &engine, true, Some(1000));
        check_get(100, 200, u64::MAX, ALL_CFS, &engine, true, Some(2000));
        check_get(200, 400, 2000, ALL_CFS, &engine, true, Some(2000));
        check_get(200, 500, u64::MAX, ALL_CFS, &engine, true, Some(3000));
    }

    for truncate_ts in [2999, 2000] {
        set_truncate_ts(truncate_ts);
        wait_for_truncate_ts();
        check_get(0, 100, u64::MAX, ALL_CFS, &engine, true, Some(1000));
        check_get(100, 300, 1000, ALL_CFS, &engine, true, Some(1000));
        check_get(100, 400, u64::MAX, ALL_CFS, &engine, true, Some(2000));
        check_get(400, 500, u64::MAX, WRT_EXT_CFS, &engine, false, None);
    }

    for truncate_ts in [1999, 1000] {
        set_truncate_ts(truncate_ts as u64);
        wait_for_truncate_ts();
        check_get(0, 300, u64::MAX, ALL_CFS, &engine, true, Some(1000));
        check_get(300, 500, u64::MAX, WRT_EXT_CFS, &engine, false, None);
    }

    {
        // Truncate all.
        set_truncate_ts(999);
        wait_for_truncate_ts();
        check_get(0, 500, u64::MAX, WRT_EXT_CFS, &engine, false, None);
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
    fn new() -> Self {
        let initial_cs = new_initial_cs();
        let initial_meta = ShardMeta::new(1, &initial_cs);
        let metas = dashmap::DashMap::new();
        metas.insert(1, Arc::new(initial_meta));
        let tmp_dir = TempDir::new().unwrap();
        let opts = new_test_options(tmp_dir.path());
        Self {
            core: Arc::new(EngineTesterCore {
                _tmp_dir: tmp_dir,
                metas,
                fs: Arc::new(InMemFS::new()),
                opts: Arc::new(opts),
                id: AtomicU64::new(0),
            }),
        }
    }
}

struct EngineTesterCore {
    _tmp_dir: TempDir,
    metas: dashmap::DashMap<u64, Arc<ShardMeta>>,
    fs: Arc<dfs::InMemFS>,
    opts: Arc<Options>,
    id: AtomicU64,
}

impl MetaIterator for EngineTester {
    fn iterate<F>(&self, mut f: F) -> Result<()>
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

impl IDAllocator for EngineTesterCore {
    fn alloc_id(&self, count: usize) -> Vec<u64> {
        let start_id = self
            .id
            .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let end_id = start_id + count as u64;
        let mut ids = Vec::with_capacity(count);
        for id in start_id..end_id {
            ids.push(id);
        }
        ids
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
                        self.engine
                            .apply_change_set(self.engine.prepare_change_set(cs, false).unwrap()),
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
    snap.set_end(GLOBAL_SHARD_END_KEY.to_vec());
    let props = snap.mut_properties();
    props.shard_id = 1;
    cs.set_snapshot(snap);
    cs
}

fn new_test_options(path: impl AsRef<Path>) -> Options {
    let mut opts = Options::default();
    opts.local_dir = path.as_ref().to_path_buf();
    opts.base_size = 64 << 10;
    opts.table_builder_options.block_size = 4 << 10;
    opts.table_builder_options.max_table_size = 16 << 10;
    opts.max_mem_table_size = 16 << 10;
    opts.num_compactors = 2;
    opts
}

fn i_to_key(i: i32) -> Vec<u8> {
    format!("key{:06}", i).into_bytes()
}

fn load_data(begin: usize, end: usize, version: u64, tx: mpsc::Sender<ApplyTask>) {
    let mut wb = WriteBatch::new(1);
    for i in begin..end {
        let key = format!("key{:06}", i);
        for cf in 0..3 {
            let val = key.repeat(cf + 2);
            let version = if cf == 1 { 0 } else { version };
            wb.put(cf, key.as_bytes(), val.as_bytes(), 0, &[], version);
        }
        if i % 100 == 99 {
            info!("load data {}:{}", i - 99, i);
            write_data(wb, &tx);
            wb = WriteBatch::new(1);
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
) {
    for i in begin..end {
        let key = format!("key{:06}", i);
        let shard = get_shard_for_key(key.as_bytes(), en);
        let snap = SnapAccess::new(&shard);
        for &cf in cfs {
            let version = if cf == 1 { 0 } else { version };
            let item = snap.get(cf, key.as_bytes(), version);
            if item.is_valid() {
                if !exist {
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
            iter.seek(shard.start.chunk());
            while iter.valid() {
                if iter.key.chunk() >= shard.end.chunk() {
                    break;
                }
                let key = format!("key{:06}", i);
                assert_eq!(iter.key(), key.as_bytes());
                let item = iter.item();
                assert_eq!(item.get_value(), key.repeat(cf + 2).as_bytes());
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
            if shard.overlap_key(key) {
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
