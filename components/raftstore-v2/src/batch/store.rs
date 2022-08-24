// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    mem,
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use batch_system::{
    BasicMailbox, BatchRouter, BatchSystem, HandleResult, HandlerBuilder, PollHandler,
};
use collections::HashMap;
use crossbeam::channel::Sender;
use engine_traits::{Engines, KvEngine, RaftEngine, TabletFactory};
use futures::{compat::Future01CompatExt, FutureExt};
use kvproto::{metapb::Store, raft_serverpb::PeerState};
use raft::INVALID_ID;
use raftstore::store::{
    fsm::store::PeerTickBatch, local_metrics::RaftMetrics, Config, RaftlogFetchRunner,
    RaftlogFetchTask, StoreWriters, Transport, WriteMsg, WriteSenders,
};
use slog::Logger;
use tikv_util::{
    box_err,
    config::{Tracker, VersionTrack},
    future::poll_future_notify,
    time::Instant as TiInstant,
    timer::SteadyTimer,
    worker::{Scheduler, Worker},
};

use super::apply::{create_apply_batch_system, ApplyPollerBuilder, ApplyRouter, ApplySystem};
use crate::{
    fsm::{PeerFsm, PeerFsmDelegate, SenderFsmPair, StoreFsm, StoreFsmDelegate},
    raft::Peer,
    router::{PeerMsg, PeerTick, StoreMsg},
    Error, Result,
};

/// A per-thread context shared by the [`StoreFsm`] and multiple [`PeerFsm`]s.
pub struct StoreContext<EK: KvEngine, ER: RaftEngine, T> {
    /// A logger without any KV. It's clean for creating new PeerFSM.
    pub logger: Logger,
    /// The transport for sending messages to peers on other stores.
    pub trans: T,
    pub has_ready: bool,
    pub raft_metrics: RaftMetrics,
    /// The latest configuration.
    pub cfg: Config,
    pub router: StoreRouter<EK, ER>,
    /// The tick batch for delay ticking. It will be flushed at the end of every
    /// round.
    pub tick_batch: Vec<PeerTickBatch>,
    /// The precise timer for scheduling tick.
    pub timer: SteadyTimer,
    pub write_senders: WriteSenders<EK, ER>,
}

impl<EK: KvEngine, ER: RaftEngine, T> StoreContext<EK, ER, T> {
    fn new(
        cfg: Config,
        trans: T,
        router: StoreRouter<EK, ER>,
        write_senders: WriteSenders<EK, ER>,
        logger: Logger,
    ) -> Self {
        Self {
            logger,
            trans,
            has_ready: false,
            raft_metrics: RaftMetrics::new(cfg.waterfall_metrics),
            cfg,
            router,
            tick_batch: vec![PeerTickBatch::default(); PeerTick::VARIANT_COUNT],
            timer: SteadyTimer::default(),
            write_senders,
        }
    }
}

/// A [`PollHandler`] that handles updates of [`StoreFsm`]s and [`PeerFsm`]s.
///
/// It is responsible for:
///
/// - Keeping the local [`StoreContext`] up-to-date.
/// - Receiving and sending messages in and out of these FSMs.
struct StorePoller<EK: KvEngine, ER: RaftEngine, T> {
    poll_ctx: StoreContext<EK, ER, T>,
    cfg_tracker: Tracker<Config>,
    /// Buffers to hold in-coming messages.
    store_msg_buf: Vec<StoreMsg>,
    peer_msg_buf: Vec<PeerMsg>,
    /// These fields controls the timing of flushing messages generated by
    /// FSMs.
    last_flush_time: TiInstant,
    need_flush_events: bool,
}

impl<EK: KvEngine, ER: RaftEngine, T> StorePoller<EK, ER, T> {
    pub fn new(poll_ctx: StoreContext<EK, ER, T>, cfg_tracker: Tracker<Config>) -> Self {
        Self {
            poll_ctx,
            cfg_tracker,
            store_msg_buf: Vec::new(),
            peer_msg_buf: Vec::new(),
            last_flush_time: TiInstant::now(),
            need_flush_events: false,
        }
    }

    /// Updates the internal buffer to match the latest configuration.
    fn apply_buf_capacity(&mut self) {
        let new_cap = self.messages_per_tick();
        tikv_util::set_vec_capacity(&mut self.store_msg_buf, new_cap);
        tikv_util::set_vec_capacity(&mut self.peer_msg_buf, new_cap);
    }

    #[inline]
    fn messages_per_tick(&self) -> usize {
        self.poll_ctx.cfg.messages_per_tick
    }

    fn flush_events(&mut self) {
        self.schedule_ticks();
    }

    fn schedule_ticks(&mut self) {
        assert_eq!(PeerTick::all_ticks().len(), self.poll_ctx.tick_batch.len());
        for batch in &mut self.poll_ctx.tick_batch {
            batch.schedule(&self.poll_ctx.timer);
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine, T: Transport + 'static> PollHandler<PeerFsm<EK, ER>, StoreFsm>
    for StorePoller<EK, ER, T>
{
    fn begin<F>(&mut self, _batch_size: usize, update_cfg: F)
    where
        for<'a> F: FnOnce(&'a batch_system::Config),
    {
        if self.store_msg_buf.capacity() == 0 || self.peer_msg_buf.capacity() == 0 {
            self.apply_buf_capacity();
        }
        // Apply configuration changes.
        if let Some(cfg) = self.cfg_tracker.any_new().map(|c| c.clone()) {
            let last_messages_per_tick = self.messages_per_tick();
            self.poll_ctx.cfg = cfg;
            if self.poll_ctx.cfg.messages_per_tick != last_messages_per_tick {
                self.apply_buf_capacity();
            }
            update_cfg(&self.poll_ctx.cfg.store_batch_system);
        }
    }

    fn handle_control(&mut self, fsm: &mut StoreFsm) -> Option<usize> {
        debug_assert!(self.store_msg_buf.is_empty());
        let batch_size = self.messages_per_tick();
        let received_cnt = fsm.recv(&mut self.store_msg_buf, batch_size);
        let expected_msg_count = if received_cnt == batch_size {
            None
        } else {
            Some(0)
        };
        let mut delegate = StoreFsmDelegate::new(fsm, &mut self.poll_ctx);
        delegate.handle_msgs(&mut self.store_msg_buf);
        expected_msg_count
    }

    fn handle_normal(&mut self, fsm: &mut impl DerefMut<Target = PeerFsm<EK, ER>>) -> HandleResult {
        debug_assert!(self.peer_msg_buf.is_empty());
        let batch_size = self.messages_per_tick();
        let received_cnt = fsm.recv(&mut self.peer_msg_buf, batch_size);
        let handle_result = if received_cnt == batch_size {
            HandleResult::KeepProcessing
        } else {
            HandleResult::stop_at(0, false)
        };
        let mut delegate = PeerFsmDelegate::new(fsm, &mut self.poll_ctx);
        delegate.on_msgs(&mut self.peer_msg_buf);
        delegate
            .fsm
            .peer_mut()
            .handle_raft_ready(delegate.store_ctx);
        handle_result
    }

    fn light_end(&mut self, _batch: &mut [Option<impl DerefMut<Target = PeerFsm<EK, ER>>>]) {
        if self.poll_ctx.trans.need_flush() {
            self.poll_ctx.trans.flush();
        }

        let now = TiInstant::now();
        if now.saturating_duration_since(self.last_flush_time) >= Duration::from_millis(1) {
            self.last_flush_time = now;
            self.need_flush_events = false;
            self.flush_events();
        } else {
            self.need_flush_events = true;
        }
    }

    fn end(&mut self, batch: &mut [Option<impl DerefMut<Target = PeerFsm<EK, ER>>>]) {}

    fn pause(&mut self) {
        if self.poll_ctx.trans.need_flush() {
            self.poll_ctx.trans.flush();
        }

        if self.need_flush_events {
            self.last_flush_time = TiInstant::now();
            self.need_flush_events = false;
            self.flush_events();
        }
    }
}

struct StorePollerBuilder<EK: KvEngine, ER: RaftEngine, T> {
    cfg: Arc<VersionTrack<Config>>,
    store_id: u64,
    engine: ER,
    tablet_factory: Arc<dyn TabletFactory<EK>>,
    trans: T,
    router: StoreRouter<EK, ER>,
    log_fetch_scheduler: Scheduler<RaftlogFetchTask>,
    write_senders: WriteSenders<EK, ER>,
    logger: Logger,
}

impl<EK: KvEngine, ER: RaftEngine, T> StorePollerBuilder<EK, ER, T> {
    pub fn new(
        cfg: Arc<VersionTrack<Config>>,
        store_id: u64,
        engine: ER,
        tablet_factory: Arc<dyn TabletFactory<EK>>,
        trans: T,
        router: StoreRouter<EK, ER>,
        log_fetch_scheduler: Scheduler<RaftlogFetchTask>,
        store_writers: &mut StoreWriters<EK, ER>,
        logger: Logger,
    ) -> Self {
        StorePollerBuilder {
            cfg,
            store_id,
            engine,
            tablet_factory,
            trans,
            router,
            log_fetch_scheduler,
            logger,
            write_senders: store_writers.senders(),
        }
    }

    /// Initializes all the existing raft machines and cleans up stale tablets.
    fn init(&self) -> Result<HashMap<u64, SenderFsmPair<EK, ER>>> {
        let mut regions = HashMap::default();
        let cfg = self.cfg.value();
        self.engine
            .for_each_raft_group::<Error, _>(&mut |region_id| {
                assert_ne!(region_id, INVALID_ID);
                let peer = match Peer::new(
                    &cfg,
                    region_id,
                    self.store_id,
                    self.tablet_factory.as_ref(),
                    self.engine.clone(),
                    self.log_fetch_scheduler.clone(),
                    &self.logger,
                )? {
                    Some(peer) => peer,
                    None => return Ok(()),
                };
                let pair = PeerFsm::new(&cfg, peer)?;
                let prev = regions.insert(region_id, pair);
                if let Some((_, p)) = prev {
                    return Err(box_err!(
                        "duplicate region {:?} vs {:?}",
                        p.logger().list(),
                        regions[&region_id].1.logger().list()
                    ));
                }
                Ok(())
            })?;
        self.clean_up_tablets(&regions)?;
        Ok(regions)
    }

    fn clean_up_tablets(&self, peers: &HashMap<u64, SenderFsmPair<EK, ER>>) -> Result<()> {
        // TODO: list all available tablets and destroy those which are not in the
        // peers.
        Ok(())
    }
}

impl<EK, ER, T> HandlerBuilder<PeerFsm<EK, ER>, StoreFsm> for StorePollerBuilder<EK, ER, T>
where
    ER: RaftEngine,
    EK: KvEngine,
    T: Transport + 'static,
{
    type Handler = StorePoller<EK, ER, T>;

    fn build(&mut self, priority: batch_system::Priority) -> Self::Handler {
        let poll_ctx = StoreContext::new(
            self.cfg.value().clone(),
            self.trans.clone(),
            self.router.clone(),
            self.write_senders.clone(),
            self.logger.clone(),
        );
        let cfg_tracker = self.cfg.clone().tracker("raftstore".to_string());
        StorePoller::new(poll_ctx, cfg_tracker)
    }
}

/// A set of background threads that will processing offloaded work from
/// raftstore.
struct Workers<EK: KvEngine, ER: RaftEngine> {
    /// Worker for fetching raft logs asynchronously
    log_fetch_worker: Worker,
    store_writers: StoreWriters<EK, ER>,
}

impl<EK: KvEngine, ER: RaftEngine> Default for Workers<EK, ER> {
    fn default() -> Self {
        Self {
            log_fetch_worker: Worker::new("raftlog-fetch-worker"),
            store_writers: StoreWriters::default(),
        }
    }
}

/// The system used for polling Raft activities.
pub struct StoreSystem<EK: KvEngine, ER: RaftEngine> {
    system: BatchSystem<PeerFsm<EK, ER>, StoreFsm>,
    apply_router: ApplyRouter<EK>,
    apply_system: ApplySystem<EK>,
    workers: Option<Workers<EK, ER>>,
    logger: Logger,
}

impl<EK: KvEngine, ER: RaftEngine> StoreSystem<EK, ER> {
    pub fn start<T>(
        &mut self,
        store: Store,
        cfg: Arc<VersionTrack<Config>>,
        raft_engine: ER,
        tablet_factory: Arc<dyn TabletFactory<EK>>,
        trans: T,
        router: &StoreRouter<EK, ER>,
    ) -> Result<()>
    where
        T: Transport + 'static,
    {
        let mut workers = Workers::default();
        workers.store_writers.spawn(
            store.get_id(),
            raft_engine.clone(),
            None,
            router,
            &trans,
            &cfg,
        )?;
        let log_fetch_scheduler = workers.log_fetch_worker.start(
            "raftlog-fetch-worker",
            RaftlogFetchRunner::new(router.clone(), raft_engine.clone()),
        );

        let mut builder = StorePollerBuilder::new(
            cfg.clone(),
            store.get_id(),
            raft_engine,
            tablet_factory,
            trans,
            router.clone(),
            log_fetch_scheduler,
            &mut workers.store_writers,
            self.logger.clone(),
        );
        self.workers = Some(workers);
        let peers = builder.init()?;
        self.apply_system
            .schedule_all(peers.values().map(|pair| pair.1.peer()));
        // Choose a different name so we know what version is actually used. rs stands
        // for raft store.
        let tag = format!("rs-{}", store.get_id());
        self.system.spawn(tag, builder);

        let mut mailboxes = Vec::with_capacity(peers.len());
        let mut address = Vec::with_capacity(peers.len());
        for (region_id, (tx, fsm)) in peers {
            address.push(region_id);
            mailboxes.push((
                region_id,
                BasicMailbox::new(tx, fsm, router.state_cnt().clone()),
            ));
        }
        router.register_all(mailboxes);

        // Make sure Msg::Start is the first message each FSM received.
        for addr in address {
            router.force_send(addr, PeerMsg::Start).unwrap();
        }
        router.send_control(StoreMsg::Start { store }).unwrap();

        let apply_poller_builder = ApplyPollerBuilder::new(cfg);
        self.apply_system
            .spawn("apply".to_owned(), apply_poller_builder);
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if self.workers.is_none() {
            return;
        }
        let mut workers = self.workers.take().unwrap();

        self.apply_system.shutdown();
        self.system.shutdown();

        workers.store_writers.shutdown();
        workers.log_fetch_worker.stop();
    }
}

#[derive(Clone)]
pub struct StoreRouter<EK: KvEngine, ER: RaftEngine> {
    router: BatchRouter<PeerFsm<EK, ER>, StoreFsm>,
    logger: Logger,
}

impl<EK: KvEngine, ER: RaftEngine> StoreRouter<EK, ER> {
    #[inline]
    pub fn logger(&self) -> &Logger {
        &self.logger
    }
}

impl<EK: KvEngine, ER: RaftEngine> Deref for StoreRouter<EK, ER> {
    type Target = BatchRouter<PeerFsm<EK, ER>, StoreFsm>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.router
    }
}

impl<EK: KvEngine, ER: RaftEngine> DerefMut for StoreRouter<EK, ER> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.router
    }
}

/// Creates the batch system for polling raft activities.
pub fn create_store_batch_system<EK, ER>(
    cfg: &Config,
    store: Store,
    logger: Logger,
) -> (StoreRouter<EK, ER>, StoreSystem<EK, ER>)
where
    EK: KvEngine,
    ER: RaftEngine,
{
    let (store_tx, store_fsm) = StoreFsm::new(cfg, store);
    let (router, system) =
        batch_system::create_system(&cfg.store_batch_system, store_tx, store_fsm);
    let (apply_router, apply_system) = create_apply_batch_system(cfg);
    let system = StoreSystem {
        system,
        apply_router,
        apply_system,
        workers: None,
        logger: logger.clone(),
    };
    (StoreRouter { router, logger }, system)
}
