// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module startups all the components of a TiKV server.
//!
//! It is responsible for reading from configs, starting up the various server
//! components, and handling errors (mostly by aborting and reporting to the
//! user).
//!
//! The entry point is `run_tikv`.
//!
//! Components are often used to initialize other components, and/or must be
//! explicitly stopped. We keep these components in the `TikvServer` struct.

use std::{
    cmp,
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        mpsc, Arc,
    },
    time::Duration,
    u64,
};

use api_version::{dispatch_api_version, KvFormat};
use causal_ts::CausalTsProviderImpl;
use concurrency_manager::ConcurrencyManager;
use encryption_export::DataKeyManager;
use engine_rocks::{
    flush_engine_statistics, from_rocks_compression_type,
    raw::{Cache, Env},
    RocksEngine, RocksStatistics,
};
use engine_traits::{
    CachedTablet, CfOptions, CfOptionsExt, Engines, FlowControlFactorsExt, KvEngine, MiscExt,
    RaftEngine, StatisticsReporter, TabletRegistry, CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use file_system::{
    get_io_rate_limiter, BytesFetcher, IoBudgetAdjustor, MetricsManager as IoMetricsManager,
};
use futures::executor::block_on;
use grpcio::{EnvBuilder, Environment};
use grpcio_health::HealthService;
use kvproto::{
    brpb::create_backup, deadlock::create_deadlock, diagnosticspb::create_diagnostics,
    import_sstpb_grpc::create_import_sst, kvrpcpb::ApiVersion,
    resource_usage_agent::create_resource_metering_pub_sub,
};
use pd_client::{PdClient, RpcClient};
use raft_log_engine::RaftLogEngine;
use raftstore::{
    coprocessor::{
        BoxConsistencyCheckObserver, ConsistencyCheckMethod, CoprocessorHost,
        RawConsistencyCheckObserver,
    },
    store::{
        memory::MEMTRACE_ROOT as MEMTRACE_RAFTSTORE, AutoSplitController, CheckLeaderRunner,
        SplitConfigManager, TabletSnapManager,
    },
    RegionInfoAccessor,
};
use raftstore_v2::{router::RaftRouter, StateStorage};
use resource_control::{
    ResourceGroupManager, ResourceManagerService, MIN_PRIORITY_UPDATE_INTERVAL,
};
use security::SecurityManager;
use tikv::{
    config::{ConfigController, DbConfigManger, DbType, LogConfigManager, TikvConfig},
    coprocessor::{self, MEMTRACE_ROOT as MEMTRACE_COPROCESSOR},
    coprocessor_v2,
    import::{ImportSstService, SstImporter},
    read_pool::{
        build_yatp_read_pool, ReadPool, ReadPoolConfigManager, UPDATE_EWMA_TIME_SLICE_INTERVAL,
    },
    server::{
        config::{Config as ServerConfig, ServerConfigManager},
        gc_worker::{AutoGcConfig, GcWorker},
        lock_manager::LockManager,
        raftkv::ReplicaReadLockChecker,
        resolve,
        service::DiagnosticsService,
        status_server::StatusServer,
        KvEngineFactoryBuilder, NodeV2, RaftKv2, Server, CPU_CORES_QUOTA_GAUGE, DEFAULT_CLUSTER_ID,
        GRPC_THREAD_PREFIX,
    },
    storage::{
        self,
        config_manager::StorageConfigManger,
        kv::LocalTablets,
        mvcc::MvccConsistencyCheckObserver,
        txn::flow_controller::{FlowController, TabletFlowController},
        Engine, Storage,
    },
};
use tikv_util::{
    check_environment_variables,
    config::{ensure_dir_exist, RaftDataStateMachine, VersionTrack},
    math::MovingAvgU32,
    metrics::INSTANCE_BACKEND_CPU_QUOTA,
    quota_limiter::{QuotaLimitConfigManager, QuotaLimiter},
    sys::{
        cpu_time::ProcessStat, disk, path_in_diff_mount_point, register_memory_usage_high_water,
        SysQuota,
    },
    thread_group::GroupProperties,
    time::{Instant, Monitor},
    worker::{Builder as WorkerBuilder, LazyWorker, Scheduler, Worker},
    yatp_pool::CleanupMethod,
    Either,
};
use tokio::runtime::Builder;

use crate::{
    common::{check_system_config, TikvServerCore, ConfiguredRaftEngine, EnginesResourceInfo},
    memory::*,
    raft_engine_switch::*,
    server::Stop,
    setup::*,
    signal_handler,
    tikv_util::sys::thread::ThreadBuildWrapper,
};

// minimum number of core kept for background requests
const BACKGROUND_REQUEST_CORE_LOWER_BOUND: f64 = 1.0;
// max ratio of core quota for background requests
const BACKGROUND_REQUEST_CORE_MAX_RATIO: f64 = 0.95;
// default ratio of core quota for background requests = core_number * 0.5
const BACKGROUND_REQUEST_CORE_DEFAULT_RATIO: f64 = 0.5;
// indication of TiKV instance is short of cpu
const SYSTEM_BUSY_THRESHOLD: f64 = 0.80;
// indication of TiKV instance in healthy state when cpu usage is in [0.5, 0.80)
const SYSTEM_HEALTHY_THRESHOLD: f64 = 0.50;
// pace of cpu quota adjustment
const CPU_QUOTA_ADJUSTMENT_PACE: f64 = 200.0; // 0.2 vcpu

#[inline]
fn run_impl<CER: ConfiguredRaftEngine, F: KvFormat>(config: TikvConfig) {
    let mut tikv = TikvServer::<CER>::init::<F>(config);

    // Must be called after `TikvServer::init`.
    let memory_limit = tikv.core.config.memory_usage_limit.unwrap().0;
    let high_water = (tikv.core.config.memory_usage_high_water * memory_limit as f64) as u64;
    register_memory_usage_high_water(high_water);

    tikv.core.check_conflict_addr();
    tikv.core.init_fs();
    tikv.core.init_yatp();
    tikv.core.init_encryption();
    let fetcher = tikv.core.init_io_utility();
    let listener = tikv.core.init_flow_receiver();
    let engines_info = tikv.init_engines(listener);
    let server_config = tikv.init_servers::<F>();
    tikv.register_services();
    tikv.init_metrics_flusher(fetcher, engines_info);
    tikv.init_storage_stats_task();
    tikv.run_server(server_config);
    tikv.run_status_server();
    tikv.init_quota_tuning_task(tikv.quota_limiter.clone());

    // TODO: support signal dump stats
    signal_handler::wait_for_signal(
        None as Option<Engines<RocksEngine, CER>>,
        tikv.kv_statistics.clone(),
        tikv.raft_statistics.clone(),
    );
    tikv.stop();
}

/// Run a TiKV server. Returns when the server is shutdown by the user, in which
/// case the server will be properly stopped.
pub fn run_tikv(config: TikvConfig) {
    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    initial_logger(&config);

    // Print version information.
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    tikv::log_tikv_info(build_timestamp);

    // Print resource quota.
    SysQuota::log_quota();
    CPU_CORES_QUOTA_GAUGE.set(SysQuota::cpu_cores_quota());

    // Do some prepare works before start.
    pre_start();

    let _m = Monitor::default();

    dispatch_api_version!(config.storage.api_version(), {
        if !config.raft_engine.enable {
            run_impl::<RocksEngine, API>(config)
        } else {
            run_impl::<RaftLogEngine, API>(config)
        }
    })
}

const DEFAULT_METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const DEFAULT_MEMTRACE_FLUSH_INTERVAL: Duration = Duration::from_millis(1_000);
const DEFAULT_ENGINE_METRICS_RESET_INTERVAL: Duration = Duration::from_millis(60_000);
const DEFAULT_STORAGE_STATS_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_QUOTA_LIMITER_TUNE_INTERVAL: Duration = Duration::from_secs(5);

/// A complete TiKV server.
struct TikvServer<ER: RaftEngine> {
    core: TikvServerCore,
    cfg_controller: Option<ConfigController>,
    security_mgr: Arc<SecurityManager>,
    pd_client: Arc<RpcClient>,
    router: Option<RaftRouter<RocksEngine, ER>>,
    node: Option<NodeV2<RpcClient, RocksEngine, ER>>,
    resolver: Option<resolve::PdStoreAddrResolver>,
    snap_mgr: Option<TabletSnapManager>, // Will be filled in `init_servers`.
    engines: Option<TikvEngines<RocksEngine, ER>>,
    kv_statistics: Option<Arc<RocksStatistics>>,
    raft_statistics: Option<Arc<RocksStatistics>>,
    servers: Option<Servers<RocksEngine, ER>>,
    region_info_accessor: Option<RegionInfoAccessor>,
    coprocessor_host: Option<CoprocessorHost<RocksEngine>>,
    to_stop: Vec<Box<dyn Stop>>,
    concurrency_manager: ConcurrencyManager,
    env: Arc<Environment>,
    background_worker: Worker,
    check_leader_worker: Worker,
    sst_worker: Option<Box<LazyWorker<String>>>,
    quota_limiter: Arc<QuotaLimiter>,
    resource_manager: Option<Arc<ResourceGroupManager>>,
    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
    tablet_registry: Option<TabletRegistry<RocksEngine>>,
}

struct TikvEngines<EK: KvEngine, ER: RaftEngine> {
    raft_engine: ER,
    engine: RaftKv2<EK, ER>,
}

struct Servers<EK: KvEngine, ER: RaftEngine> {
    lock_mgr: LockManager,
    server: LocalServer<EK, ER>,
    importer: Arc<SstImporter>,
    rsmeter_pubsub_service: resource_metering::PubSubService,
}

type LocalServer<EK, ER> = Server<resolve::PdStoreAddrResolver, RaftKv2<EK, ER>>;

impl<ER> TikvServer<ER>
where
    ER: RaftEngine,
{
    fn init<F: KvFormat>(mut config: TikvConfig) -> TikvServer<ER> {
        tikv_util::thread_group::set_properties(Some(GroupProperties::default()));
        // It is okay use pd config and security config before `init_config`,
        // because these configs must be provided by command line, and only
        // used during startup process.
        let security_mgr = Arc::new(
            SecurityManager::new(&config.security)
                .unwrap_or_else(|e| fatal!("failed to create security manager: {}", e)),
        );
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(config.server.grpc_concurrency)
                .name_prefix(thd_name!(GRPC_THREAD_PREFIX))
                .build(),
        );
        let pd_client =
            Self::connect_to_pd_cluster(&mut config, env.clone(), Arc::clone(&security_mgr));

        // Initialize and check config
        let cfg_controller = Self::init_config(config);
        let config = cfg_controller.get_current();

        let store_path = Path::new(&config.storage.data_dir).to_owned();

        let thread_count = config.server.background_thread_count;
        let background_worker = WorkerBuilder::new("background")
            .thread_count(thread_count)
            .create();

        // Initialize concurrency manager
        let latest_ts = block_on(pd_client.get_tso()).expect("failed to get timestamp from PD");
        let concurrency_manager = ConcurrencyManager::new(latest_ts);

        // use different quota for front-end and back-end requests
        let quota_limiter = Arc::new(QuotaLimiter::new(
            config.quota.foreground_cpu_time,
            config.quota.foreground_write_bandwidth,
            config.quota.foreground_read_bandwidth,
            config.quota.background_cpu_time,
            config.quota.background_write_bandwidth,
            config.quota.background_read_bandwidth,
            config.quota.max_delay_duration,
            config.quota.enable_auto_tune,
        ));

        let resource_manager = if config.resource_control.enabled {
            let mgr = Arc::new(ResourceGroupManager::default());
            let mut resource_mgr_service =
                ResourceManagerService::new(mgr.clone(), pd_client.clone());
            // spawn a task to periodically update the minimal virtual time of all resource
            // groups.
            let resource_mgr = mgr.clone();
            background_worker.spawn_interval_task(MIN_PRIORITY_UPDATE_INTERVAL, move || {
                resource_mgr.advance_min_virtual_time();
            });
            // spawn a task to watch all resource groups update.
            background_worker.spawn_async_task(async move {
                resource_mgr_service.watch_resource_groups().await;
            });
            Some(mgr)
        } else {
            None
        };

        let mut causal_ts_provider = None;
        if let ApiVersion::V2 = F::TAG {
            let tso = block_on(causal_ts::BatchTsoProvider::new_opt(
                pd_client.clone(),
                config.causal_ts.renew_interval.0,
                config.causal_ts.alloc_ahead_buffer.0,
                config.causal_ts.renew_batch_min_size,
                config.causal_ts.renew_batch_max_size,
            ));
            if let Err(e) = tso {
                fatal!("Causal timestamp provider initialize failed: {:?}", e);
            }
            causal_ts_provider = Some(Arc::new(tso.unwrap().into()));
            info!("Causal timestamp provider startup.");
        }

        // Run check leader in a dedicate thread, because it is time sensitive
        // and crucial to TiCDC replication lag.
        let check_leader_worker = WorkerBuilder::new("check_leader").thread_count(1).create();

        TikvServer {
            core: TikvServerCore {
                config,
                store_path,
                lock_files: vec![],
                encryption_key_manager: None,
                flow_info_sender: None,
                flow_info_receiver: None,
            },
            cfg_controller: Some(cfg_controller),
            security_mgr,
            pd_client,
            router: None,
            node: None,
            resolver: None,
            snap_mgr: None,
            engines: None,
            kv_statistics: None,
            raft_statistics: None,
            servers: None,
            region_info_accessor: None,
            coprocessor_host: None,
            to_stop: vec![],
            concurrency_manager,
            env,
            background_worker,
            check_leader_worker,
            sst_worker: None,
            quota_limiter,
            resource_manager,
            causal_ts_provider,
            tablet_registry: None,
        }
    }

    /// Initialize and check the config
    ///
    /// Warnings are logged and fatal errors exist.
    ///
    /// #  Fatal errors
    ///
    /// - If `dynamic config` feature is enabled and failed to register config
    ///   to PD
    /// - If some critical configs (like data dir) are differrent from last run
    /// - If the config can't pass `validate()`
    /// - If the max open file descriptor limit is not high enough to support
    ///   the main database and the raft database.
    fn init_config(mut config: TikvConfig) -> ConfigController {
        validate_and_persist_config(&mut config, true);

        ensure_dir_exist(&config.storage.data_dir).unwrap();
        if !config.rocksdb.wal_dir.is_empty() {
            ensure_dir_exist(&config.rocksdb.wal_dir).unwrap();
        }
        if config.raft_engine.enable {
            ensure_dir_exist(&config.raft_engine.config().dir).unwrap();
        } else {
            ensure_dir_exist(&config.raft_store.raftdb_path).unwrap();
            if !config.raftdb.wal_dir.is_empty() {
                ensure_dir_exist(&config.raftdb.wal_dir).unwrap();
            }
        }

        check_system_config(&config);

        tikv_util::set_panic_hook(config.abort_on_panic, &config.storage.data_dir);

        info!(
            "using config";
            "config" => serde_json::to_string(&config).unwrap(),
        );
        if config.panic_when_unexpected_key_or_data {
            info!("panic-when-unexpected-key-or-data is on");
            tikv_util::set_panic_when_unexpected_key_or_data(true);
        }

        config.write_into_metrics();

        ConfigController::new(config)
    }

    fn connect_to_pd_cluster(
        config: &mut TikvConfig,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Arc<RpcClient> {
        let pd_client = Arc::new(
            RpcClient::new(&config.pd, Some(env), security_mgr)
                .unwrap_or_else(|e| fatal!("failed to create rpc client: {}", e)),
        );

        let cluster_id = pd_client
            .get_cluster_id()
            .unwrap_or_else(|e| fatal!("failed to get cluster id: {}", e));
        if cluster_id == DEFAULT_CLUSTER_ID {
            fatal!("cluster id can't be {}", DEFAULT_CLUSTER_ID);
        }
        config.server.cluster_id = cluster_id;
        info!(
            "connect to PD cluster";
            "cluster_id" => cluster_id
        );

        pd_client
    }

    fn init_gc_worker(&mut self) -> GcWorker<RaftKv2<RocksEngine, ER>> {
        let engines = self.engines.as_ref().unwrap();
        let gc_worker = GcWorker::new(
            engines.engine.clone(),
            self.core.flow_info_sender.take().unwrap(),
            self.core.config.gc.clone(),
            self.pd_client.feature_gate().clone(),
            Arc::new(self.region_info_accessor.clone().unwrap()),
        );

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Gc,
            Box::new(gc_worker.get_config_manager()),
        );

        gc_worker
    }

    fn init_servers<F: KvFormat>(&mut self) -> Arc<VersionTrack<ServerConfig>> {
        let flow_controller = Arc::new(FlowController::Tablet(TabletFlowController::new(
            &self.core.config.storage.flow_control,
            self.tablet_registry.clone().unwrap(),
            self.core.flow_info_receiver.take().unwrap(),
        )));
        let mut gc_worker = self.init_gc_worker();
        let ttl_checker = Box::new(LazyWorker::new("ttl-checker"));
        let ttl_scheduler = ttl_checker.scheduler();

        let cfg_controller = self.cfg_controller.as_mut().unwrap();

        cfg_controller.register(
            tikv::config::Module::Quota,
            Box::new(QuotaLimitConfigManager::new(Arc::clone(
                &self.quota_limiter,
            ))),
        );

        cfg_controller.register(tikv::config::Module::Log, Box::new(LogConfigManager));

        let lock_mgr = LockManager::new(&self.core.config.pessimistic_txn);
        cfg_controller.register(
            tikv::config::Module::PessimisticTxn,
            Box::new(lock_mgr.config_manager()),
        );
        lock_mgr.register_detector_role_change_observer(self.coprocessor_host.as_mut().unwrap());

        let engines = self.engines.as_ref().unwrap();

        let pd_worker = LazyWorker::new("pd-worker");
        let pd_sender = raftstore_v2::PdReporter::new(
            pd_worker.scheduler(),
            slog_global::borrow_global().new(slog::o!()),
        );

        let unified_read_pool = if self.core.config.readpool.is_unified_pool_enabled() {
            let resource_ctl = self
                .resource_manager
                .as_ref()
                .map(|m| m.derive_controller("unified-read-pool".into(), true));
            Some(build_yatp_read_pool(
                &self.core.config.readpool.unified,
                pd_sender.clone(),
                engines.engine.clone(),
                resource_ctl,
                CleanupMethod::Remote(self.background_worker.remote()),
            ))
        } else {
            None
        };
        if let Some(unified_read_pool) = &unified_read_pool {
            let handle = unified_read_pool.handle();
            self.background_worker.spawn_interval_task(
                UPDATE_EWMA_TIME_SLICE_INTERVAL,
                move || {
                    handle.update_ewma_time_slice();
                },
            );
        }

        // The `DebugService` and `DiagnosticsService` will share the same thread pool
        let props = tikv_util::thread_group::current_properties();
        let debug_thread_pool = Arc::new(
            Builder::new_multi_thread()
                .thread_name(thd_name!("debugger"))
                .worker_threads(1)
                .after_start_wrapper(move || {
                    tikv_alloc::add_thread_memory_accessor();
                    tikv_util::thread_group::set_properties(props.clone());
                })
                .before_stop_wrapper(tikv_alloc::remove_thread_memory_accessor)
                .build()
                .unwrap(),
        );

        // Start resource metering.
        let (recorder_notifier, collector_reg_handle, resource_tag_factory, recorder_worker) =
            resource_metering::init_recorder(
                self.core.config.resource_metering.precision.as_millis(),
            );
        self.to_stop.push(recorder_worker);
        let (reporter_notifier, data_sink_reg_handle, reporter_worker) =
            resource_metering::init_reporter(
                self.core.config.resource_metering.clone(),
                collector_reg_handle.clone(),
            );
        self.to_stop.push(reporter_worker);
        let (address_change_notifier, single_target_worker) = resource_metering::init_single_target(
            self.core.config.resource_metering.receiver_address.clone(),
            self.env.clone(),
            data_sink_reg_handle.clone(),
        );
        self.to_stop.push(single_target_worker);
        let rsmeter_pubsub_service = resource_metering::PubSubService::new(data_sink_reg_handle);

        let cfg_manager = resource_metering::ConfigManager::new(
            self.core.config.resource_metering.clone(),
            recorder_notifier,
            reporter_notifier,
            address_change_notifier,
        );
        cfg_controller.register(
            tikv::config::Module::ResourceMetering,
            Box::new(cfg_manager),
        );

        let storage_read_pool_handle = if self.core.config.readpool.storage.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let storage_read_pools = ReadPool::from(storage::build_read_pool(
                &self.core.config.readpool.storage,
                pd_sender.clone(),
                engines.engine.clone(),
            ));
            storage_read_pools.handle()
        };

        let storage = Storage::<_, _, F>::from_engine(
            engines.engine.clone(),
            &self.core.config.storage,
            storage_read_pool_handle,
            lock_mgr.clone(),
            self.concurrency_manager.clone(),
            lock_mgr.get_storage_dynamic_configs(),
            flow_controller.clone(),
            pd_sender.clone(),
            resource_tag_factory.clone(),
            Arc::clone(&self.quota_limiter),
            self.pd_client.feature_gate().clone(),
            self.causal_ts_provider.clone(),
            self.resource_manager
                .as_ref()
                .map(|m| m.derive_controller("scheduler-worker-pool".to_owned(), true)),
        )
        .unwrap_or_else(|e| fatal!("failed to create raft storage: {}", e));
        cfg_controller.register(
            tikv::config::Module::Storage,
            Box::new(StorageConfigManger::new(
                self.tablet_registry.as_ref().unwrap().clone(),
                ttl_scheduler,
                flow_controller,
                storage.get_scheduler(),
            )),
        );

        let (resolver, state) = resolve::new_resolver(
            self.pd_client.clone(),
            &self.background_worker,
            storage.get_engine().raft_extension(),
        );
        self.resolver = Some(resolver);

        ReplicaReadLockChecker::new(self.concurrency_manager.clone())
            .register(self.coprocessor_host.as_mut().unwrap());

        // Create snapshot manager, server.
        let snap_path = self
            .core
            .store_path
            .join(Path::new("tablet_snap"))
            .to_str()
            .unwrap()
            .to_owned();

        let snap_mgr = match TabletSnapManager::new(&snap_path) {
            Ok(mgr) => mgr,
            Err(e) => fatal!("failed to create snapshot manager at {}: {}", snap_path, e),
        };

        // Create coprocessor endpoint.
        let cop_read_pool_handle = if self.core.config.readpool.coprocessor.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let cop_read_pools = ReadPool::from(coprocessor::readpool_impl::build_read_pool(
                &self.core.config.readpool.coprocessor,
                pd_sender,
                engines.engine.clone(),
            ));
            cop_read_pools.handle()
        };

        let mut unified_read_pool_scale_receiver = None;
        if self.core.config.readpool.is_unified_pool_enabled() {
            let (unified_read_pool_scale_notifier, rx) = mpsc::sync_channel(10);
            cfg_controller.register(
                tikv::config::Module::Readpool,
                Box::new(ReadPoolConfigManager::new(
                    unified_read_pool.as_ref().unwrap().handle(),
                    unified_read_pool_scale_notifier,
                    &self.background_worker,
                    self.core.config.readpool.unified.max_thread_count,
                    self.core.config.readpool.unified.auto_adjust_pool_size,
                )),
            );
            unified_read_pool_scale_receiver = Some(rx);
        }

        let check_leader_runner = CheckLeaderRunner::new(
            self.router.as_ref().unwrap().store_meta().clone(),
            self.coprocessor_host.clone().unwrap(),
        );
        let check_leader_scheduler = self
            .check_leader_worker
            .start("check-leader", check_leader_runner);

        let server_config = Arc::new(VersionTrack::new(self.core.config.server.clone()));

        self.core
            .config
            .raft_store
            .validate(
                self.core.config.coprocessor.region_split_size(),
                self.core.config.coprocessor.enable_region_bucket(),
                self.core.config.coprocessor.region_bucket_size,
            )
            .unwrap_or_else(|e| fatal!("failed to validate raftstore config {}", e));
        let raft_store = Arc::new(VersionTrack::new(self.core.config.raft_store.clone()));
        let health_service = HealthService::default();

        let node = self.node.as_ref().unwrap();

        self.snap_mgr = Some(snap_mgr.clone());
        // Create server
        let server = Server::new(
            node.id(),
            &server_config,
            &self.security_mgr,
            storage,
            coprocessor::Endpoint::new(
                &server_config.value(),
                cop_read_pool_handle,
                self.concurrency_manager.clone(),
                resource_tag_factory,
                Arc::clone(&self.quota_limiter),
            ),
            coprocessor_v2::Endpoint::new(&self.core.config.coprocessor_v2),
            self.resolver.clone().unwrap(),
            Either::Right(snap_mgr.clone()),
            gc_worker.clone(),
            check_leader_scheduler,
            self.env.clone(),
            unified_read_pool,
            debug_thread_pool,
            health_service,
        )
        .unwrap_or_else(|e| fatal!("failed to create server: {}", e));
        cfg_controller.register(
            tikv::config::Module::Server,
            Box::new(ServerConfigManager::new(
                server.get_snap_worker_scheduler(),
                server_config.clone(),
                server.get_grpc_mem_quota().clone(),
            )),
        );

        let import_path = self.core.store_path.join("import");
        let mut importer = SstImporter::new(
            &self.core.config.import,
            import_path,
            self.core.encryption_key_manager.clone(),
            self.core.config.storage.api_version(),
        )
        .unwrap();
        for (cf_name, compression_type) in &[
            (
                CF_DEFAULT,
                self.core
                    .config
                    .rocksdb
                    .defaultcf
                    .bottommost_level_compression,
            ),
            (
                CF_WRITE,
                self.core
                    .config
                    .rocksdb
                    .writecf
                    .bottommost_level_compression,
            ),
        ] {
            importer.set_compression_type(cf_name, from_rocks_compression_type(*compression_type));
        }
        let importer = Arc::new(importer);

        // V2 starts split-check worker within raftstore.

        let split_config_manager =
            SplitConfigManager::new(Arc::new(VersionTrack::new(self.core.config.split.clone())));
        cfg_controller.register(
            tikv::config::Module::Split,
            Box::new(split_config_manager.clone()),
        );

        let auto_split_controller = AutoSplitController::new(
            split_config_manager,
            self.core.config.server.grpc_concurrency,
            self.core.config.readpool.unified.max_thread_count,
            unified_read_pool_scale_receiver,
        );

        // `ConsistencyCheckObserver` must be registered before `Node::start`.
        let safe_point = Arc::new(AtomicU64::new(0));
        let observer = match self.core.config.coprocessor.consistency_check_method {
            ConsistencyCheckMethod::Mvcc => BoxConsistencyCheckObserver::new(
                MvccConsistencyCheckObserver::new(safe_point.clone()),
            ),
            ConsistencyCheckMethod::Raw => {
                BoxConsistencyCheckObserver::new(RawConsistencyCheckObserver::default())
            }
        };
        self.coprocessor_host
            .as_mut()
            .unwrap()
            .registry
            .register_consistency_check_observer(100, observer);

        self.node
            .as_mut()
            .unwrap()
            .start(
                engines.raft_engine.clone(),
                self.tablet_registry.clone().unwrap(),
                self.router.as_ref().unwrap(),
                server.transport(),
                snap_mgr,
                self.concurrency_manager.clone(),
                self.causal_ts_provider.clone(),
                self.coprocessor_host.clone().unwrap(),
                auto_split_controller,
                collector_reg_handle,
                self.background_worker.clone(),
                pd_worker,
                raft_store,
                &state,
                importer.clone(),
            )
            .unwrap_or_else(|e| fatal!("failed to start node: {}", e));

        // Start auto gc. Must after `Node::start` because `node_id` is initialized
        // there.
        let store_id = self.node.as_ref().unwrap().id();
        let auto_gc_config = AutoGcConfig::new(
            self.pd_client.clone(),
            self.region_info_accessor.clone().unwrap(),
            store_id,
        );
        gc_worker
            .start(store_id)
            .unwrap_or_else(|e| fatal!("failed to start gc worker: {}", e));
        if let Err(e) = gc_worker.start_auto_gc(auto_gc_config, safe_point) {
            fatal!("failed to start auto_gc on storage, error: {}", e);
        }

        initial_metric(&self.core.config.metric);

        self.servers = Some(Servers {
            lock_mgr,
            server,
            importer,
            rsmeter_pubsub_service,
        });

        server_config
    }

    fn register_services(&mut self) {
        let servers = self.servers.as_mut().unwrap();
        let engines = self.engines.as_ref().unwrap();

        // Backup service.
        let mut backup_worker = Box::new(self.background_worker.lazy_build("backup-endpoint"));
        let backup_scheduler = backup_worker.scheduler();
        let backup_service = backup::Service::<RocksEngine, ER>::new(backup_scheduler);
        if servers
            .server
            .register_service(create_backup(backup_service))
            .is_some()
        {
            fatal!("failed to register backup service");
        }

        let backup_endpoint = backup::Endpoint::new(
            self.node.as_ref().unwrap().id(),
            engines.engine.clone(),
            self.region_info_accessor.clone().unwrap(),
            LocalTablets::Registry(self.tablet_registry.as_ref().unwrap().clone()),
            self.core.config.backup.clone(),
            self.concurrency_manager.clone(),
            self.core.config.storage.api_version(),
            self.causal_ts_provider.clone(),
        );
        self.cfg_controller.as_mut().unwrap().register(
            tikv::config::Module::Backup,
            Box::new(backup_endpoint.get_config_manager()),
        );
        backup_worker.start(backup_endpoint);

        // Import SST service.
        let import_service = ImportSstService::new(
            self.core.config.import.clone(),
            self.core.config.raft_store.raft_entry_max_size,
            engines.engine.clone(),
            LocalTablets::Registry(self.tablet_registry.as_ref().unwrap().clone()),
            servers.importer.clone(),
        );
        let import_cfg_mgr = import_service.get_config_manager();

        if servers
            .server
            .register_service(create_import_sst(import_service))
            .is_some()
        {
            fatal!("failed to register import service");
        }

        self.cfg_controller
            .as_mut()
            .unwrap()
            .register(tikv::config::Module::Import, Box::new(import_cfg_mgr));

        // Create Diagnostics service
        let diag_service = DiagnosticsService::new(
            servers.server.get_debug_thread_pool().clone(),
            self.core.config.log.file.filename.clone(),
            self.core.config.slow_log_file.clone(),
        );
        if servers
            .server
            .register_service(create_diagnostics(diag_service))
            .is_some()
        {
            fatal!("failed to register diagnostics service");
        }

        // Lock manager.
        if servers
            .server
            .register_service(create_deadlock(servers.lock_mgr.deadlock_service()))
            .is_some()
        {
            fatal!("failed to register deadlock service");
        }

        servers
            .lock_mgr
            .start(
                self.node.as_ref().unwrap().id(),
                self.pd_client.clone(),
                self.resolver.clone().unwrap(),
                self.security_mgr.clone(),
                &self.core.config.pessimistic_txn,
            )
            .unwrap_or_else(|e| fatal!("failed to start lock manager: {}", e));

        if servers
            .server
            .register_service(create_resource_metering_pub_sub(
                servers.rsmeter_pubsub_service.clone(),
            ))
            .is_some()
        {
            warn!("failed to register resource metering pubsub service");
        }
    }

    fn init_metrics_flusher(
        &mut self,
        fetcher: BytesFetcher,
        engines_info: Arc<EnginesResourceInfo>,
    ) {
        let mut engine_metrics = EngineMetricsManager::<RocksEngine, ER>::new(
            self.tablet_registry.clone().unwrap(),
            self.kv_statistics.clone(),
            self.core.config.rocksdb.titan.enabled,
            self.engines.as_ref().unwrap().raft_engine.clone(),
            self.raft_statistics.clone(),
        );
        let mut io_metrics = IoMetricsManager::new(fetcher);
        let engines_info_clone = engines_info.clone();

        // region_id -> (suffix, tablet)
        // `update` of EnginesResourceInfo is called perodically which needs this map
        // for recording the latest tablet for each region.
        // `cached_latest_tablets` is passed to `update` to avoid memory
        // allocation each time when calling `update`.
        let mut cached_latest_tablets = HashMap::default();
        self.background_worker
            .spawn_interval_task(DEFAULT_METRICS_FLUSH_INTERVAL, move || {
                let now = Instant::now();
                engine_metrics.flush(now);
                io_metrics.flush(now);
                engines_info_clone.update(now, &mut cached_latest_tablets);
            });
        if let Some(limiter) = get_io_rate_limiter() {
            limiter.set_low_priority_io_adjustor_if_needed(Some(engines_info));
        }

        let mut mem_trace_metrics = MemoryTraceManager::default();
        mem_trace_metrics.register_provider(MEMTRACE_RAFTSTORE.clone());
        mem_trace_metrics.register_provider(MEMTRACE_COPROCESSOR.clone());
        self.background_worker
            .spawn_interval_task(DEFAULT_MEMTRACE_FLUSH_INTERVAL, move || {
                let now = Instant::now();
                mem_trace_metrics.flush(now);
            });
    }

    // Only background cpu quota tuning is implemented at present. iops and frontend
    // quota tuning is on the way
    fn init_quota_tuning_task(&self, quota_limiter: Arc<QuotaLimiter>) {
        // No need to do auto tune when capacity is really low
        if SysQuota::cpu_cores_quota() * BACKGROUND_REQUEST_CORE_MAX_RATIO
            < BACKGROUND_REQUEST_CORE_LOWER_BOUND
        {
            return;
        };

        // Determine the base cpu quota
        let base_cpu_quota =
            // if cpu quota is not specified, start from optimistic case
            if quota_limiter.cputime_limiter(false).is_infinite() {
                1000_f64
                    * f64::max(
                        BACKGROUND_REQUEST_CORE_LOWER_BOUND,
                        SysQuota::cpu_cores_quota() * BACKGROUND_REQUEST_CORE_DEFAULT_RATIO,
                    )
            } else {
                quota_limiter.cputime_limiter(false) / 1000_f64
            };

        // Calculate the celling and floor quota
        let celling_quota = f64::min(
            base_cpu_quota * 2.0,
            1_000_f64 * SysQuota::cpu_cores_quota() * BACKGROUND_REQUEST_CORE_MAX_RATIO,
        );
        let floor_quota = f64::max(
            base_cpu_quota * 0.5,
            1_000_f64 * BACKGROUND_REQUEST_CORE_LOWER_BOUND,
        );

        let mut proc_stats: ProcessStat = ProcessStat::cur_proc_stat().unwrap();
        self.background_worker.spawn_interval_task(
            DEFAULT_QUOTA_LIMITER_TUNE_INTERVAL,
            move || {
                if quota_limiter.auto_tune_enabled() {
                    let cputime_limit = quota_limiter.cputime_limiter(false);
                    let old_quota = if cputime_limit.is_infinite() {
                        base_cpu_quota
                    } else {
                        cputime_limit / 1000_f64
                    };
                    let cpu_usage = match proc_stats.cpu_usage() {
                        Ok(r) => r,
                        Err(_e) => 0.0,
                    };
                    // Try tuning quota when cpu_usage is correctly collected.
                    // rule based tuning:
                    // - if instance is busy, shrink cpu quota for analyze by one quota pace until
                    //   lower bound is hit;
                    // - if instance cpu usage is healthy, no op;
                    // - if instance is idle, increase cpu quota by one quota pace  until upper
                    //   bound is hit.
                    if cpu_usage > 0.0f64 {
                        let mut target_quota = old_quota;

                        let cpu_util = cpu_usage / SysQuota::cpu_cores_quota();
                        if cpu_util >= SYSTEM_BUSY_THRESHOLD {
                            target_quota =
                                f64::max(target_quota - CPU_QUOTA_ADJUSTMENT_PACE, floor_quota);
                        } else if cpu_util < SYSTEM_HEALTHY_THRESHOLD {
                            target_quota =
                                f64::min(target_quota + CPU_QUOTA_ADJUSTMENT_PACE, celling_quota);
                        }

                        if old_quota != target_quota {
                            quota_limiter.set_cpu_time_limit(target_quota as usize, false);
                            debug!(
                                "cpu_time_limiter tuned for backend request";
                                "cpu_util" => ?cpu_util,
                                "new quota" => ?target_quota);
                            INSTANCE_BACKEND_CPU_QUOTA.set(target_quota as i64);
                        }
                    }
                }
            },
        );
    }

    fn init_storage_stats_task(&self) {
        let config_disk_capacity: u64 = self.core.config.raft_store.capacity.0;
        let data_dir = self.core.config.storage.data_dir.clone();
        let store_path = self.core.store_path.clone();
        let snap_mgr = self.snap_mgr.clone().unwrap();
        let reserve_space = disk::get_disk_reserved_space();
        let reserve_raft_space = disk::get_raft_disk_reserved_space();
        if reserve_space == 0 && reserve_raft_space == 0 {
            info!("disk space checker not enabled");
            return;
        }
        let raft_engine = self.engines.as_ref().unwrap().raft_engine.clone();
        let tablet_registry = self.tablet_registry.clone().unwrap();
        let raft_path = raft_engine.get_engine_path().to_string();
        let separated_raft_mount_path =
            path_in_diff_mount_point(raft_path.as_str(), tablet_registry.tablet_root());
        let raft_almost_full_threshold = reserve_raft_space;
        let raft_already_full_threshold = reserve_raft_space / 2;

        let almost_full_threshold = reserve_space;
        let already_full_threshold = reserve_space / 2;
        fn calculate_disk_usage(a: disk::DiskUsage, b: disk::DiskUsage) -> disk::DiskUsage {
            match (a, b) {
                (disk::DiskUsage::AlreadyFull, _) => disk::DiskUsage::AlreadyFull,
                (_, disk::DiskUsage::AlreadyFull) => disk::DiskUsage::AlreadyFull,
                (disk::DiskUsage::AlmostFull, _) => disk::DiskUsage::AlmostFull,
                (_, disk::DiskUsage::AlmostFull) => disk::DiskUsage::AlmostFull,
                (disk::DiskUsage::Normal, disk::DiskUsage::Normal) => disk::DiskUsage::Normal,
            }
        }
        self.background_worker
            .spawn_interval_task(DEFAULT_STORAGE_STATS_INTERVAL, move || {
                let disk_stats = match fs2::statvfs(&store_path) {
                    Err(e) => {
                        error!(
                            "get disk stat for kv store failed";
                            "kv path" => store_path.to_str(),
                            "err" => ?e
                        );
                        return;
                    }
                    Ok(stats) => stats,
                };
                let disk_cap = disk_stats.total_space();
                let snap_size = snap_mgr.total_snap_size().unwrap();

                let mut kv_size = 0;
                tablet_registry.for_each_opened_tablet(|_, cached| {
                    if let Some(tablet) = cached.latest() {
                        kv_size += tablet.get_engine_used_size().unwrap_or(0);
                    }
                    true
                });

                let raft_size = raft_engine
                    .get_engine_size()
                    .expect("get raft engine size");

                let mut raft_disk_status = disk::DiskUsage::Normal;
                if separated_raft_mount_path && reserve_raft_space != 0 {
                    let raft_disk_stats = match fs2::statvfs(&raft_path) {
                        Err(e) => {
                            error!(
                                "get disk stat for raft engine failed";
                                "raft engine path" => raft_path.clone(),
                                "err" => ?e
                            );
                            return;
                        }
                        Ok(stats) => stats,
                    };
                    let raft_disk_cap = raft_disk_stats.total_space();
                    let mut raft_disk_available =
                        raft_disk_cap.checked_sub(raft_size).unwrap_or_default();
                    raft_disk_available = cmp::min(raft_disk_available, raft_disk_stats.available_space());
                    raft_disk_status = if raft_disk_available <= raft_already_full_threshold
                    {
                        disk::DiskUsage::AlreadyFull
                    } else if raft_disk_available <= raft_almost_full_threshold
                    {
                        disk::DiskUsage::AlmostFull
                    } else {
                        disk::DiskUsage::Normal
                    };
                }
                let placeholer_file_path = PathBuf::from_str(&data_dir)
                    .unwrap()
                    .join(Path::new(file_system::SPACE_PLACEHOLDER_FILE));

                let placeholder_size: u64 =
                    file_system::get_file_size(placeholer_file_path).unwrap_or(0);

                let used_size = if !separated_raft_mount_path {
                    snap_size + kv_size + raft_size + placeholder_size
                } else {
                    snap_size + kv_size + placeholder_size
                };
                let capacity = if config_disk_capacity == 0 || disk_cap < config_disk_capacity {
                    disk_cap
                } else {
                    config_disk_capacity
                };

                let mut available = capacity.checked_sub(used_size).unwrap_or_default();
                available = cmp::min(available, disk_stats.available_space());

                let prev_disk_status = disk::get_disk_status(0); //0 no need care about failpoint.
                let cur_kv_disk_status = if available <= already_full_threshold {
                    disk::DiskUsage::AlreadyFull
                } else if available <= almost_full_threshold {
                    disk::DiskUsage::AlmostFull
                } else {
                    disk::DiskUsage::Normal
                };
                let cur_disk_status = calculate_disk_usage(raft_disk_status, cur_kv_disk_status);
                if prev_disk_status != cur_disk_status {
                    warn!(
                        "disk usage {:?}->{:?} (raft engine usage: {:?}, kv engine usage: {:?}), seperated raft mount={}, kv available={}, snap={}, kv={}, raft={}, capacity={}",
                        prev_disk_status,
                        cur_disk_status,
                        raft_disk_status,
                        cur_kv_disk_status,
                        separated_raft_mount_path,
                        available,
                        snap_size,
                        kv_size,
                        raft_size,
                        capacity
                    );
                }
                disk::set_disk_status(cur_disk_status);
            })
    }

    fn init_sst_recovery_sender(&mut self) -> Option<Scheduler<String>> {
        if !self
            .core
            .config
            .storage
            .background_error_recovery_window
            .is_zero()
        {
            let sst_worker = Box::new(LazyWorker::new("sst-recovery"));
            let scheduler = sst_worker.scheduler();
            self.sst_worker = Some(sst_worker);
            Some(scheduler)
        } else {
            None
        }
    }

    fn run_server(&mut self, server_config: Arc<VersionTrack<ServerConfig>>) {
        let server = self.servers.as_mut().unwrap();
        server
            .server
            .build_and_bind()
            .unwrap_or_else(|e| fatal!("failed to build server: {}", e));
        server
            .server
            .start(
                server_config,
                self.security_mgr.clone(),
                self.tablet_registry.clone().unwrap(),
            )
            .unwrap_or_else(|e| fatal!("failed to start server: {}", e));
    }

    fn run_status_server(&mut self) {
        // Create a status server.
        let status_enabled = !self.core.config.server.status_addr.is_empty();
        if status_enabled {
            let mut status_server = match StatusServer::new(
                self.core.config.server.status_thread_pool_size,
                self.cfg_controller.take().unwrap(),
                Arc::new(self.core.config.security.clone()),
                self.engines.as_ref().unwrap().engine.raft_extension(),
                self.core.store_path.clone(),
                self.resource_manager.clone(),
            ) {
                Ok(status_server) => Box::new(status_server),
                Err(e) => {
                    error_unknown!(%e; "failed to start runtime for status service");
                    return;
                }
            };
            // Start the status server.
            if let Err(e) = status_server.start(self.core.config.server.status_addr.clone()) {
                error_unknown!(%e; "failed to bind addr for status service");
            } else {
                self.to_stop.push(status_server);
            }
        }
    }

    fn stop(mut self) {
        tikv_util::thread_group::mark_shutdown();
        let mut servers = self.servers.unwrap();
        servers
            .server
            .stop()
            .unwrap_or_else(|e| fatal!("failed to stop server: {}", e));

        self.node.as_mut().unwrap().stop();
        self.region_info_accessor.as_mut().unwrap().stop();

        servers.lock_mgr.stop();

        if let Some(sst_worker) = self.sst_worker {
            sst_worker.stop_worker();
        }

        self.to_stop.into_iter().for_each(|s| s.stop());
    }
}

impl<CER: ConfiguredRaftEngine> TikvServer<CER> {
    fn init_engines(
        &mut self,
        flow_listener: engine_rocks::FlowListener,
    ) -> Arc<EnginesResourceInfo> {
        let block_cache = self
            .core
            .config
            .storage
            .block_cache
            .build_shared_cache(self.core.config.storage.engine);
        let env = self
            .core
            .config
            .build_shared_rocks_env(
                self.core.encryption_key_manager.clone(),
                get_io_rate_limiter(),
            )
            .unwrap();

        // Create raft engine
        let (raft_engine, raft_statistics) = CER::build(
            &self.core.config,
            &env,
            &self.core.encryption_key_manager,
            &block_cache,
        );
        self.raft_statistics = raft_statistics;

        // Create kv engine.
        let builder = KvEngineFactoryBuilder::new(env, &self.core.config, block_cache)
            .sst_recovery_sender(self.init_sst_recovery_sender())
            .flow_listener(flow_listener);

        let mut node = NodeV2::new(&self.core.config.server, self.pd_client.clone(), None);
        node.try_bootstrap_store(&self.core.config.raft_store, &raft_engine)
            .unwrap_or_else(|e| fatal!("failed to bootstrap store: {:?}", e));
        assert_ne!(node.id(), 0);

        let router = node.router().clone();

        // Create kv engine.
        let builder = builder.state_storage(Arc::new(StateStorage::new(
            raft_engine.clone(),
            router.clone(),
        )));
        let factory = Box::new(builder.build());
        self.kv_statistics = Some(factory.rocks_statistics());
        let registry = TabletRegistry::new(factory, self.core.store_path.join("tablets"))
            .unwrap_or_else(|e| fatal!("failed to create tablet registry {:?}", e));
        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Rocksdb,
            Box::new(DbConfigManger::new(registry.clone(), DbType::Kv)),
        );
        self.tablet_registry = Some(registry.clone());
        raft_engine.register_config(cfg_controller);

        let engines_info = Arc::new(EnginesResourceInfo::new(
            registry,
            raft_engine.as_rocks_engine().cloned(),
            180, // max_samples_to_preserve
        ));

        let router = RaftRouter::new(node.id(), router);
        let mut coprocessor_host: CoprocessorHost<RocksEngine> = CoprocessorHost::new(
            router.store_router().clone(),
            self.core.config.coprocessor.clone(),
        );
        let region_info_accessor = RegionInfoAccessor::new(&mut coprocessor_host);

        let engine = RaftKv2::new(router.clone(), region_info_accessor.region_leaders());

        self.engines = Some(TikvEngines {
            raft_engine,
            engine,
        });
        self.router = Some(router);
        self.node = Some(node);
        self.coprocessor_host = Some(coprocessor_host);
        self.region_info_accessor = Some(region_info_accessor);

        engines_info
    }
}

/// Various sanity-checks and logging before running a server.
///
/// Warnings are logged.
///
/// # Logs
///
/// The presence of these environment variables that affect the database
/// behavior is logged.
///
/// - `GRPC_POLL_STRATEGY`
/// - `http_proxy` and `https_proxy`
///
/// # Warnings
///
/// - if `net.core.somaxconn` < 32768
/// - if `net.ipv4.tcp_syncookies` is not 0
/// - if `vm.swappiness` is not 0
/// - if data directories are not on SSDs
/// - if the "TZ" environment variable is not set on unix
fn pre_start() {
    check_environment_variables();
    for e in tikv_util::config::check_kernel() {
        warn!(
            "check: kernel";
            "err" => %e
        );
    }
}
pub struct EngineMetricsManager<EK: KvEngine, ER: RaftEngine> {
    tablet_registry: TabletRegistry<EK>,
    kv_statistics: Option<Arc<RocksStatistics>>,
    kv_is_titan: bool,
    raft_engine: ER,
    raft_statistics: Option<Arc<RocksStatistics>>,
    last_reset: Instant,
}

impl<EK: KvEngine, ER: RaftEngine> EngineMetricsManager<EK, ER> {
    pub fn new(
        tablet_registry: TabletRegistry<EK>,
        kv_statistics: Option<Arc<RocksStatistics>>,
        kv_is_titan: bool,
        raft_engine: ER,
        raft_statistics: Option<Arc<RocksStatistics>>,
    ) -> Self {
        EngineMetricsManager {
            tablet_registry,
            kv_statistics,
            kv_is_titan,
            raft_engine,
            raft_statistics,
            last_reset: Instant::now(),
        }
    }

    pub fn flush(&mut self, now: Instant) {
        let mut reporter = EK::StatisticsReporter::new("kv");
        self.tablet_registry
            .for_each_opened_tablet(|_, db: &mut CachedTablet<EK>| {
                if let Some(db) = db.latest() {
                    reporter.collect(db);
                }
                true
            });
        reporter.flush();
        self.raft_engine.flush_metrics("raft");

        if let Some(s) = self.kv_statistics.as_ref() {
            flush_engine_statistics(s, "kv", self.kv_is_titan);
        }
        if let Some(s) = self.raft_statistics.as_ref() {
            flush_engine_statistics(s, "raft", false);
        }
        if now.saturating_duration_since(self.last_reset) >= DEFAULT_ENGINE_METRICS_RESET_INTERVAL {
            if let Some(s) = self.kv_statistics.as_ref() {
                s.reset();
            }
            if let Some(s) = self.raft_statistics.as_ref() {
                s.reset();
            }
            self.last_reset = now;
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        sync::{atomic::Ordering, Arc},
    };

    use engine_rocks::raw::Env;
    use engine_traits::{
        FlowControlFactorsExt, MiscExt, SyncMutable, TabletContext, TabletRegistry, CF_DEFAULT,
    };
    use tempfile::Builder;
    use tikv::{config::TikvConfig, server::KvEngineFactoryBuilder};
    use tikv_util::{config::ReadableSize, time::Instant};

    use super::EnginesResourceInfo;

    #[test]
    fn test_engines_resource_info_update() {
        let mut config = TikvConfig::default();
        config.rocksdb.defaultcf.disable_auto_compactions = true;
        config.rocksdb.defaultcf.soft_pending_compaction_bytes_limit = Some(ReadableSize(1));
        config.rocksdb.writecf.soft_pending_compaction_bytes_limit = Some(ReadableSize(1));
        config.rocksdb.lockcf.soft_pending_compaction_bytes_limit = Some(ReadableSize(1));
        let env = Arc::new(Env::default());
        let path = Builder::new().prefix("test-update").tempdir().unwrap();
        let cache = config
            .storage
            .block_cache
            .build_shared_cache(config.storage.engine);

        let factory = KvEngineFactoryBuilder::new(env, &config, cache).build();
        let reg = TabletRegistry::new(Box::new(factory), path.path().join("tablets")).unwrap();

        for i in 1..6 {
            let ctx = TabletContext::with_infinite_region(i, Some(10));
            reg.load(ctx, true).unwrap();
        }

        let mut cached = reg.get(1).unwrap();
        let mut tablet = cached.latest().unwrap();
        // Prepare some data for two tablets of the same region. So we can test whether
        // we fetch the bytes from the latest one.
        for i in 1..21 {
            tablet.put_cf(CF_DEFAULT, b"key", b"val").unwrap();
            if i % 2 == 0 {
                tablet.flush_cf(CF_DEFAULT, true).unwrap();
            }
        }
        let old_pending_compaction_bytes = tablet
            .get_cf_pending_compaction_bytes(CF_DEFAULT)
            .unwrap()
            .unwrap();

        let ctx = TabletContext::with_infinite_region(1, Some(20));
        reg.load(ctx, true).unwrap();
        tablet = cached.latest().unwrap();

        for i in 1..11 {
            tablet.put_cf(CF_DEFAULT, b"key", b"val").unwrap();
            if i % 2 == 0 {
                tablet.flush_cf(CF_DEFAULT, true).unwrap();
            }
        }
        let new_pending_compaction_bytes = tablet
            .get_cf_pending_compaction_bytes(CF_DEFAULT)
            .unwrap()
            .unwrap();

        assert!(old_pending_compaction_bytes > new_pending_compaction_bytes);

        let engines_info = Arc::new(EnginesResourceInfo::new(reg, None, 10));

        let mut cached_latest_tablets = HashMap::default();
        engines_info.update(Instant::now(), &mut cached_latest_tablets);

        // The memory allocation should be reserved
        assert!(cached_latest_tablets.capacity() >= 5);
        // The tablet cache should be cleared
        assert!(cached_latest_tablets.is_empty());

        // The latest_normalized_pending_bytes should be equal to the pending compaction
        // bytes of tablet_1_20
        assert_eq!(
            (new_pending_compaction_bytes * 100) as u32,
            engines_info
                .latest_normalized_pending_bytes
                .load(Ordering::Relaxed)
        );
    }
}
