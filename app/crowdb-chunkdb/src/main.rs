// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-chunkdb` entry point.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use crowdb_chunkdb::allocator::{ChunkAllocator, DiskdbClientPool};
use crowdb_chunkdb::chunkdb_config::ChunkdbConfig;
use crowdb_chunkdb::conversion::io::ConversionDiskIo;
use crowdb_chunkdb::conversion::{ConversionCoordinator, MirrorToEcTaskHandler};
use crowdb_chunkdb::lifecycle::{ChunkLockMap, LifecycleHandler};
use crowdb_chunkdb::metrics::ChunkdbMetrics;
use crowdb_chunkdb::metrics::LifecycleMetrics;
use crowdb_chunkdb::range_guard::RangeGuard;
use crowdb_chunkdb::repair::{RepairCoordinator, RepairStripTaskHandler};
use crowdb_chunkdb::routing::{default_binding_table, BindingCache};
use crowdb_chunkdb::service::ChunkdbRpcService;
use crowdb_chunkdb::storage::ChunkStore;
use crowdb_chunkdb::task::{TaskExecutor, TaskHandler, TaskManager, TaskScanner, TaskStore};
use crowdb_chunkdb::topology::{
    build_snapshot, notify::NotifyHandler, refresh::run_refresh_loop, TopologyCache,
};
use crowdb_common::metrics::{MetricsRegistry, MetricsRunner};
use crowdb_kv_client::{
    ClientConfig, CrowdbKvClient, HardwareClient, RangeBindingClient, ServiceRegistryClient,
    WatchNotifyClient,
};
use tracing::{error, info, warn};

/// CROWDB chunkdb server CLI.
#[derive(Parser, Debug)]
#[command(name = "crowdb-chunkdb", about = "CROWDB distributed chunk manager")]
struct Cli {
    /// Config file path (TOML).
    #[arg(long)]
    config: String,

    /// HTTP management listen address (overrides config).
    #[arg(long)]
    http_addr: Option<String>,

    /// crowdb-rpc listener address (overrides config `rpc_listen_addr`).
    #[arg(long)]
    rpc_listen_addr: Option<String>,

    /// HTTP management port (overrides the port in config `http_listen_addr`).
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    http_port: Option<u16>,

    /// crowdb-rpc listener port (overrides the port in config `rpc_listen_addr`).
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    rpc_port: Option<u16>,

    /// Number of crowdb-rpc I/O worker threads. Overrides config.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    rpc_workers: Option<u32>,

    /// Log directory. Default: "log" (relative to CWD).
    #[arg(long)]
    log_dir: Option<String>,

    /// Log level for both Rust and C++ stacks. Default: "info"
    /// (or derived from `RUST_LOG`).
    #[arg(long)]
    log_level: Option<String>,

    /// Max log file size in MiB before rotation. Default: 30.
    #[arg(long, default_value_t = crowdb_common::logging::DEFAULT_LOG_MAX_FILE_MB)]
    log_max_file_mb: usize,

    /// Number of rotated log files to keep. Default: 5.
    #[arg(long, default_value_t = crowdb_common::logging::DEFAULT_LOG_MAX_FILES)]
    log_max_files: usize,

    /// Metrics flush interval in seconds. Zero disables metrics logging.
    #[arg(long, default_value_t = 5)]
    metrics_interval: u64,

    /// Also print logs to console (in addition to file logging).
    #[arg(short = 'l', long)]
    log: bool,

    /// Mirror C++ log lines at this level or above to stderr.
    #[arg(long)]
    log_stderr: Option<String>,
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    let args = Cli::parse();

    let log_dir = args.log_dir.clone().unwrap_or_else(|| "log".to_string());
    let cpp_level = args
        .log_level
        .clone()
        .unwrap_or_else(|| crowdb_common::logging::cpp_level_from_rust_log("info"));

    // Layered logging: INFO+ to rotating file, WARN+ to console.
    // RUST_LOG overrides both sinks for debugging.
    let _log_guards = if args.log {
        crowdb_common::logging::init_file_and_console_logging_split(
            &log_dir,
            "crowdb-chunkdb",
            args.log_max_file_mb,
            args.log_max_files,
            "info",
            "warn",
        )
        .expect("failed to initialize crowdb-chunkdb logging")
    } else {
        crowdb_common::logging::init_file_logging(
            &log_dir,
            "crowdb-chunkdb",
            args.log_max_file_mb,
            args.log_max_files,
            "info",
        )
        .expect("failed to initialize crowdb-chunkdb logging")
    };

    // Initialize the crowdb-rpc C++ spdlog logger (connection failures,
    // transport errors). No-op when the build has no spdlog.
    crowdb_rpc_ffi::init_logging(
        &log_dir,
        &cpp_level,
        args.log_max_file_mb,
        args.log_max_files,
        "crowdb-chunkdb-rpc",
    );

    if let Some(ref stderr_level) = args.log_stderr {
        crowdb_rpc_ffi::add_log_stderr(stderr_level);
    }

    let config = load_config(&args);
    info!(config = ?config, "crowdb-chunkdb starting");

    let (workflow_metrics, mut metrics_runner) = create_metrics(
        args.metrics_interval,
        &log_dir,
        args.log_max_file_mb,
        args.log_max_files,
    );
    if let Some(runner) = &mut metrics_runner {
        runner.start();
    }
    let workflow_metrics = Arc::new(workflow_metrics);

    let http_listen_addr: SocketAddr = config
        .server
        .http_listen_addr
        .parse()
        .expect("valid http_listen_addr");
    let rpc_listen_addr: SocketAddr = config
        .server
        .rpc_listen_addr
        .parse()
        .expect("valid rpc_listen_addr");

    // Build KV client for group-0 topology access.
    let mut kv_config = ClientConfig::new(config.server.kv_server_mgmt_seeds.clone());
    kv_config.pool_size_per_endpoint = config.server.kv_pool_size;
    kv_config.rpc_workers = config.server.kv_rpc_workers;
    let kv = Arc::new(CrowdbKvClient::new(kv_config));
    let hw = HardwareClient::from_shared(Arc::clone(&kv));
    let refresh_hw = HardwareClient::from_shared(Arc::clone(&kv));
    let watch = WatchNotifyClient::from_shared(Arc::clone(&kv));
    let svc = ServiceRegistryClient::from_shared(Arc::clone(&kv));
    let svc_keepalive = ServiceRegistryClient::from_shared(Arc::clone(&kv));

    // Topology cache + refresh loop + notify handler.
    let cache = TopologyCache::new();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

    let Some(initial_topology) = build_snapshot(&refresh_hw).await else {
        error!("initial topology refresh failed; refusing readiness");
        return;
    };
    cache.replace(initial_topology);

    let refresh_cache = cache.clone();
    let refresh_interval = Duration::from_secs(u64::from(config.topology.refresh_interval_secs));
    let refresh_stop = stop_rx.clone();
    let refresh_handle = tokio::spawn(async move {
        run_refresh_loop(refresh_cache, refresh_hw, refresh_interval, refresh_stop).await;
    });

    let notify_handler = NotifyHandler::new(watch, hw, cache.clone());
    let notify_stop = stop_rx.clone();
    let notify_handle = tokio::spawn(async move {
        notify_handler.run(notify_stop).await;
    });

    // Binding cache + chunk store.
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(0, 0));
    let store = Arc::new(ChunkStore::new(Arc::clone(&kv), bindings.clone()));
    let task_store = Arc::new(TaskStore::new(Arc::clone(&kv), bindings));

    // Range guard (R99): load chunkdb instance binding from group-0.
    // Falls back to allow-all when no binding table exists (v1 compat).
    let range_binding = RangeBindingClient::from_shared(Arc::clone(&kv));
    let range_guard = Arc::new(RangeGuard::new(config.range_guard.allow_all_when_empty));
    if let Err(e) = range_binding.refresh().await {
        warn!(error = %e, "failed to load chunkdb range binding from group-0 (using allow-all fallback)");
    }
    if !range_binding.is_empty() {
        let instance_id = config
            .server
            .instance_id
            .as_ref()
            .and_then(|s| s.parse::<u64>().ok());
        if let Some(iid) = instance_id {
            if let Err(e) = range_guard.load_from_group0(&kv, iid).await {
                warn!(error = %e, "failed to load owned ranges for instance {iid}");
            }
        }
    }
    // Spawn range binding notifier to keep the guard fresh.
    let _binding_notify_handle = match range_binding.spawn_notifier() {
        Ok(handle) => Some(handle),
        Err(e) => {
            warn!(error = %e, "failed to spawn range binding notifier");
            None
        }
    };

    // Service-registry keep-alive: register this chunkdb instance under
    // `/srv/chunkdb/<instance_id>` and heartbeat periodically. The
    // crowdb-kv-server group-0 leader's `BindingMonitor` reads these
    // entries to compute the chunkdb range binding table.
    let keepalive_handle = spawn_chunkdb_keepalive(
        svc_keepalive,
        config.server.instance_id.as_deref(),
        &config.server.rpc_listen_addr,
        Duration::from_secs(u64::from(config.server.keepalive_interval_secs)),
        stop_rx.clone(),
    );

    let range_refresh_guard = Arc::clone(&range_guard);
    let range_refresh_kv = Arc::clone(&kv);
    let range_refresh_stop = stop_rx.clone();
    let range_refresh_instance = config
        .server
        .instance_id
        .as_ref()
        .and_then(|value| value.parse::<u64>().ok());
    let range_refresh_handle = tokio::spawn(async move {
        let Some(instance_id) = range_refresh_instance else {
            return;
        };
        let mut stop = range_refresh_stop;
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(error) = range_refresh_guard.load_from_group0(&range_refresh_kv, instance_id).await {
                        warn!(%error, instance_id, "periodic range guard refresh failed");
                    }
                }
                changed = stop.changed() => {
                    if changed.is_ok() && *stop.borrow() {
                        return;
                    }
                }
            }
        }
    });

    // Diskdb client pool + chunk allocator.
    let pool = Arc::new(DiskdbClientPool::with_transport(
        svc,
        config.server.diskdb_pool_size,
        config.server.diskdb_rpc_workers,
    ));
    if let Err(error) = pool.refresh_endpoints().await {
        error!(%error, "initial diskdb discovery failed; refusing readiness");
        return;
    }
    pool.update_disk_id_lookup(&cache.snapshot().disk_groups());
    let allocator =
        Arc::new(ChunkAllocator::new(Arc::clone(&pool)).with_metrics(Arc::clone(&workflow_metrics)));

    // Per-chunk lock map + payload cache (R100).
    let lifecycle_metrics = Arc::new(LifecycleMetrics::new());
    let hold_warn_threshold = Duration::from_millis(config.lifecycle.lock_hold_warn_threshold_ms);
    let lock_map = Arc::new(ChunkLockMap::new(
        config.lifecycle.cache_capacity,
        Arc::clone(&lifecycle_metrics),
        hold_warn_threshold,
    ));

    // Spawn sweep task for idle lock reaping.
    let sweep_interval = Duration::from_secs(u64::from(config.lifecycle.sweep_chunk_lock_interval_secs));
    let sweep_handle = tokio::spawn(run_sweep_loop(
        Arc::clone(&lock_map),
        sweep_interval,
        stop_rx.clone(),
    ));

    let reservation_blocks = range_guard.quota_share(config.reservation.max_blocks);
    let reservation_bytes = range_guard.quota_share(config.reservation.max_bytes);

    // Lifecycle handler.
    let handler = Arc::new(
        LifecycleHandler::new(Arc::clone(&store), allocator, cache)
            .with_range_guard(Arc::clone(&range_guard))
            .with_locks(Arc::clone(&lock_map))
            .with_metrics(Arc::clone(&workflow_metrics))
            .with_reservation_limits(reservation_blocks, reservation_bytes)
            .with_allow_unsafe_ec(config.placement.allow_unsafe_ec)
            .with_layout_validity(Duration::from_millis(config.lifecycle.layout_validity_ms)),
    );
    match handler.rebuild_reservation_admission().await {
        Ok((blocks, bytes)) => info!(blocks, bytes, "reservation admission rebuilt"),
        Err(error) => {
            error!(%error, "reservation admission rebuild failed");
            return;
        }
    }
    match handler.reconcile_pending_chunks().await {
        Ok(count) => info!(count, "pending chunk allocations reconciled"),
        Err(error) => {
            error!(%error, "pending chunk allocation reconciliation failed");
            return;
        }
    }
    let writer_lease_sweep_handle = tokio::spawn(run_writer_lease_sweep_loop(
        Arc::clone(&handler),
        sweep_interval,
        stop_rx.clone(),
    ));

    // Build the crowdb-rpc server. The RpcServer listens on the RPC
    // port and dispatches to ChunkdbRpcService handlers.
    let rpc_rt_handle = tokio::runtime::Handle::current();
    let task_manager = Arc::new(TaskManager::new(
        Arc::clone(&task_store),
        config
            .server
            .instance_id
            .as_ref()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1),
        config.conversion.task_lease_secs.saturating_mul(1_000),
    ));
    let conversion = Arc::new(
        ConversionCoordinator::new(Arc::clone(&handler), Arc::clone(&task_store))
            .with_wake(task_manager.wake_handle())
            .with_policy(
                config.conversion.data_num,
                config.conversion.code_num,
                config.conversion.min_mirror_strips,
                config.conversion.min_seal_age_secs.saturating_mul(1_000),
            ),
    );
    let reservation_reconcile_handle = {
        let conversion = Arc::clone(&conversion);
        let mut stop = stop_rx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match conversion.reconcile_reservations(256, unix_time_ms()).await {
                            Ok(reconciled) if reconciled > 0 => {
                                info!(reconciled, "expired strip reservations reconciled");
                            }
                            Ok(_) => {}
                            Err(error) => warn!(%error, "strip reservation reconciliation failed"),
                        }
                    }
                    changed = stop.changed() => {
                        if changed.is_err() || *stop.borrow() {
                            return;
                        }
                    }
                }
            }
        })
    };
    let reservation_admission_handle = {
        let handler = Arc::clone(&handler);
        let range_guard = Arc::clone(&range_guard);
        let mut stop = stop_rx.clone();
        let interval = Duration::from_secs(config.reservation.scan_interval_secs);
        let max_blocks = config.reservation.max_blocks;
        let max_bytes = config.reservation.max_bytes;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        handler.update_reservation_limits(
                            range_guard.quota_share(max_blocks),
                            range_guard.quota_share(max_bytes),
                        );
                    }
                    changed = stop.changed() => {
                        if changed.is_err() || *stop.borrow() {
                            return;
                        }
                    }
                }
            }
        })
    };
    let conversion_scan_handle = config.conversion.enabled.then(|| {
        let conversion = Arc::clone(&conversion);
        let mut stop = stop_rx.clone();
        let interval = Duration::from_secs(config.conversion.scan_interval_secs);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match conversion.trigger_configured_batch(false, 256, unix_time_ms()).await {
                            Ok(accepted_chunks) if accepted_chunks > 0 => {
                                info!(accepted_chunks, "automatic mirror-to-EC scan admitted chunks");
                            }
                            Ok(_) => {}
                            Err(error) => warn!(%error, "automatic mirror-to-EC scan failed"),
                        }
                    }
                    changed = stop.changed() => {
                        if changed.is_err() || *stop.borrow() {
                            return;
                        }
                    }
                }
            }
        })
    });
    let repair = Arc::new(
        RepairCoordinator::new(Arc::clone(&handler), Arc::clone(&task_store))
            .with_wake(task_manager.wake_handle())
            .with_metrics(Arc::clone(&workflow_metrics.repair)),
    );
    let repair_scan_handle = config.repair.enabled.then(|| {
        let repair = Arc::clone(&repair);
        let mut stop = stop_rx.clone();
        let interval = Duration::from_secs(config.repair.scan_interval_secs);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match repair.scan_batch(256, unix_time_ms()).await {
                            Ok(accepted) if accepted > 0 => {
                                info!(accepted, "read-repair scan admitted tasks");
                            }
                            Ok(_) => {}
                            Err(error) => warn!(%error, "read-repair scan failed"),
                        }
                    }
                    changed = stop.changed() => {
                        if changed.is_err() || *stop.borrow() {
                            return;
                        }
                    }
                }
            }
        })
    });
    let (task_scanner_handle, conversion_route_refresh_handle) = match ConversionDiskIo::connect(
        &ServiceRegistryClient::from_shared(Arc::clone(&kv)),
        &HardwareClient::from_shared(Arc::clone(&kv)),
    )
    .await
    {
        Ok(io) => {
            let io = Arc::new(io);
            let conversion_task_handler = Arc::new(MirrorToEcTaskHandler::new(
                Arc::clone(&handler),
                Arc::clone(&task_store),
                Arc::clone(&io),
                Arc::clone(&workflow_metrics.conversion),
                config.conversion.max_bandwidth_mbps,
                config.conversion.max_concurrency,
            ));
            let repair_task_handler = Arc::new(RepairStripTaskHandler::new(
                Arc::clone(&handler),
                Arc::clone(&io),
                config.repair.memory_bytes,
                config.repair.max_concurrency,
                config.repair.allow_unsafe_placement,
                Arc::clone(&workflow_metrics.repair),
            ));
            let task_handlers: Vec<Arc<dyn TaskHandler>> = vec![conversion_task_handler, repair_task_handler];
            let executor = Arc::new(
                TaskExecutor::new(
                    Arc::clone(&task_manager),
                    config
                        .conversion
                        .max_concurrency
                        .saturating_add(config.repair.max_concurrency),
                    task_handlers,
                )
                .expect("unique conversion task handler"),
            );
            let scanner = TaskScanner::new(
                Arc::clone(&task_store),
                Arc::clone(&task_manager),
                executor,
                256,
                Duration::from_secs(1),
            );
            let scanner_stop = stop_rx.clone();
            let scanner_handle = tokio::spawn(async move { scanner.run(scanner_stop).await });
            let service = ServiceRegistryClient::from_shared(Arc::clone(&kv));
            let hardware = HardwareClient::from_shared(Arc::clone(&kv));
            let mut refresh_stop = stop_rx.clone();
            let refresh_interval = Duration::from_secs(u64::from(config.topology.refresh_interval_secs));
            let refresh_handle = tokio::spawn(async move {
                let mut ticker = tokio::time::interval(refresh_interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            if let Err(error) = io.refresh(&service, &hardware).await {
                                warn!(%error, "background conversion DiskIO route refresh failed");
                            }
                        }
                        changed = refresh_stop.changed() => {
                            if changed.is_err() || *refresh_stop.borrow() {
                                return;
                            }
                        }
                    }
                }
            });
            (Some(scanner_handle), Some(refresh_handle))
        }
        Err(error) => {
            warn!(%error, "background conversion DiskIO is unavailable; client fast path remains enabled");
            (None, None)
        }
    };
    let rpc_service = Arc::new(
        ChunkdbRpcService::new(Arc::clone(&handler), Arc::clone(&workflow_metrics), rpc_rt_handle)
            .with_conversion(Arc::clone(&conversion)),
    );
    let rpc_server = Arc::new(crowdb_rpc_ffi::RpcServer::with_engines(
        None,
        1,
        config.server.rpc_workers,
    ));
    rpc_server
        .listen(
            rpc_listen_addr.ip().to_string().as_str(),
            i32::from(rpc_listen_addr.port()),
        )
        .expect("rpc server listen");
    rpc_service.register_handlers(&rpc_server);
    rpc_server.start();
    info!(%rpc_listen_addr, "crowdb-rpc server listening (R116 migration)");

    // Start HTTP health + metrics + cache invalidation server.
    let readiness = HttpReadiness {
        range_guard: Arc::clone(&range_guard),
        kv: Arc::clone(&kv),
        instance_id: config
            .server
            .instance_id
            .as_ref()
            .and_then(|value| value.parse().ok()),
    };
    let http_handle = tokio::spawn(run_http_server(
        http_listen_addr,
        readiness,
        Arc::clone(&lock_map),
        Arc::clone(&workflow_metrics.conversion),
        Arc::clone(&workflow_metrics.repair),
        Arc::clone(&conversion),
    ));

    let rpc_server_stop = Arc::clone(&rpc_server);
    let _ = tokio::signal::ctrl_c().await;
    info!("received shutdown signal");
    rpc_server_stop.stop();
    let _ = stop_tx.send(true);
    let _ = http_handle.await;
    let _ = refresh_handle.await;
    let _ = notify_handle.await;
    let _ = writer_lease_sweep_handle.await;
    let _ = reservation_reconcile_handle.await;
    let _ = reservation_admission_handle.await;
    if let Some(handle) = task_scanner_handle {
        let _ = handle.await;
    }
    if let Some(handle) = conversion_route_refresh_handle {
        let _ = handle.await;
    }
    if let Some(handle) = conversion_scan_handle {
        let _ = handle.await;
    }
    if let Some(handle) = repair_scan_handle {
        let _ = handle.await;
    }
    let _ = range_refresh_handle.await;
    let _ = sweep_handle.await;
    if let Some(h) = keepalive_handle {
        let _ = h.await;
    }
    if let Some(runner) = &mut metrics_runner {
        runner.stop().await;
    }
    info!("crowdb-chunkdb stopped");
}

fn create_metrics(
    interval_secs: u64,
    log_dir: &str,
    max_file_mb: usize,
    max_files: usize,
) -> (ChunkdbMetrics, Option<MetricsRunner>) {
    if interval_secs == 0 {
        let mut registry = MetricsRegistry::new();
        return (ChunkdbMetrics::register(&mut registry), None);
    }
    let file = crowdb_common::logging::open_metrics_log(log_dir, "crowdb-chunkdb", max_file_mb, max_files)
        .expect("failed to open metrics log file");
    let mut runner = MetricsRunner::new(file, interval_secs);
    runner.set_cpp_flush(|writer, window_secs, timestamp, rust_width, count_w, tps_w| {
        let cpp_width = crowdb_rpc_ffi::cpp_global_metrics_max_name_len();
        if let Some(metrics) = crowdb_rpc_ffi::flush_cpp_global_metrics(
            window_secs,
            timestamp,
            "cpp-rpc",
            rust_width.max(cpp_width),
            count_w,
            tps_w,
        ) {
            let _ = std::io::Write::write_all(writer, metrics.as_bytes());
        }
    });
    let metrics = {
        let mut registry = runner
            .registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ChunkdbMetrics::register(&mut registry)
    };
    (metrics, Some(runner))
}

/// Periodic sweep loop — reaps idle chunk locks.
async fn run_sweep_loop(
    locks: Arc<ChunkLockMap>,
    interval: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut timer = tokio::time::interval(interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = timer.tick() => {
                locks.reap_idle();
            }
            _ = stop.changed() => {
                if *stop.borrow() {
                    info!("sweep task stopping");
                    break;
                }
            }
        }
    }
}

async fn run_writer_lease_sweep_loop(
    handler: Arc<LifecycleHandler>,
    interval: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(error) = handler.seal_expired_writer_chunks().await {
                    warn!(%error, "shared writer lease sweep failed");
                }
                if let Err(error) = handler.reconcile_pending_chunks().await {
                    warn!(%error, "chunk cleanup reconciliation failed");
                }
            }
            changed = stop.changed() => {
                if changed.is_ok() && *stop.borrow() {
                    return;
                }
            }
        }
    }
}

/// Spawn a chunkdb service-registry keep-alive loop. Registers the
/// instance under `/srv/chunkdb/<instance_id>` and heartbeats every
/// `interval`. Stops on the `stop` signal, unregistering on clean
/// shutdown. `instance_id_str` parses to a `u64`; if it is `None` or
/// unparseable, the loop is skipped with a warning (the binding
/// monitor will not see this instance).
fn spawn_chunkdb_keepalive(
    svc: ServiceRegistryClient,
    instance_id_str: Option<&str>,
    listen_addr: &str,
    interval: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Option<tokio::task::JoinHandle<()>> {
    let instance_id = instance_id_str?.parse::<u64>().ok()?;
    let rpc_endpoint = format!("http://{listen_addr}");
    let handle = tokio::spawn(async move {
        if let Err(e) = svc.register_chunkdb(instance_id, &rpc_endpoint).await {
            warn!(error = %e, "chunkdb keep-alive: initial register failed");
        } else {
            info!(instance_id, "chunkdb keep-alive: registered");
        }
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = svc.heartbeat_chunkdb(instance_id, &rpc_endpoint).await {
                        warn!(error = %e, "chunkdb keep-alive: heartbeat failed");
                    }
                }
                _ = stop.changed() => {
                    if *stop.borrow() {
                        info!(instance_id, "chunkdb keep-alive: shutting down; unregistering");
                        let _ = tokio::time::timeout(
                            Duration::from_secs(1),
                            svc.unregister("chunkdb", instance_id),
                        ).await;
                        break;
                    }
                }
            }
        }
    });
    Some(handle)
}

struct HttpReadiness {
    range_guard: Arc<RangeGuard>,
    kv: Arc<CrowdbKvClient>,
    instance_id: Option<u64>,
}

/// HTTP server — health, metrics, cache invalidation endpoints.
async fn run_http_server(
    addr: SocketAddr,
    readiness: HttpReadiness,
    locks: Arc<ChunkLockMap>,
    conversion_metrics: Arc<crowdb_chunkdb::metrics::ConversionMetrics>,
    repair_metrics: Arc<crowdb_chunkdb::metrics::RepairMetrics>,
    conversion: Arc<ConversionCoordinator>,
) {
    let app = axum::Router::new()
        .route(
            "/ready",
            axum::routing::get(move || {
                let range_guard = Arc::clone(&readiness.range_guard);
                let kv = Arc::clone(&readiness.kv);
                async move { ready_response(&range_guard, &kv, readiness.instance_id).await }
            }),
        )
        .route("/health", axum::routing::get(|| async { "ok" }))
        .route(
            "/metrics",
            axum::routing::get({
                let locks = Arc::clone(&locks);
                move || async move {
                    let snap = locks.metrics_snapshot();
                    axum::Json(snap)
                }
            }),
        )
        .route(
            "/conversion_metrics",
            axum::routing::get(move || {
                let metrics = Arc::clone(&conversion_metrics);
                async move { axum::Json(metrics.snapshot()) }
            }),
        )
        .route(
            "/repair_metrics",
            axum::routing::get(move || {
                let metrics = Arc::clone(&repair_metrics);
                async move { axum::Json(metrics.snapshot()) }
            }),
        )
        .route(
            "/convert_chunk",
            axum::routing::post({
                let conversion = Arc::clone(&conversion);
                move |axum::Json(body): axum::Json<ConvertChunkBody>| {
                    let conversion = Arc::clone(&conversion);
                    async move {
                        match conversion
                            .trigger_configured_chunk(body.chunk_id, unix_time_ms())
                            .await
                        {
                            Ok(accepted_groups) => axum::Json(serde_json::json!({
                                "accepted_groups": accepted_groups
                            })),
                            Err(error) => axum::Json(serde_json::json!({ "error": error.to_string() })),
                        }
                    }
                }
            }),
        )
        .route(
            "/convert_all",
            axum::routing::post(move |axum::Json(body): axum::Json<ConvertAllBody>| {
                let conversion = Arc::clone(&conversion);
                async move {
                    match conversion
                        .trigger_configured_batch(body.sealed_only, body.max_chunks, unix_time_ms())
                        .await
                    {
                        Ok(accepted_chunks) => axum::Json(serde_json::json!({
                            "accepted_chunks": accepted_chunks
                        })),
                        Err(error) => axum::Json(serde_json::json!({ "error": error.to_string() })),
                    }
                }
            }),
        )
        .route(
            "/invalidate_chunk",
            axum::routing::post({
                let locks = Arc::clone(&locks);
                move |axum::Json(body): axum::Json<InvalidateChunkBody>| async move {
                    if let Some(id) = body.chunk_id {
                        let invalidated = locks.invalidate_chunk(&id);
                        axum::Json(serde_json::json!({ "invalidated": invalidated }))
                    } else {
                        axum::Json(serde_json::json!({ "invalidated": false }))
                    }
                }
            }),
        )
        .route(
            "/invalidate_range",
            axum::routing::post({
                let locks = Arc::clone(&locks);
                move |axum::Json(body): axum::Json<InvalidateRangeBody>| async move {
                    let count = locks.invalidate_range(body.bucket_start, body.bucket_end);
                    axum::Json(serde_json::json!({ "invalidated_count": count }))
                }
            }),
        );
    info!(%addr, "HTTP server listening");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind http addr");
    axum::serve(listener, app).await.expect("HTTP server error");
}

async fn ready_response(
    range_guard: &RangeGuard,
    kv: &CrowdbKvClient,
    instance_id: Option<u64>,
) -> (axum::http::StatusCode, &'static str) {
    if let Some(instance_id) = instance_id {
        if range_guard.load_from_group0(kv, instance_id).await.is_err() {
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "range ownership refresh failed",
            );
        }
    }
    if range_guard.is_ready() {
        (axum::http::StatusCode::OK, "ok")
    } else {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "range ownership pending",
        )
    }
}

fn load_config(args: &Cli) -> ChunkdbConfig {
    let config_path = &args.config;
    let mut config =
        crowdb_common::config::load_from_file::<ChunkdbConfig>(std::path::Path::new(config_path))
            .unwrap_or_else(|e| panic!("failed to load config file {config_path}: {e}"));

    if let Some(addr) = &args.http_addr {
        config.server.http_listen_addr.clone_from(addr);
    }
    if let Some(addr) = &args.rpc_listen_addr {
        config.server.rpc_listen_addr.clone_from(addr);
    }
    if let Some(port) = args.http_port {
        config.server.http_listen_addr = replace_port(&config.server.http_listen_addr, port);
    }
    if let Some(port) = args.rpc_port {
        config.server.rpc_listen_addr = replace_port(&config.server.rpc_listen_addr, port);
    }
    if let Some(rpc_workers) = args.rpc_workers {
        config.server.rpc_workers = rpc_workers;
    }

    crowdb_common::config::BaseConfig::validate(&config)
        .unwrap_or_else(|e| panic!("invalid config after CLI overrides: {e}"));

    config
}

/// Replace the port portion of a `host:port` address string.
fn replace_port(addr: &str, port: u16) -> String {
    if let Some(idx) = addr.rfind(':') {
        format!("{}:{port}", &addr[..idx])
    } else {
        format!("0.0.0.0:{port}")
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// Request body for `POST /invalidate_chunk`.
#[derive(serde::Deserialize)]
struct InvalidateChunkBody {
    chunk_id: Option<crowdb_protocol::common::ChunkId>,
}

/// Request body for `POST /invalidate_range`.
#[derive(serde::Deserialize)]
struct InvalidateRangeBody {
    bucket_start: u16,
    bucket_end: u16,
}

#[derive(serde::Deserialize)]
struct ConvertChunkBody {
    chunk_id: crowdb_protocol::common::ChunkId,
}

#[derive(serde::Deserialize)]
struct ConvertAllBody {
    #[serde(default)]
    sealed_only: bool,
    #[serde(default)]
    max_chunks: u32,
}
