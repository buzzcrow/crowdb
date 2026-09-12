// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-chunk-kv-server` process entry point.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use crowdb_chunk_kv_server::{
    management_router, CatalogPublisher, ChunkKvRpcService, ChunkKvServerConfig, ChunkKvService,
    ChunkKvStorage, DomainMonitorRegistry, Group0ControlStore, ManagementState,
};
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::chunk_kv::{EnsureDomainMonitorOutcome, EnsureDomainMonitorRequest};
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(
    name = "crowdb-chunk-kv-server",
    about = "CROWDB range-partitioned chunk-backed KV server"
)]
struct Cli {
    /// Server configuration file.
    #[arg(long)]
    config: PathBuf,

    /// HTTP management listen address override.
    #[arg(long)]
    http_addr: Option<String>,

    /// Data RPC listen address override.
    #[arg(long)]
    rpc_addr: Option<String>,

    /// Log directory.
    #[arg(long, default_value = "log")]
    log_dir: String,

    /// Also emit warning and error logs to the console.
    #[arg(short = 'l', long)]
    log: bool,

    /// Maximum log file size in MiB before rotation.
    #[arg(long, default_value_t = crowdb_common::logging::DEFAULT_LOG_MAX_FILE_MB)]
    log_max_file_mb: usize,

    /// Number of rotated log files to retain.
    #[arg(long, default_value_t = crowdb_common::logging::DEFAULT_LOG_MAX_FILES)]
    log_max_files: usize,
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    let args = Cli::parse();
    let _log_guards = if args.log {
        crowdb_common::logging::init_file_and_console_logging_split(
            &args.log_dir,
            "crowdb-chunk-kv-server",
            args.log_max_file_mb,
            args.log_max_files,
            "info",
            "warn",
        )
    } else {
        crowdb_common::logging::init_file_logging(
            &args.log_dir,
            "crowdb-chunk-kv-server",
            args.log_max_file_mb,
            args.log_max_files,
            "info",
        )
    }
    .expect("failed to initialize chunk KV server logging");
    crowdb_tree_ffi::ct_init_logging(
        &args.log_dir,
        "info",
        args.log_max_file_mb,
        args.log_max_files,
        "crowdb-chunk-kv-server-tree",
    );
    crowdb_rpc_ffi::init_logging(
        &args.log_dir,
        "info",
        args.log_max_file_mb,
        args.log_max_files,
        "crowdb-chunk-kv-server-rpc",
    );

    let mut config = match ChunkKvServerConfig::load(&args.config) {
        Ok(config) => config,
        Err(error) => {
            error!(path = %args.config.display(), %error, "failed to load configuration");
            return;
        }
    };
    if let Some(http_addr) = args.http_addr {
        config.http_listen_addr = http_addr;
    }
    if let Some(rpc_addr) = args.rpc_addr {
        config.rpc_listen_addr = rpc_addr;
    }
    if let Err(error) = config.validate() {
        error!(%error, "configuration overrides are invalid");
        return;
    }

    let http_addr: SocketAddr = config
        .http_listen_addr
        .parse()
        .expect("validated HTTP listen address");
    let rpc_addr: SocketAddr = config
        .rpc_listen_addr
        .parse()
        .expect("validated RPC listen address");
    let rpc_advertise_addr: SocketAddr = config
        .rpc_advertise_addr
        .parse()
        .expect("validated RPC advertise address");
    info!(
        instance_id = config.instance_id,
        %http_addr,
        %rpc_addr,
        %rpc_advertise_addr,
        "crowdb-chunk-kv-server starting"
    );

    let storage = match ChunkKvStorage::connect(&config).await {
        Ok(storage) => storage,
        Err(error) => {
            error!(%error, "failed to connect production chunk storage");
            return;
        }
    };
    let service = match ChunkKvService::new(config.instance_id, config.max_hosted_partitions) {
        Ok(service) => Arc::new(service),
        Err(error) => {
            error!(%error, "failed to initialize chunk KV service");
            return;
        }
    };
    let control_store = Arc::new(Group0ControlStore::from_client(Arc::clone(storage.kv())));
    let monitor_registry = DomainMonitorRegistry::new(
        control_store.clone(),
        vec![crowdb_chunk_kv_server::serving::monitor::SupportedMonitor {
            domain: "chunk-kv".into(),
            driver_version: 1,
            max_capability_version: 1,
        }],
    );
    let ensure_request = EnsureDomainMonitorRequest {
        descriptor: config.monitor.clone(),
    };
    match monitor_registry.ensure(&ensure_request).await {
        Ok(EnsureDomainMonitorOutcome::Created | EnsureDomainMonitorOutcome::AlreadyExists) => {}
        Ok(outcome) => {
            error!(?outcome, "chunk KV monitor registration was rejected");
            return;
        }
        Err(error) => {
            error!(%error, "failed to persist chunk KV monitor registration");
            return;
        }
    }

    let catalog = Arc::new(CatalogPublisher::new(control_store.clone()));
    match catalog.load_current().await {
        Ok(Some((head, pages))) => {
            if let Err(error) = service.install_catalog(&head, &pages) {
                error!(%error, "failed to install initial chunk KV catalog");
                return;
            }
            info!(generation = head.generation, "installed initial chunk KV catalog");
        }
        Ok(None) => warn!("chunk KV catalog is not published; service remains unready"),
        Err(error) => {
            error!(%error, "failed to load initial chunk KV catalog");
            return;
        }
    }
    let refresh_service = Arc::clone(&service);
    let refresh_catalog = Arc::clone(&catalog);
    let refresh_interval = std::time::Duration::from_millis(config.catalog_refresh_interval_ms);
    let refresh_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(refresh_interval);
        interval.tick().await;
        loop {
            interval.tick().await;
            match refresh_catalog.load_current().await {
                Ok(Some((head, pages))) => match refresh_service.install_catalog(&head, &pages) {
                    Ok(()) => info!(
                        generation = head.generation,
                        "installed refreshed chunk KV catalog"
                    ),
                    Err(crowdb_chunk_kv_server::CatalogError::GenerationConflict) => {}
                    Err(error) => warn!(%error, "rejected refreshed chunk KV catalog"),
                },
                Ok(None) => warn!("chunk KV catalog head is absent; retaining installed catalog"),
                Err(error) => warn!(%error, "catalog refresh failed; retaining installed catalog"),
            }
        }
    });

    let service_registry = Arc::new(ServiceRegistryClient::from_shared(Arc::clone(storage.kv())));
    let capacity_bytes = u64::try_from(config.max_hosted_partitions)
        .unwrap_or(u64::MAX)
        .saturating_mul(config.balance.target_partition_bytes);
    let initial_observation = service.registry_observation(capacity_bytes, 0);
    if let Err(error) = service_registry
        .register_chunk_kv(
            config.instance_id,
            &rpc_advertise_addr.to_string(),
            &initial_observation,
        )
        .await
    {
        error!(%error, "failed to register chunk KV instance");
        refresh_task.abort();
        return;
    }
    install_latest_grant(&control_store, &service, &config).await;
    let heartbeat_service = Arc::clone(&service);
    let heartbeat_registry = Arc::clone(&service_registry);
    let heartbeat_store = Arc::clone(&control_store);
    let heartbeat_config = config.clone();
    let heartbeat_endpoint = rpc_advertise_addr.to_string();
    let heartbeat_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(
            heartbeat_config.monitor.heartbeat_interval_ms,
        ));
        interval.tick().await;
        let mut previous_requests = heartbeat_service.metrics().snapshot().requests;
        let mut previous_ms = heartbeat_service.monotonic_ms();
        loop {
            interval.tick().await;
            let now_ms = heartbeat_service.monotonic_ms();
            let requests = heartbeat_service.metrics().snapshot().requests;
            let elapsed_ms = now_ms.saturating_sub(previous_ms).max(1);
            let request_rate = requests.saturating_sub(previous_requests).saturating_mul(1_000) / elapsed_ms;
            previous_requests = requests;
            previous_ms = now_ms;
            let observation = heartbeat_service.registry_observation(capacity_bytes, request_rate);
            if let Err(error) = heartbeat_registry
                .heartbeat_chunk_kv(heartbeat_config.instance_id, &heartbeat_endpoint, &observation)
                .await
            {
                warn!(%error, "chunk KV heartbeat failed");
            }
            install_latest_grant(&heartbeat_store, &heartbeat_service, &heartbeat_config).await;
        }
    });

    let rpc_server = Arc::new(crowdb_rpc_ffi::RpcServer::with_engines(
        None,
        1,
        config.rpc_workers,
    ));
    if let Err(error) = rpc_server.listen(&rpc_addr.ip().to_string(), i32::from(rpc_addr.port())) {
        error!(%rpc_addr, %error, "data RPC bind failed");
        return;
    }
    let rpc_service = Arc::new(ChunkKvRpcService::new(
        Arc::clone(&service),
        tokio::runtime::Handle::current(),
    ));
    rpc_service.register_handlers(&rpc_server);
    rpc_server.start();
    info!(%rpc_addr, "data RPC server listening");

    let listener = match tokio::net::TcpListener::bind(http_addr).await {
        Ok(listener) => listener,
        Err(error) => {
            error!(%http_addr, %error, "HTTP management bind failed");
            return;
        }
    };
    info!(%http_addr, "HTTP management server listening");

    let shutdown_service = Arc::clone(&service);
    let shutdown_rpc = Arc::clone(&rpc_server);
    let app = management_router(ManagementState::new(Arc::clone(&service)));
    if let Err(error) = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            shutdown_service.begin_drain();
            shutdown_rpc.stop();
            info!("chunk KV service admission drained");
        })
        .await
    {
        error!(%error, "HTTP management server failed");
    }
    heartbeat_task.abort();
    refresh_task.abort();
    if let Err(error) = service_registry.unregister("chunk-kv", config.instance_id).await {
        warn!(%error, "failed to unregister chunk KV instance");
    }
}

async fn install_latest_grant(
    store: &Group0ControlStore,
    service: &ChunkKvService,
    config: &ChunkKvServerConfig,
) {
    match store.load_serving_grant(config.instance_id).await {
        Ok(Some(grant)) => {
            if let Err(error) =
                service
                    .authority()
                    .install(grant, &config.monitor, wall_time_ms(), service.monotonic_ms())
            {
                warn!(%error, "rejected chunk KV serving grant");
            }
        }
        Ok(None) => service.authority().clear(),
        Err(error) => warn!(%error, "serving-grant refresh failed; retaining local lease deadline"),
    }
}

fn wall_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
}

async fn shutdown_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                warn!(%error, "SIGINT handler failed");
            }
        }
        _ = terminate.recv() => {}
    }
    info!("received shutdown signal");
}
