use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crowdb_access_iceberg::catalog::{
    Capabilities, CatalogError, CatalogLifecycle, CatalogRepository, ManagementPrivilege, RootState,
    RoutedCatalogStore,
};
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};
use crowdb_access_iceberg::storage::{connect, foreground_blocks, IcebergLargeWriteSettings};
use crowdb_access_iceberg::wire::BearerAuthenticator;
use crowdb_access_s3::native_buffer::NativeBodyAllocator;
use crowdb_chunk_client::ChunkIoClient;
use tokio::net::TcpListener;
use tokio::sync::watch;

use super::{serve, IcebergHttpService};
use crate::config::{load_args, AccessConfig};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub struct IcebergRuntimeConfig {
    pub listen: String,
    pub management_seeds: Vec<String>,
    pub authentication: BearerAuthenticator,
}

impl IcebergRuntimeConfig {
    /// # Errors
    /// Rejects missing/invalid credentials, seeds or listener configuration.
    pub fn from_env() -> Result<Self, BoxError> {
        Self::from_config(&AccessConfig::default())
    }

    /// # Errors
    /// Rejects missing/invalid credentials, seeds or listener configuration.
    pub fn from_config(access: &AccessConfig) -> Result<Self, BoxError> {
        let management_seeds: Vec<_> = if access.common.management_seeds.is_empty() {
            let seeds = std::env::var("CROWDB_MANAGEMENT_SEEDS")?;
            if seeds.len() > 8192 {
                return Err("management seed configuration is oversized".into());
            }
            seeds
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        } else {
            access.common.management_seeds.clone()
        };
        if management_seeds.is_empty() || management_seeds.len() > 16 {
            return Err("one to sixteen management seeds are required".into());
        }
        let authentication = BearerAuthenticator::new(
            &std::env::var("CROWDB_ICEBERG_READ_TOKEN")?,
            &std::env::var("CROWDB_ICEBERG_WRITE_TOKEN")
                .map_err(|_| "CROWDB_ICEBERG_WRITE_TOKEN must be set")?,
            &std::env::var("CROWDB_ICEBERG_MANAGE_TOKEN")?,
            &std::env::var("CROWDB_ICEBERG_CLEAR_TOKEN")?,
        )
        .map_err(|error| format!("invalid Iceberg bearer credential configuration: {error}"))?;
        let listen = access
            .iceberg
            .listen
            .clone()
            .or_else(|| std::env::var("CROWDB_ICEBERG_LISTEN").ok())
            .unwrap_or_else(|| "127.0.0.1:8181".into());
        let _: std::net::SocketAddr = listen.parse()?;
        Ok(Self {
            listen,
            management_seeds,
            authentication,
        })
    }
}

/// # Errors
/// Returns configuration, authentication, storage, management or listener failures.
pub async fn run(arguments: Vec<String>) -> Result<(), BoxError> {
    run_with_shutdown(arguments, None).await
}

/// Runs Iceberg with an optional coordinated process shutdown signal.
///
/// # Errors
/// Returns configuration, authentication, storage, management or listener failures.
pub async fn run_with_shutdown(
    arguments: Vec<String>,
    shutdown: Option<watch::Receiver<bool>>,
) -> Result<(), BoxError> {
    let (access_config, arguments) = load_args(arguments)?;
    let config = IcebergRuntimeConfig::from_config(&access_config)?;
    if arguments.len() > 7 {
        return Err("too many Iceberg command arguments".into());
    }
    let (repository, store, chunks) = connect(
        config.management_seeds.clone(),
        access_config.read.policy(),
        access_config.iceberg_small_write().policy(),
        access_config.common.diskio_connections_per_endpoint,
        access_config.common.diskio_rpc_workers,
    )
    .await?;
    let result = if arguments.is_empty() || arguments == ["serve"] {
        Box::pin(start_listener(
            config,
            repository,
            store,
            chunks.clone(),
            access_config,
            shutdown,
        ))
        .await
    } else if arguments.first().is_some_and(|argument| argument == "gc") {
        super::gc_control::manage(
            &repository,
            store.clone(),
            &config.authentication,
            &arguments[1..],
            &access_config.iceberg.gc,
        )
        .await
    } else {
        manage(&repository, &config.authentication, &arguments).await
    };
    let shutdown = chunks.shutdown_small_writes().await;
    result?;
    shutdown?;
    Ok(())
}

async fn start_listener(
    runtime: IcebergRuntimeConfig,
    repository: Arc<CatalogRepository>,
    store: Arc<RoutedCatalogStore>,
    chunks: ChunkIoClient,
    access_config: AccessConfig,
    shutdown: Option<watch::Receiver<bool>>,
) -> Result<(), BoxError> {
    let IcebergRuntimeConfig {
        listen: address,
        management_seeds,
        authentication,
    } = runtime;
    let gc_config = super::gc_runtime::GcRuntimeConfig::from_config(&access_config.iceberg.gc)?;
    for _ in 0..600 {
        match repository.recover(now_ms()?).await {
            Ok(()) => break,
            Err(CatalogError::Busy) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(error) => return Err(error.into()),
        }
    }
    let (root, authority) = repository.status().await?;
    if root.state != RootState::Ready || authority.lifecycle != CatalogLifecycle::Ready {
        return Err("Iceberg catalog is not ready for this server".into());
    }
    let timeout = Duration::from_millis(authority.admission_bounds.request_ms);
    if timeout.is_zero() || timeout > Duration::from_secs(300) {
        return Err("catalog request timeout is outside server bounds".into());
    }
    let blocks = foreground_blocks(chunks.clone(), store.clone());
    let native_budget = access_config
        .iceberg
        .native_budget_bytes
        .unwrap_or(256 * 1024 * 1024);
    let native_allocator = Arc::new(
        NativeBodyAllocator::new(native_budget, 1024 * 1024)?.with_prefix_copy_metric(
            crowdb_common::metrics::global_bandwidth("access.http.receive.prefix_copy.bw"),
        ),
    );
    let large_write = iceberg_large_write(&access_config)?;
    let mut service = IcebergHttpService::new(repository.clone(), authentication, timeout)
        .with_namespaces(store.clone())?
        .with_fileio_native(
            store.clone(),
            blocks.clone(),
            "us-east-1".into(),
            Some(native_allocator),
        )?
        .with_small_object_threshold(access_config.iceberg_small_write().threshold_exclusive())?
        .with_large_write_policy(large_write)?;
    if authority.admission_bounds.delegated_access_ms >= 900_000 {
        let endpoint =
            std::env::var("CROWDB_ICEBERG_PUBLIC_URI").unwrap_or_else(|_| format!("http://{address}"));
        service = service
            .with_tables(store.clone(), blocks.clone())?
            .with_table_credentials(store.clone(), endpoint)?;
    } else {
        tracing::warn!("table routes disabled: persisted catalog delegation bound is below fifteen minutes");
    }
    let service = Arc::new(service);
    let listener = TcpListener::bind(&address).await?;
    tracing::info!(%address, "Iceberg listener ready");
    let serving = serve(listener, service, wait_for_shutdown(shutdown));
    let multipart = Box::pin(super::file_recovery::run(
        repository.clone(),
        store.clone(),
        blocks.clone(),
    ));
    let tables = super::table_recovery::run(repository.clone(), store.clone(), blocks.clone());
    let (gc_store, gc_chunks) =
        connect_gc_pool(&access_config, management_seeds, store.clone(), gc_config.enabled).await?;
    let gc_client = gc_chunks.clone().unwrap_or_else(|| chunks.clone());
    let gc_failure_client = gc_client.clone();
    let gc = super::gc_runtime::run(repository.clone(), gc_store, gc_client, gc_config);
    let result: Result<(), BoxError> = tokio::select! {
        result = serving => result.map_err(Into::into),
        () = chunks.wait_for_small_write_manager_failure() => {
            Err("Iceberg small-write manager stopped unexpectedly".into())
        }
        () = gc_failure_client.wait_for_small_write_manager_failure(), if gc_chunks.is_some() => {
            Err("Iceberg GC small-write manager stopped unexpectedly".into())
        }
        () = super::recovery::run(repository, crowdb_access_iceberg::namespace::NamespaceRecovery::new(store)) => {
            Err("Iceberg namespace recovery stopped unexpectedly".into())
        }
        () = multipart => Err("Iceberg multipart recovery stopped unexpectedly".into()),
        () = tables => Err("Iceberg table recovery stopped unexpectedly".into()),
        () = gc => Err("Iceberg GC stopped unexpectedly".into()),
    };
    let gc_shutdown = if let Some(gc_chunks) = gc_chunks {
        gc_chunks.shutdown_small_writes().await
    } else {
        Ok(())
    };
    result?;
    gc_shutdown?;
    tracing::info!("Iceberg listener drained");
    Ok(())
}

async fn connect_gc_pool(
    access_config: &AccessConfig,
    management_seeds: Vec<String>,
    store: Arc<RoutedCatalogStore>,
    enabled: bool,
) -> Result<(Arc<RoutedCatalogStore>, Option<ChunkIoClient>), BoxError> {
    if !enabled {
        return Ok((store, None));
    }
    let (_, gc_store, gc_chunks) = connect(
        management_seeds,
        access_config.read.policy(),
        access_config.iceberg_small_write().policy(),
        access_config.common.diskio_connections_per_endpoint,
        access_config.common.diskio_rpc_workers,
    )
    .await?;
    Ok((gc_store, Some(gc_chunks)))
}

fn iceberg_large_write(
    access_config: &AccessConfig,
) -> Result<crowdb_chunk_client::LargeWritePolicy, BoxError> {
    let small = access_config.iceberg_small_write();
    Ok(IcebergLargeWriteSettings {
        ec_data: access_config.iceberg.ec_data.unwrap_or(small.ec_data),
        ec_code: access_config.iceberg.ec_code.unwrap_or(small.ec_code),
        disk_block_bytes: small.disk_block_bytes,
        mirror_copies: access_config.iceberg.large_mirror_copies,
        max_chunk_size: access_config.iceberg.max_chunk_size,
        memory_budget_bytes: access_config.iceberg.large_memory_budget_bytes,
        prefetch_strips_per_chunk: access_config.iceberg.large_prefetch_strips_per_chunk,
        prefetch_max_strips_per_batch: access_config.iceberg.large_prefetch_max_strips_per_batch,
        parallel_strip_writes: access_config.iceberg.large_parallel_strip_writes,
        held_buffers: access_config.iceberg.large_held_buffers,
        chunk_preparation_depth: access_config.iceberg.large_chunk_preparation_depth,
    }
    .policy()?)
}

async fn wait_for_shutdown(mut shutdown: Option<watch::Receiver<bool>>) {
    if let Some(receiver) = shutdown.as_mut() {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            () = async {
                loop {
                    if *receiver.borrow() || receiver.changed().await.is_err() {
                        break;
                    }
                }
            } => {}
        }
    } else {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn manage(
    repository: &CatalogRepository,
    authentication: &BearerAuthenticator,
    arguments: &[String],
) -> Result<(), BoxError> {
    let token = std::env::var("CROWDB_ICEBERG_TOKEN")?;
    let principal = authentication
        .authenticate(&format!("Bearer {token}"))
        .ok_or("invalid management bearer token")?;
    if principal.management == ManagementPrivilege::None {
        return Err("management privilege is required".into());
    }
    if arguments == ["status"] {
        let (root, authority) = repository.status().await?;
        println!(
            "{}",
            serde_json::json!({"catalog_id": authority.catalog.to_string(), "display_name": authority.display_name,
            "activation_epoch": root.context.activation_epoch, "state": format!("{:?}", root.state),
            "capability_bits": format!("0x{:04x}", authority.capabilities.bits()),
            "config_generation": authority.config_generation})
        );
        return Ok(());
    }
    if arguments == ["inspect"] {
        match repository.status().await {
            Ok((root, authority)) => println!(
                "{}",
                serde_json::json!({"initialized": true, "catalog_id": authority.catalog.to_string(),
                "display_name": authority.display_name, "activation_epoch": root.context.activation_epoch,
                "state": format!("{:?}", root.state), "capability_bits": format!("0x{:04x}", authority.capabilities.bits()),
                "root_operation_id": root.operation.to_string()})
            ),
            Err(CatalogError::Uninitialized) => println!("{{\"initialized\":false}}"),
            Err(error) => return Err(error.into()),
        }
        return Ok(());
    }
    let action = match arguments.first().map(String::as_str) {
        Some("initialize") if arguments.len() == 3 => ManagementAction::Initialize,
        Some("rename") if arguments.len() == 4 => ManagementAction::Rename,
        Some("clear") if arguments.len() == 5 => ManagementAction::Clear,
        Some("activate") if arguments.len() == 5 => ManagementAction::Activate,
        _ => return Err("usage: crowdb-access-server iceberg initialize UUIDv7 NAME | rename UUIDv7 NAME EPOCH | clear UUIDv7 NAME EPOCH CONFIRM_CATALOG_ID | activate UUIDv7 NAME EPOCH CAPABILITY_BITS_HEX | status | inspect | serve".into()),
    };
    let request = ManagementRequest {
        identity: RequestIdentity::parse(&arguments[1], now_ms()?)?,
        principal: principal.name.into(),
        action,
        display_name: arguments[2].clone(),
        expected_epoch: arguments
            .get(3)
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(0),
        confirmation: if action == ManagementAction::Clear {
            arguments.get(4).map(|value| value.parse()).transpose()?
        } else {
            None
        },
        capabilities: if action == ManagementAction::Activate {
            Some(Capabilities::from_bits(u16::from_str_radix(
                arguments[4].trim_start_matches("0x"),
                16,
            )?)?)
        } else {
            None
        },
    };
    for _ in 0..600 {
        match repository
            .execute(request.clone(), principal.management, now_ms()?)
            .await
        {
            Ok(authority) => {
                println!(
                    "{}",
                    serde_json::json!({"catalog_id": authority.catalog.to_string(), "display_name": authority.display_name,
                    "name_generation": authority.name_generation, "config_generation": authority.config_generation,
                    "capability_bits": format!("0x{:04x}", authority.capabilities.bits()),
                    "operation_id": request.identity.operation.to_string()})
                );
                return Ok(());
            }
            Err(CatalogError::Busy) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(error) => return Err(error.into()),
        }
    }
    Err("management operation is still pending; retry with the same identity and input".into())
}

pub(super) fn now_ms() -> Result<u64, BoxError> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}
