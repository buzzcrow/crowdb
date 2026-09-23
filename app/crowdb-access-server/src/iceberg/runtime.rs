use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crowdb_access_iceberg::catalog::{
    CatalogError, CatalogLifecycle, CatalogRepository, ClearBounds, ManagementPrivilege, RootState,
    RoutedCatalogStore,
};
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};
use crowdb_access_iceberg::wire::BearerAuthenticator;
use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, SmallWritePolicy};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_kv_client::{ClientConfig as KvConfig, CrowdbKvClient};
use tokio::net::TcpListener;

use super::{serve, IcebergHttpService};

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
        let seeds = std::env::var("CROWDB_MANAGEMENT_SEEDS")?;
        if seeds.len() > 8192 {
            return Err("management seed configuration is oversized".into());
        }
        let management_seeds: Vec<_> = seeds
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect();
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
        let listen = std::env::var("CROWDB_ICEBERG_LISTEN").unwrap_or_else(|_| "127.0.0.1:8181".into());
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
pub async fn run() -> Result<(), BoxError> {
    let config = IcebergRuntimeConfig::from_env()?;
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if arguments.len() > 5 {
        return Err("too many Iceberg command arguments".into());
    }
    let (repository, store, chunks) = connect(config.management_seeds).await?;
    let result = if arguments.is_empty() || arguments == ["serve"] {
        start_listener(
            &config.listen,
            repository,
            store,
            config.authentication,
            chunks.clone(),
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

async fn connect(
    seeds: Vec<String>,
) -> Result<(Arc<CatalogRepository>, Arc<RoutedCatalogStore>, ChunkIoClient), BoxError> {
    let control = Arc::new(CrowdbKvClient::new(KvConfig::new(seeds.clone())));
    let client_config = ClientConfig::default();
    let source = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(Arc::clone(&control)));
    let transport = Arc::new(ChunkKvRpcTransport::new(
        client_config.max_owner_connections,
        1,
        2,
    ));
    let client = Arc::new(ChunkKvClient::new(client_config, source, transport)?);
    client.refresh_catalog().await?;
    let chunks = ChunkIoClient::connect_with_kv(
        ChunkIoClientConfig {
            management_seeds: seeds,
            diskio_connections_per_endpoint: 2,
            diskio_rpc_workers: 2,
            small_write: SmallWritePolicy::default(),
        },
        control,
    )
    .await?;
    let store = Arc::new(RoutedCatalogStore::new(client));
    let repository = Arc::new(CatalogRepository::new(store.clone(), ClearBounds::default())?);
    Ok((repository, store, chunks))
}

async fn start_listener(
    address: &str,
    repository: Arc<CatalogRepository>,
    store: Arc<RoutedCatalogStore>,
    authentication: BearerAuthenticator,
    chunks: ChunkIoClient,
) -> Result<(), BoxError> {
    for _ in 0..600 {
        match repository.recover(now_ms()?).await {
            Ok(()) => break,
            Err(CatalogError::Busy) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(error) => return Err(error.into()),
        }
    }
    let (root, authority) = repository.status().await?;
    if root.state != RootState::Ready
        || authority.lifecycle != CatalogLifecycle::Ready
        || authority.capabilities.bits() != 0
    {
        return Err("Iceberg catalog is not ready for this server".into());
    }
    let timeout = Duration::from_millis(authority.admission_bounds.request_ms);
    if timeout.is_zero() || timeout > Duration::from_secs(60) {
        return Err("catalog request timeout is outside server bounds".into());
    }
    let blocks: Arc<dyn crowdb_access_iceberg::file::FileBlockStore> =
        Arc::new(crowdb_access_iceberg::file::NativeFileBlocks::new(chunks.clone()));
    let service = Arc::new(
        IcebergHttpService::new(repository.clone(), authentication, timeout)
            .with_namespaces(store.clone())?
            .with_fileio(store.clone(), blocks.clone(), "us-east-1".into())?,
    );
    let listener = TcpListener::bind(address).await?;
    tracing::info!(%address, "Iceberg listener ready");
    let serving = serve(listener, service, async {
        let _ = tokio::signal::ctrl_c().await;
    });
    let multipart = Box::pin(super::file_recovery::run(
        repository.clone(),
        store.clone(),
        blocks,
    ));
    tokio::select! {
        result = serving => result?,
        () = super::recovery::run(repository, crowdb_access_iceberg::namespace::NamespaceRecovery::new(store)) => {}
        () = multipart => {}
    }
    tracing::info!("Iceberg listener drained");
    Ok(())
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
            "activation_epoch": root.context.activation_epoch, "state": format!("{:?}", root.state)})
        );
        return Ok(());
    }
    let action = match arguments.first().map(String::as_str) {
        Some("initialize") if arguments.len() == 3 => ManagementAction::Initialize,
        Some("rename") if arguments.len() == 4 => ManagementAction::Rename,
        Some("clear") if arguments.len() == 5 => ManagementAction::Clear,
        _ => return Err("usage: crowdb-iceberg initialize UUIDv7 NAME | rename UUIDv7 NAME EPOCH | clear UUIDv7 NAME EPOCH CONFIRM_CATALOG_ID | status | serve".into()),
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
        confirmation: arguments.get(4).map(|value| value.parse()).transpose()?,
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
                    "name_generation": authority.name_generation, "operation_id": request.identity.operation.to_string()})
                );
                return Ok(());
            }
            Err(CatalogError::Busy) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(error) => return Err(error.into()),
        }
    }
    Err("management operation is still pending; retry with the same identity and input".into())
}

fn now_ms() -> Result<u64, BoxError> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}
