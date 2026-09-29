// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[cfg(feature = "s3")]
use std::sync::Arc;
#[cfg(feature = "s3")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "s3")]
use crowdb_access_s3::auth::{
    CredentialCache, CredentialCipher, MasterKey, RequestAuthenticator, SigV4Verifier,
    TrustedNetworkAuthenticator,
};
#[cfg(feature = "s3")]
use crowdb_access_s3::metadata::TenantId;
#[cfg(feature = "s3")]
use crowdb_access_s3::metrics::{DependencyHealth, S3Health, S3Metrics};
#[cfg(feature = "s3")]
use crowdb_access_s3::native_buffer::NativeBodyAllocator;
#[cfg(feature = "s3")]
use crowdb_access_server::config::{load_args, AccessConfig};
#[cfg(feature = "s3")]
use crowdb_access_server::credentials::CredentialAuthority;
#[cfg(feature = "s3")]
use crowdb_access_server::s3::{serve, ProductionS3Operations, S3Dispatcher, S3ServiceConfig};
#[cfg(feature = "s3")]
use crowdb_access_server::storage::S3StorageClients;
#[cfg(feature = "s3")]
use crowdb_chunk_client::SmallWritePolicy;
#[cfg(feature = "s3")]
#[cfg(feature = "s3")]
use crowdb_common::ec::EcScheme;
#[cfg(feature = "s3")]
use crowdb_kv_client::{ClientConfig as KvConfig, CrowdbKvClient};
#[cfg(feature = "s3")]
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|arg| arg == "iceberg") {
        args.remove(0);
        init_access_logging()?;
        return crowdb_access_server::iceberg::run(args)
            .await
            .map_err(|error| -> Box<dyn std::error::Error> { error });
    }
    let s3_only = args.first().is_some_and(|arg| arg == "s3");
    if s3_only {
        args.remove(0);
    }
    init_access_logging()?;
    #[cfg(feature = "s3")]
    let (access_config, remaining_args) = load_args(args.clone())?;
    #[cfg(not(feature = "s3"))]
    let _ = args;
    #[cfg(feature = "s3")]
    if matches!(
        remaining_args.first().map(String::as_str),
        Some("issue-user" | "ensure-user" | "lookup-user")
    ) {
        return issue_user(&remaining_args, &access_config).await;
    }
    #[cfg(feature = "s3")]
    if !remaining_args.is_empty() {
        return Err("unexpected S3 server arguments".into());
    }
    #[cfg(feature = "s3")]
    if !s3_only && access_config.s3.listen.is_none() && std::env::var_os("CROWDB_S3_LISTEN").is_none() {
        return Err("S3 listen address is required when starting both access listeners".into());
    }
    #[cfg(feature = "s3")]
    if s3_only {
        run_s3(&access_config).await?;
    } else {
        tokio::try_join!(run_s3(&access_config), async {
            crowdb_access_server::iceberg::run(args)
                .await
                .map_err(|error| -> Box<dyn std::error::Error> { error })
        })?;
    }
    #[cfg(not(feature = "s3"))]
    crowdb_access_server::iceberg::run(args)
        .await
        .map_err(|error| -> Box<dyn std::error::Error> { error })?;
    Ok(())
}

fn init_access_logging() -> Result<(), std::io::Error> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();
    let log_dir = std::env::var("CROWDB_ACCESS_LOG_DIR").unwrap_or_default();
    if !log_dir.is_empty() {
        std::fs::create_dir_all(&log_dir)?;
    }
    crowdb_rpc_ffi::init_logging(
        &log_dir,
        if log_dir.is_empty() { "warn" } else { "info" },
        30,
        5,
        "crowdb-access-rpc",
    );
    if !log_dir.is_empty() {
        crowdb_rpc_ffi::add_log_stderr("warn");
    }
    Ok(())
}

#[cfg(feature = "s3")]
async fn run_s3(access_config: &AccessConfig) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(address) = access_config
        .s3
        .listen
        .clone()
        .or_else(|| std::env::var("CROWDB_S3_LISTEN").ok())
    {
        let management_seeds = if access_config.common.management_seeds.is_empty() {
            management_seeds()?
        } else {
            access_config.common.management_seeds.clone()
        };
        let tenant_name = match &access_config.s3.tenant {
            Some(name) => name.clone(),
            None => required_env("CROWDB_S3_TENANT")?,
        };
        let tenant = TenantId::new(tenant_name.into_bytes())?;
        let master_key = MasterKey::from_hex(&required_env("CROWDB_S3_MASTER_KEY")?)?;
        let credential_cipher = Arc::new(CredentialCipher::new(&master_key));
        let continuation_key = credential_cipher.continuation_key().to_vec();
        let (ec_scheme, small_write, small_threshold) = s3_write_routing(access_config)?;
        let storage = S3StorageClients::connect_with_read_policy(
            management_seeds,
            access_config.common.diskio_connections_per_endpoint,
            access_config.common.diskio_rpc_workers,
            small_write,
            access_config.read.policy(),
        )
        .await?;
        let chunks = Arc::clone(&storage.chunks);
        let authority = Arc::new(CredentialAuthority::new(
            Arc::clone(&storage.control),
            Arc::clone(&credential_cipher),
        ));
        let (authenticator, credential_refresh, trusted_network) =
            authenticate_s3(access_config, &address, &authority).await?;
        let service_config = s3_service_config(
            access_config,
            tenant,
            continuation_key,
            small_threshold,
            ec_scheme,
        )?;
        let metrics = Arc::new(S3Metrics::default());
        let cleanup_backlog_limit = configured_u64(
            access_config.s3.cleanup_backlog_limit,
            "CROWDB_S3_CLEANUP_BACKLOG_LIMIT",
        )?
        .unwrap_or(10_000);
        let health = Arc::new(S3Health::starting(cleanup_backlog_limit));
        health.set_metadata(DependencyHealth::Ready);
        health.set_chunks(DependencyHealth::Ready);
        health.set_authentication(DependencyHealth::Ready);
        let operations = Arc::new(
            ProductionS3Operations::new(storage, service_config)
                .map_err(|_| "invalid S3 service configuration")?
                .with_metrics(Arc::clone(&metrics))
                .with_health(Arc::clone(&health)),
        );
        let expiry_task = start_multipart_expiry(Arc::clone(&operations));
        let native_budget = configured_usize(
            access_config.s3.native_budget_bytes,
            "CROWDB_S3_NATIVE_BUDGET_BYTES",
        )?
        .unwrap_or(256 * 1024 * 1024);
        let body_allocator = Arc::new(NativeBodyAllocator::new(native_budget, 1024 * 1024)?);
        let handler = Arc::new(
            S3Dispatcher::new(
                authenticator,
                operations,
                metrics,
                "crowdb-access-server".into(),
                trusted_network,
            )
            .with_native_body_allocator(body_allocator)
            .with_chunk_metrics(Arc::clone(&chunks))
            .with_health(Arc::clone(&health)),
        );
        let listener = TcpListener::bind(address).await?;
        health.set_listener(DependencyHealth::Ready);
        let serve_result = serve(listener, handler, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
        health.stop();
        expiry_task.abort();
        let shutdown_result = chunks.shutdown_small_writes().await;
        if let Some(task) = credential_refresh {
            task.abort();
        }
        serve_result?;
        shutdown_result?;
    }
    Ok(())
}

#[cfg(feature = "s3")]
fn start_multipart_expiry(operations: Arc<ProductionS3Operations>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60 * 60));
        loop {
            interval.tick().await;
            match operations.expire_multipart_uploads().await {
                Ok(0) => {}
                Ok(count) => tracing::debug!(count, "expired S3 multipart sessions"),
                Err(error) => tracing::warn!(?error, "S3 multipart expiry sweep deferred"),
            }
        }
    })
}

#[cfg(feature = "s3")]
async fn authenticate_s3(
    access: &AccessConfig,
    address: &str,
    authority: &Arc<CredentialAuthority>,
) -> Result<
    (
        Arc<dyn RequestAuthenticator>,
        Option<tokio::task::JoinHandle<()>>,
        bool,
    ),
    Box<dyn std::error::Error>,
> {
    let trusted_network = access
        .s3
        .trusted_network
        .unwrap_or(std::env::var("CROWDB_S3_TRUSTED_NETWORK").as_deref() == Ok("true"));
    if trusted_network {
        tracing::warn!(%address, "starting S3 with explicit trusted-network authentication bypass");
        return Ok((Arc::new(TrustedNetworkAuthenticator::new()), None, true));
    }
    let cache = Arc::new(CredentialCache::new(90));
    refresh_credentials(authority, &cache).await?;
    let refresh_authority = Arc::clone(authority);
    let refresh_cache = Arc::clone(&cache);
    let task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(error) = refresh_credentials(&refresh_authority, &refresh_cache).await {
                tracing::error!(%error, "S3 credential refresh failed; cache will fail closed when stale");
            }
        }
    });
    let region = access
        .s3
        .region
        .clone()
        .or_else(|| std::env::var("CROWDB_S3_REGION").ok())
        .unwrap_or_else(|| "us-east-1".into());
    Ok((
        Arc::new(SigV4Verifier::new(cache, region, 900)),
        Some(task),
        false,
    ))
}

#[cfg(feature = "s3")]
fn s3_service_config(
    access: &AccessConfig,
    tenant: TenantId,
    continuation_key: Vec<u8>,
    small_write_limit: usize,
    ec_scheme: EcScheme,
) -> Result<S3ServiceConfig, Box<dyn std::error::Error>> {
    let mut config = S3ServiceConfig::basic(tenant, continuation_key, small_write_limit);
    config.large_write.ec_scheme = ec_scheme;
    if let Some(limit) = access.s3.list_scan_items {
        config.list_scan_items = limit;
    }
    if let Some(limit) = access.s3.list_scan_bytes {
        config.list_scan_bytes = limit;
    }
    if let Some(ttl) = access.s3.continuation_ttl_seconds {
        config.continuation_ttl_seconds = ttl;
    }
    config.small_object_limit =
        configured_usize(access.s3.small_object_limit, "CROWDB_S3_SMALL_OBJECT_LIMIT")?
            .unwrap_or(config.small_object_limit)
            .min(small_write_limit.saturating_sub(1));
    configure_large_write(&mut config, access)?;
    Ok(config)
}

#[cfg(feature = "s3")]
fn configure_large_write(
    config: &mut S3ServiceConfig,
    access: &AccessConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    Arc::make_mut(&mut config.large_write.client).read_buffer_size = access.small_write.disk_block_bytes;
    if let Some(max_chunk_size) = configured_u64(access.s3.max_chunk_size, "CROWDB_S3_MAX_CHUNK_SIZE")? {
        if max_chunk_size == 0 {
            return Err("CROWDB S3 max chunk size must be nonzero".into());
        }
        Arc::make_mut(&mut config.large_write.client).max_chunk_size = max_chunk_size;
    }
    Ok(())
}

#[cfg(feature = "s3")]
fn s3_ec_scheme(access: &AccessConfig) -> Result<EcScheme, Box<dyn std::error::Error>> {
    let ec_data =
        configured_usize(access.s3.ec_data, "CROWDB_S3_EC_DATA")?.unwrap_or(access.small_write.ec_data);
    let ec_code =
        configured_usize(access.s3.ec_code, "CROWDB_S3_EC_CODE")?.unwrap_or(access.small_write.ec_code);
    if ec_data == 0 || ec_data > 32 || ec_code == 0 {
        return Err("CROWDB S3 EC data and code counts are invalid".into());
    }
    Ok(EcScheme::new(ec_data, ec_code))
}

#[cfg(feature = "s3")]
fn s3_write_routing(
    access: &AccessConfig,
) -> Result<(EcScheme, SmallWritePolicy, usize), Box<dyn std::error::Error>> {
    let ec_scheme = s3_ec_scheme(access)?;
    let mut config = access.small_write.clone();
    config.ec_data = ec_scheme.data_num;
    config.ec_code = ec_scheme.code_num;
    let policy = config.policy();
    policy.validate()?;
    let threshold = config.threshold_exclusive();
    if threshold > policy.object_limit {
        return Err("S3 small-object threshold exceeds the shared writer limit".into());
    }
    Ok((ec_scheme, policy, threshold))
}

#[cfg(feature = "s3")]
async fn issue_user(args: &[String], access: &AccessConfig) -> Result<(), Box<dyn std::error::Error>> {
    let command = args.first().ok_or("missing S3 user command")?;
    let user = args
        .get(1)
        .filter(|value| !value.is_empty())
        .ok_or("usage: crowdb-access-server issue-user|ensure-user|lookup-user USER")?;
    if args.len() != 2 {
        return Err("usage: crowdb-access-server issue-user|ensure-user|lookup-user USER".into());
    }
    let master_key = MasterKey::from_hex(&required_env("CROWDB_S3_MASTER_KEY")?)?;
    let cipher = Arc::new(CredentialCipher::new(&master_key));
    let seeds = if access.common.management_seeds.is_empty() {
        management_seeds()?
    } else {
        access.common.management_seeds.clone()
    };
    let control = Arc::new(CrowdbKvClient::new(KvConfig::new(seeds)));
    let authority = CredentialAuthority::new(control, cipher);
    let token = match command.as_str() {
        "ensure-user" => authority.ensure_user(user.as_bytes()).await?,
        "lookup-user" => authority
            .lookup_user(user.as_bytes())
            .await?
            .ok_or("S3 user does not exist")?,
        _ => authority.issue_user(user.as_bytes()).await?,
    };
    println!("AWS_ACCESS_KEY_ID={}", token.access_key_id);
    println!("AWS_SECRET_ACCESS_KEY={}", token.secret_key);
    Ok(())
}

#[cfg(feature = "s3")]
async fn refresh_credentials(
    authority: &CredentialAuthority,
    cache: &CredentialCache,
) -> Result<(), Box<dyn std::error::Error>> {
    let credentials = authority.load_credentials().await?;
    let refreshed_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    cache.install(credentials, refreshed_at)?;
    Ok(())
}

#[cfg(feature = "s3")]
fn management_seeds() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let seeds = required_env("CROWDB_MANAGEMENT_SEEDS")?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if seeds.is_empty() {
        return Err("CROWDB_MANAGEMENT_SEEDS must contain at least one endpoint".into());
    }
    Ok(seeds)
}

#[cfg(feature = "s3")]
fn optional_usize(name: &str) -> Result<Option<usize>, Box<dyn std::error::Error>> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|error| format!("{name} is invalid: {error}").into())
        })
        .transpose()
}

#[cfg(feature = "s3")]
fn configured_usize(
    configured: Option<usize>,
    env_name: &str,
) -> Result<Option<usize>, Box<dyn std::error::Error>> {
    match configured {
        Some(value) => Ok(Some(value)),
        None => optional_usize(env_name),
    }
}

#[cfg(feature = "s3")]
fn configured_u64(
    configured: Option<u64>,
    env_name: &str,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    match configured {
        Some(value) => Ok(Some(value)),
        None => {
            optional_usize(env_name).map(|value| value.map(|value| u64::try_from(value).unwrap_or(u64::MAX)))
        }
    }
}

#[cfg(feature = "s3")]
fn required_env(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|_| format!("{name} is required when S3 is enabled").into())
}
