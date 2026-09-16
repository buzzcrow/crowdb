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
use crowdb_access_server::credentials::CredentialAuthority;
#[cfg(feature = "s3")]
use crowdb_access_server::s3::{serve, ProductionS3Operations, S3Dispatcher, S3ServiceConfig};
#[cfg(feature = "s3")]
use crowdb_access_server::storage::S3StorageClients;
#[cfg(feature = "s3")]
use crowdb_chunk_client::SmallWritePolicy;
#[cfg(feature = "s3")]
use crowdb_common::ec::EcScheme;
#[cfg(feature = "s3")]
use crowdb_kv_client::{ClientConfig as KvConfig, CrowdbKvClient};
#[cfg(feature = "s3")]
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    #[cfg(feature = "s3")]
    if std::env::args().nth(1).as_deref() == Some("issue-user") {
        return issue_user().await;
    }
    #[cfg(feature = "s3")]
    if let Ok(address) = std::env::var("CROWDB_S3_LISTEN") {
        let management_seeds = management_seeds()?;
        let tenant = TenantId::new(required_env("CROWDB_S3_TENANT")?.into_bytes())?;
        let master_key = MasterKey::from_hex(&required_env("CROWDB_S3_MASTER_KEY")?)?;
        let credential_cipher = Arc::new(CredentialCipher::new(&master_key));
        let continuation_key = credential_cipher.continuation_key().to_vec();
        let small_write = SmallWritePolicy::default();
        let storage = S3StorageClients::connect(management_seeds, 2, 2, small_write.clone()).await?;
        let chunks = Arc::clone(&storage.chunks);
        let authority = Arc::new(CredentialAuthority::new(
            Arc::clone(&storage.control),
            Arc::clone(&credential_cipher),
        ));
        let trusted_network = std::env::var("CROWDB_S3_TRUSTED_NETWORK").as_deref() == Ok("true");
        let (authenticator, credential_refresh): (
            Arc<dyn RequestAuthenticator>,
            Option<tokio::task::JoinHandle<()>>,
        ) = if trusted_network {
            tracing::warn!(%address, "starting S3 with explicit trusted-network authentication bypass");
            (Arc::new(TrustedNetworkAuthenticator::new()), None)
        } else {
            let cache = Arc::new(CredentialCache::new(90));
            refresh_credentials(&authority, &cache).await?;
            let refresh_authority = Arc::clone(&authority);
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
            let region = std::env::var("CROWDB_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
            (Arc::new(SigV4Verifier::new(cache, region, 900)), Some(task))
        };
        let mut service_config = S3ServiceConfig::basic(tenant, continuation_key, small_write.object_limit);
        service_config.small_object_limit =
            optional_usize("CROWDB_S3_SMALL_OBJECT_LIMIT")?.unwrap_or(service_config.small_object_limit);
        configure_large_write(&mut service_config)?;
        let metrics = Arc::new(S3Metrics::default());
        let cleanup_backlog_limit = optional_usize("CROWDB_S3_CLEANUP_BACKLOG_LIMIT")?
            .map_or(10_000, |value| u64::try_from(value).unwrap_or(u64::MAX));
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
        let body_allocator = Arc::new(NativeBodyAllocator::new(256 * 1024 * 1024, 1024 * 1024)?);
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
fn configure_large_write(config: &mut S3ServiceConfig) -> Result<(), Box<dyn std::error::Error>> {
    let ec_data = optional_usize("CROWDB_S3_EC_DATA")?.unwrap_or(config.large_write.ec_scheme.data_num);
    let ec_code = optional_usize("CROWDB_S3_EC_CODE")?.unwrap_or(config.large_write.ec_scheme.code_num);
    if ec_data == 0 || ec_code == 0 {
        return Err("CROWDB S3 EC data and code counts must be nonzero".into());
    }
    config.large_write.ec_scheme = EcScheme::new(ec_data, ec_code);
    if let Some(max_chunk_size) = optional_usize("CROWDB_S3_MAX_CHUNK_SIZE")? {
        if max_chunk_size == 0 {
            return Err("CROWDB S3 max chunk size must be nonzero".into());
        }
        Arc::make_mut(&mut config.large_write.client).max_chunk_size =
            u64::try_from(max_chunk_size).unwrap_or(u64::MAX);
    }
    Ok(())
}

#[cfg(feature = "s3")]
async fn issue_user() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::args()
        .nth(2)
        .filter(|value| !value.is_empty())
        .ok_or("usage: crowdb-access-server issue-user USER")?;
    if std::env::args().nth(3).is_some() {
        return Err("usage: crowdb-access-server issue-user USER".into());
    }
    let master_key = MasterKey::from_hex(&required_env("CROWDB_S3_MASTER_KEY")?)?;
    let cipher = Arc::new(CredentialCipher::new(&master_key));
    let control = Arc::new(CrowdbKvClient::new(KvConfig::new(management_seeds()?)));
    let authority = CredentialAuthority::new(control, cipher);
    let token = authority.issue_user(user.as_bytes()).await?;
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
fn required_env(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|_| format!("{name} is required when S3 is enabled").into())
}
