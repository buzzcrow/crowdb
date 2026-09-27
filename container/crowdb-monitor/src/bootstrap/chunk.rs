use std::fs;
use std::time::Duration;

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, ServiceRegistryClient};
use crowdb_protocol::chunk_kv::Id128;
use serde::Deserialize;
use thiserror::Error;
use tokio::time::{sleep, Instant};

use crate::DeploymentProfile;

const READY_DEADLINE: Duration = Duration::from_secs(30);
const PROBE_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Debug, Error)]
pub enum ChunkBootstrapError {
    #[error("chunk service configuration failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("chunk service registry lookup failed: {0}")]
    Registry(#[from] crowdb_kv_client::Error),
    #[error("chunk service configuration is invalid: {0}")]
    Invalid(&'static str),
    #[error("chunk service registration did not become ready")]
    Deadline,
}

#[derive(Deserialize)]
struct ChunkdbConfig {
    server: ChunkdbServer,
}

#[derive(Deserialize)]
struct ChunkdbServer {
    instance_id: String,
    rpc_listen_addr: String,
}

#[derive(Deserialize)]
struct ChunkKvConfig {
    instance_id: u64,
    rpc_advertise_addr: String,
    bootstrap_partition: BootstrapPartition,
}

#[derive(Deserialize)]
struct BootstrapPartition {
    partition_id: Id128,
    owner_epoch: u64,
}

/// # Errors
/// Requires the live `ChunkDB` and `Chunk-KV` registrations to match rendered configuration.
pub async fn verify_chunk_services(
    management_seed: &str,
    profile: &DeploymentProfile,
) -> Result<(), ChunkBootstrapError> {
    let config_root = profile.paths.run_root.join("config");
    let chunkdb: ChunkdbConfig = toml::from_str(&fs::read_to_string(config_root.join("chunkdb.toml"))?)
        .map_err(|_| ChunkBootstrapError::Invalid("ChunkDB configuration cannot be parsed"))?;
    let chunk_kv: ChunkKvConfig = toml::from_str(&fs::read_to_string(config_root.join("chunk-kv.toml"))?)
        .map_err(|_| ChunkBootstrapError::Invalid("Chunk-KV configuration cannot be parsed"))?;
    let chunkdb_id = chunkdb
        .server
        .instance_id
        .parse::<u64>()
        .map_err(|_| ChunkBootstrapError::Invalid("ChunkDB instance ID is invalid"))?;
    if chunkdb_id == 0 || chunk_kv.instance_id == 0 || chunk_kv.bootstrap_partition.owner_epoch == 0 {
        return Err(ChunkBootstrapError::Invalid(
            "chunk identity or owner epoch is zero",
        ));
    }
    let registry = ServiceRegistryClient::new(CrowdbKvClient::new(ClientConfig::new(vec![
        management_seed.to_owned()
    ])));
    registry.kv().refresh_topology().await?;
    let deadline = Instant::now() + READY_DEADLINE;
    loop {
        let chunkdb_instances = registry.read_all_instances("chunkdb").await?;
        let chunk_kv_instances = registry.read_all_instances("chunk-kv").await?;
        if chunkdb_instances.len() > 1 || chunk_kv_instances.len() > 1 {
            return Err(ChunkBootstrapError::Invalid(
                "unexpected live chunk service instance",
            ));
        }
        if let Some((id, instance)) = chunkdb_instances.first() {
            if *id != chunkdb_id
                || instance.instance_id != chunkdb_id
                || instance.rpc_endpoint != format!("http://{}", chunkdb.server.rpc_listen_addr)
            {
                return Err(ChunkBootstrapError::Invalid(
                    "ChunkDB registration conflicts with configuration",
                ));
            }
        }
        if let Some((id, instance)) = chunk_kv_instances.first() {
            if *id != chunk_kv.instance_id
                || instance.instance_id != chunk_kv.instance_id
                || instance.rpc_endpoint != chunk_kv.rpc_advertise_addr
                || instance
                    .extra
                    .as_ref()
                    .and_then(|extra| extra.chunk_kv.as_ref())
                    .is_none()
            {
                return Err(ChunkBootstrapError::Invalid(
                    "Chunk-KV registration conflicts with configuration",
                ));
            }
        }
        let partition_ready = chunk_kv_instances
            .first()
            .and_then(|(_, instance)| instance.extra.as_ref())
            .and_then(|extra| extra.chunk_kv.as_ref())
            .is_some_and(|extra| {
                extra.hosted.iter().any(|hosted| {
                    hosted.partition_id == chunk_kv.bootstrap_partition.partition_id
                        && hosted.owner_epoch == chunk_kv.bootstrap_partition.owner_epoch
                        && !hosted.recovering
                })
            });
        if !chunkdb_instances.is_empty() && partition_ready {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ChunkBootstrapError::Deadline);
        }
        sleep(PROBE_INTERVAL).await;
    }
}
