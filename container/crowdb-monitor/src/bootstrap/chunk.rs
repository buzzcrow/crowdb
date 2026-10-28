use std::fs;
use std::time::Duration;

use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient, ServiceRegistryClient};
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
    Registry(#[from] crowdb_paxos_kv_client::Error),
    #[error("chunk service configuration is invalid: {0}")]
    Invalid(&'static str),
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
    #[allow(dead_code)]
    partition_id: Id128,
    owner_epoch: u64,
}

struct ExpectedRegistration {
    service: &'static str,
    instance_id: u64,
    endpoint: String,
    chunk_kv_metadata: bool,
}

fn expected_registrations(
    profile: &DeploymentProfile,
) -> Result<Vec<ExpectedRegistration>, ChunkBootstrapError> {
    let config_root = profile.paths.run_root.join("config");
    let mut expected = Vec::new();
    if profile.services.iter().any(|service| service.id == "chunkdb") {
        let config: ChunkdbConfig = toml::from_str(&fs::read_to_string(config_root.join("chunkdb.toml"))?)
            .map_err(|_| ChunkBootstrapError::Invalid("ChunkDB configuration cannot be parsed"))?;
        let instance_id = config
            .server
            .instance_id
            .parse::<u64>()
            .map_err(|_| ChunkBootstrapError::Invalid("ChunkDB instance ID is invalid"))?;
        if instance_id == 0 {
            return Err(ChunkBootstrapError::Invalid("ChunkDB instance ID is zero"));
        }
        expected.push(ExpectedRegistration {
            service: "chunkdb",
            instance_id,
            endpoint: format!("http://{}", config.server.rpc_listen_addr),
            chunk_kv_metadata: false,
        });
    }
    if profile.services.iter().any(|service| service.id == "chunk-kv") {
        let config: ChunkKvConfig =
            toml::from_str(&fs::read_to_string(config_root.join("chunk-kv.toml"))?)
                .map_err(|_| ChunkBootstrapError::Invalid("Chunk-KV configuration cannot be parsed"))?;
        if config.instance_id == 0 || config.bootstrap_partition.owner_epoch == 0 {
            return Err(ChunkBootstrapError::Invalid(
                "Chunk-KV identity or owner epoch is zero",
            ));
        }
        expected.push(ExpectedRegistration {
            service: "chunk-kv",
            instance_id: config.instance_id,
            endpoint: config.rpc_advertise_addr,
            chunk_kv_metadata: true,
        });
    }
    Ok(expected)
}

/// # Errors
/// Requires selected live chunk registrations to match their rendered configuration.
pub async fn verify_chunk_services(
    management_seed: &str,
    profile: &DeploymentProfile,
) -> Result<(), ChunkBootstrapError> {
    let expected = expected_registrations(profile)?;
    if expected.is_empty() {
        return Ok(());
    }
    let registry = ServiceRegistryClient::new(PaxosKvClient::new(ClientConfig::new(vec![
        management_seed.to_owned()
    ])));
    registry.kv().refresh_topology().await?;
    let deadline = Instant::now() + READY_DEADLINE;
    loop {
        let mut ready = true;
        for wanted in &expected {
            let instances = registry.read_all_instances(wanted.service).await?;
            if instances.len() > 1 {
                return Err(ChunkBootstrapError::Invalid(
                    "unexpected live chunk service instance",
                ));
            }
            let Some((id, instance)) = instances.first() else {
                ready = false;
                continue;
            };
            if *id != wanted.instance_id
                || instance.instance_id != wanted.instance_id
                || instance.rpc_endpoint != wanted.endpoint
                || (wanted.chunk_kv_metadata
                    && instance
                        .extra
                        .as_ref()
                        .and_then(|extra| extra.chunk_kv.as_ref())
                        .is_none())
            {
                return Err(ChunkBootstrapError::Invalid(match wanted.service {
                    "chunkdb" => "ChunkDB registration conflicts with configuration",
                    "chunk-kv" => "Chunk-KV registration conflicts with configuration",
                    _ => "chunk registration conflicts with configuration",
                }));
            }
        }
        // Persisted assignments can remain in recovery after their tree chunk
        // disappears. Keep the existing supervision policy while repair retries.
        if ready || Instant::now() >= deadline {
            return Ok(());
        }
        sleep(PROBE_INTERVAL).await;
    }
}
