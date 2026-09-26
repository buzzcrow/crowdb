use std::time::Duration;

use crowdb_diskio_client::{DiskId, DiskioClient, DiskioClientConfig, DiskioError, OperationOptions};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, ServiceRegistryClient};
use thiserror::Error;
use tokio::time::{sleep, Instant};

use crate::DeploymentProfile;

const READY_DEADLINE: Duration = Duration::from_secs(30);
const PROBE_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Debug, Error)]
pub enum StorageProbeError {
    #[error("DiskIO probe failed: {0}")]
    Diskio(#[from] DiskioError),
    #[error("DiskIO registration lookup failed: {0}")]
    Registry(#[from] crowdb_kv_client::Error),
    #[error("DiskIO profile is invalid: {0}")]
    Invalid(&'static str),
    #[error("DiskIO registration did not become ready")]
    Deadline,
}

/// # Errors
/// Requires every profile disk to accept a read-only fsync through its Group 0 route.
pub async fn verify_diskio_disks(
    management_seed: &str,
    profile: &DeploymentProfile,
) -> Result<(), StorageProbeError> {
    profile
        .validate()
        .map_err(|_| StorageProbeError::Invalid("deployment profile is invalid"))?;
    let disks = profile
        .disks
        .iter()
        .map(|disk| parse_disk_id(&disk.disk_id))
        .collect::<Result<Vec<_>, _>>()?;
    wait_for_registration(management_seed, profile).await?;
    let config = DiskioClientConfig {
        management_seeds: vec![management_seed.to_owned()],
        default_timeout: Duration::from_secs(2),
        ..DiskioClientConfig::default()
    };
    let client = DiskioClient::connect(config).await?;
    for disk in disks {
        client
            .fsync(disk, OperationOptions::within(Duration::from_secs(2)))
            .await?;
    }
    Ok(())
}

async fn wait_for_registration(
    management_seed: &str,
    profile: &DeploymentProfile,
) -> Result<(), StorageProbeError> {
    let node = profile
        .nodes
        .first()
        .ok_or(StorageProbeError::Invalid("node is absent"))?;
    let service = profile
        .services
        .iter()
        .find(|service| service.id == "diskio")
        .ok_or(StorageProbeError::Invalid("DiskIO service is absent"))?;
    let group_id = profile
        .disks
        .first()
        .ok_or(StorageProbeError::Invalid("disk is absent"))?
        .disk_group_id;
    let registry = ServiceRegistryClient::new(CrowdbKvClient::new(ClientConfig::new(vec![
        management_seed.to_owned()
    ])));
    registry.kv().refresh_topology().await?;
    let deadline = Instant::now() + READY_DEADLINE;
    loop {
        if let Some(instance) = registry.read_instance("diskio", node.node_id).await? {
            let owner = instance.extra.and_then(|extra| extra.diskdb);
            if instance.rpc_endpoint == service.probe.target
                && owner.as_ref().is_some_and(|owner| {
                    owner.rack_id == Some(node.rack_id)
                        && owner.node_id == Some(node.node_id)
                        && owner.owned_dg_ids == [group_id]
                })
            {
                return Ok(());
            }
            return Err(StorageProbeError::Invalid(
                "DiskIO registration conflicts with profile",
            ));
        }
        if Instant::now() >= deadline {
            return Err(StorageProbeError::Deadline);
        }
        sleep(PROBE_INTERVAL).await;
    }
}

fn parse_disk_id(value: &str) -> Result<DiskId, StorageProbeError> {
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(StorageProbeError::Invalid("disk ID is not 32 hex digits"));
    }
    let high = u64::from_str_radix(&value[..16], 16)
        .map_err(|_| StorageProbeError::Invalid("disk ID is invalid"))?;
    let low = u64::from_str_radix(&value[16..], 16)
        .map_err(|_| StorageProbeError::Invalid("disk ID is invalid"))?;
    Ok(DiskId::new(high, low))
}
