// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authoritative `DiskIO` topology construction.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crowdb_kv_client::{DiskRecord, HardwareClient, ServiceRegistryClient};
use crowdb_protocol::common::InstanceValue;

use crate::{DiskId, DiskioError, DiskioResult};

#[cfg(feature = "test-util")]
#[derive(Debug, Clone)]
pub struct TestTopologyInstance {
    pub instance_id: u64,
    pub endpoint: String,
    pub rack_id: Option<u64>,
    pub node_id: Option<u64>,
    pub disk_group_ids: Vec<u64>,
}

#[cfg(feature = "test-util")]
#[derive(Debug, Clone, Copy)]
pub struct TestTopologyDisk {
    pub disk_id: DiskId,
    pub rack_id: u64,
    pub node_id: u64,
    pub disk_group_id: u64,
}

#[cfg(feature = "test-util")]
/// Validate injected observations through the production topology builder.
///
/// # Errors
///
/// Returns a topology error for missing, duplicate, mismatched, or malformed
/// ownership observations.
pub fn validate_topology_for_tests(
    instances: Vec<TestTopologyInstance>,
    disks: Vec<TestTopologyDisk>,
) -> DiskioResult<usize> {
    use crowdb_protocol::common::{DiskdbExtra, ServiceExtra};

    let instances = instances.into_iter().map(|instance| InstanceValue {
        instance_id: instance.instance_id,
        rpc_endpoint: instance.endpoint,
        last_heartbeat_ms: 1,
        extra: Some(ServiceExtra {
            diskdb: Some(DiskdbExtra {
                rack_id: instance.rack_id,
                node_id: instance.node_id,
                owned_dg_ids: instance.disk_group_ids,
                group_usages: Vec::new(),
            }),
            kv_server: None,
            chunk_kv: None,
        }),
    });
    let disks = disks
        .into_iter()
        .map(|disk| DiskRecord {
            rack_id: disk.rack_id,
            node_id: disk.node_id,
            disk_group_id: disk.disk_group_id,
            disk_id: crowdb_protocol::common::DiskId {
                high: disk.disk_id.high,
                low: disk.disk_id.low,
            },
            value: crowdb_protocol::diskdb::rpc::DiskValue::default(),
        })
        .collect();
    build(instances, disks).map(|draft| draft.routes.len())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiskRoute {
    pub(crate) rack_id: u64,
    pub(crate) node_id: u64,
    pub(crate) disk_group_id: u64,
    pub(crate) instance_id: u64,
    pub(crate) endpoint: Arc<str>,
    pub(crate) pool_key: Arc<str>,
}

#[derive(Debug)]
pub(crate) struct TopologyDraft {
    pub(crate) routes: HashMap<DiskId, DiskRoute>,
    pub(crate) nodes: usize,
    pub(crate) endpoints: usize,
}

pub(crate) async fn discover(
    service: &ServiceRegistryClient,
    hardware: &HardwareClient,
) -> DiskioResult<TopologyDraft> {
    let instances = service
        .read_all_diskio_instances()
        .await
        .map_err(|error| DiskioError::TopologyUnavailable(format!("read DiskIO instances: {error}")))?;
    let disks = hardware
        .list_all_disks()
        .await
        .map_err(|error| DiskioError::TopologyUnavailable(format!("read hardware disks: {error}")))?;
    build(instances.into_iter().map(|(_, value)| value), disks)
}

fn build(
    instances: impl IntoIterator<Item = InstanceValue>,
    disks: Vec<DiskRecord>,
) -> DiskioResult<TopologyDraft> {
    let mut owners = HashMap::<u64, (u64, u64, u64, Arc<str>, Arc<str>)>::new();
    for instance in instances {
        let extra = instance.extra.and_then(|extra| extra.diskdb).ok_or_else(|| {
            DiskioError::TopologyInconsistent(format!(
                "DiskIO instance {} has no ownership metadata",
                instance.instance_id
            ))
        })?;
        let rack_id = extra.rack_id.ok_or_else(|| {
            DiskioError::TopologyInconsistent(format!(
                "DiskIO instance {} has no rack identity",
                instance.instance_id
            ))
        })?;
        let node_id = extra.node_id.ok_or_else(|| {
            DiskioError::TopologyInconsistent(format!(
                "DiskIO instance {} has no node identity",
                instance.instance_id
            ))
        })?;
        parse_endpoint(&instance.rpc_endpoint)?;
        let endpoint: Arc<str> = Arc::from(instance.rpc_endpoint.as_str());
        let pool_key: Arc<str> = Arc::from(format!("{}@{}", instance.instance_id, endpoint));
        for disk_group_id in extra.owned_dg_ids {
            if let Some(previous) = owners.insert(
                disk_group_id,
                (
                    rack_id,
                    node_id,
                    instance.instance_id,
                    Arc::clone(&endpoint),
                    Arc::clone(&pool_key),
                ),
            ) {
                return Err(DiskioError::TopologyInconsistent(format!(
                    "disk group {disk_group_id} has duplicate owners {} and {}",
                    previous.2, instance.instance_id
                )));
            }
        }
    }

    let mut routes = HashMap::with_capacity(disks.len());
    let mut nodes = HashSet::new();
    let mut endpoints = HashSet::new();
    for disk in disks {
        let owner = owners.get(&disk.disk_group_id).ok_or_else(|| {
            DiskioError::TopologyInconsistent(format!(
                "disk group {} has no live DiskIO owner",
                disk.disk_group_id
            ))
        })?;
        if owner.0 != disk.rack_id || owner.1 != disk.node_id {
            return Err(DiskioError::TopologyInconsistent(format!(
                "disk group {} is under hardware {}/{} but DiskIO {} reports {}/{}",
                disk.disk_group_id, disk.rack_id, disk.node_id, owner.2, owner.0, owner.1
            )));
        }
        let disk_id = DiskId::new(disk.disk_id.high, disk.disk_id.low);
        let route = DiskRoute {
            rack_id: disk.rack_id,
            node_id: disk.node_id,
            disk_group_id: disk.disk_group_id,
            instance_id: owner.2,
            endpoint: Arc::clone(&owner.3),
            pool_key: Arc::clone(&owner.4),
        };
        if routes.insert(disk_id, route).is_some() {
            return Err(DiskioError::TopologyInconsistent(format!(
                "disk {}:{} appears more than once in hardware topology",
                disk_id.high, disk_id.low
            )));
        }
        nodes.insert((disk.rack_id, disk.node_id));
        endpoints.insert(Arc::clone(&owner.3));
    }
    Ok(TopologyDraft {
        routes,
        nodes: nodes.len(),
        endpoints: endpoints.len(),
    })
}

pub(crate) fn parse_endpoint(endpoint: &str) -> DiskioResult<(&str, i32)> {
    let endpoint = endpoint.strip_prefix("http://").unwrap_or(endpoint);
    if endpoint.starts_with("https://") {
        return Err(DiskioError::TopologyInconsistent(
            "DiskIO RPC endpoint does not support TLS URLs".into(),
        ));
    }
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| DiskioError::TopologyInconsistent(format!("invalid DiskIO endpoint {endpoint}")))?;
    if host.is_empty() {
        return Err(DiskioError::TopologyInconsistent(
            "DiskIO endpoint host is empty".into(),
        ));
    }
    let port = port.parse::<u16>().map_err(|error| {
        DiskioError::TopologyInconsistent(format!("invalid DiskIO endpoint {endpoint}: {error}"))
    })?;
    if port == 0 {
        return Err(DiskioError::TopologyInconsistent(
            "DiskIO endpoint port must be nonzero".into(),
        ));
    }
    Ok((host, i32::from(port)))
}
