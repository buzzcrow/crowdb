// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    NodeSetupChoices, NodeSetupRequest, NodeSetupStatus, SetupService, SetupServiceKind, SetupStorage,
};
use std::collections::BTreeSet;

impl NodeSetupStatus {
    /// Validate an authenticated observation against its retained admission.
    ///
    /// # Errors
    /// Rejects another operation/node/cluster or an unsupported status version.
    /// An unbound setup may still omit its eventual numeric node and cluster.
    pub fn validate_identity(
        &self,
        discovery: &str,
        operation: &str,
        node: u64,
        cluster: Option<&str>,
    ) -> Result<(), &'static str> {
        if self.version != 1
            || self.discovery_id != discovery
            || self.operation_id != operation
            || self.node_id.is_some_and(|actual| actual != node)
            || cluster
                .zip(self.cluster_id.as_deref())
                .is_some_and(|(expected, actual)| expected != actual)
        {
            return Err("node setup observation differs from retained admission identity");
        }
        Ok(())
    }
}

impl NodeSetupRequest {
    /// Check bounds and choices before durable monitor acceptance.
    ///
    /// # Errors
    /// Rejects unknown versions, oversized inputs, ambiguous service selections,
    /// unsupported listeners, duplicate media and missing initialization consent.
    /// UUID, exact mount identity and cluster admission are checked by the monitor.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != 1 || self.discovery_id.len() != 36 || self.operation_id.len() != 36 {
            return Err("invalid node setup version or identity");
        }
        if serde_json::to_vec(self)
            .map_err(|_| "node setup cannot be encoded")?
            .len()
            > 32768
        {
            return Err("node setup exceeds its control message bound");
        }
        self.choices.validate()
    }
}

impl NodeSetupChoices {
    /// Validate storage and service inputs before accepting a deployment policy.
    ///
    /// # Errors
    /// Rejects invalid geometry, duplicated media and ambiguous listeners.
    pub fn validate(&self) -> Result<(), &'static str> {
        let mut kinds = BTreeSet::new();
        let mut ports = BTreeSet::new();
        if self.services.len() != 6 {
            return Err("node setup requires exactly six service choices");
        }
        for service in &self.services {
            if !kinds.insert(service.kind) || !valid_listeners(service, &mut ports) {
                return Err("duplicate service choice or invalid listener configuration");
            }
        }
        match &self.storage {
            SetupStorage::Diskless => {}
            SetupStorage::Automatic {
                capacity_bytes,
                reserve_bytes,
                ..
            } => {
                let zone = 32 * 1024 * 1024 * 1024;
                if *capacity_bytes == 0
                    || *capacity_bytes > i64::MAX as u64
                    || *reserve_bytes > i64::MAX as u64
                    || capacity_bytes % (64 * 1024 * 1024) != 0
                    || (*capacity_bytes > zone && capacity_bytes % zone != 0)
                {
                    return Err("invalid default file capacity, geometry or reserve");
                }
            }
            SetupStorage::Supplied { disks } => {
                if disks.is_empty() || disks.len() > 64 {
                    return Err("select one to 64 supplied disks or diskless");
                }
                let mut paths = BTreeSet::new();
                let mut identities = BTreeSet::new();
                for disk in disks {
                    if !disk.initialize {
                        return Err("selected medium requires explicit initialization agreement");
                    }
                    if disk.backend == crate::mgmt::node::storage::DiskBackend::BlockDevice
                        && disk.resource_identity.is_none()
                    {
                        return Err("raw medium selection requires canonical resource lineage");
                    }
                    if let Some(resource) = &disk.resource_identity {
                        crate::disk_storage::PhysicalBacking {
                            physical_host_id: "selected-medium".into(),
                            resource_identity: resource.clone(),
                            capacity_bytes: disk.capacity_bytes,
                        }
                        .validate()?;
                    }
                    if disk.path.is_empty()
                        || disk.path.len() > 512
                        || disk.path.contains('\0')
                        || disk.backing_identity.is_empty()
                        || disk.backing_identity.len() > 256
                        || disk.capacity_bytes == 0
                        || disk.capacity_bytes > i64::MAX as u64
                        || disk
                            .media_type
                            .is_some_and(|value| crate::diskdb::rpc::DiskType::try_from(value).is_err())
                        || !paths.insert(&disk.path)
                        || !identities
                            .insert(disk.resource_identity.as_ref().unwrap_or(&disk.backing_identity))
                    {
                        return Err("invalid or duplicate supplied medium observation");
                    }
                }
            }
        }
        Ok(())
    }
}

fn valid_listeners(service: &SetupService, ports: &mut BTreeSet<u16>) -> bool {
    use SetupServiceKind::{AccessServer, Chunkdb, Diskdb, Diskio};
    let listeners = &service.listeners;
    if service.kind == SetupServiceKind::PaxosKv
        && (listeners
            .http_port
            .is_some_and(|port| port != crate::PAXOS_KV_SERVER_MGMT_BASE)
            || listeners
                .rpc_port
                .is_some_and(|port| port != crate::PAXOS_KV_SERVER_RPC_BASE))
    {
        return false;
    }
    if (service.kind != Chunkdb && listeners.dynamic_ownership.is_some())
        || (service.kind != Diskdb && listeners.keepalive_interval_secs.is_some())
        || (service.kind != AccessServer
            && (listeners.s3_port.is_some()
                || listeners.health_port.is_some()
                || listeners.dataset_port.is_some()))
        || (service.kind == AccessServer && listeners.rpc_port.is_some())
        || (service.kind == Diskio && listeners.http_port.is_some())
    {
        return false;
    }
    if !service.enabled {
        return true;
    }
    for port in [
        listeners.http_port,
        listeners.rpc_port,
        listeners.s3_port,
        listeners.health_port,
        listeners.dataset_port,
    ]
    .into_iter()
    .flatten()
    {
        if port == 0 || !ports.insert(port) {
            return false;
        }
    }
    true
}
