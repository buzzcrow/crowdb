// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! In-memory console operation inputs and topology snapshots. Durable cluster
//! records live in Group 0; process launch policy uses `config::web`.

use std::sync::atomic::AtomicU64;

use crowdb_protocol::{NodeId, RackId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::cluster::DiskGroupId;
use crate::error::{Error, Result};

pub mod web;

static TMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Ephemeral inputs for cluster operations and bootstrap. This struct is not
/// a durable topology store.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleConfig {
    #[serde(default, rename = "rack")]
    pub racks: Vec<RackEntry>,
    #[serde(default, rename = "node")]
    pub nodes: Vec<NodeEntry>,
    #[serde(default, rename = "server")]
    pub servers: Vec<ServerEntry>,
    #[serde(default)]
    pub stores: Vec<StoreEntry>,
    #[serde(default)]
    pub groups: Vec<GroupEntry>,
    #[serde(default, rename = "disk_group")]
    pub disk_groups: Vec<DiskGroupEntry>,
    #[serde(default, rename = "disk")]
    pub disks: Vec<DiskEntry>,
    /// Reproducible commands for locally deployed benchmark services.
    #[serde(default)]
    pub local_launches: BTreeMap<String, LocalLaunchSpec>,
}

/// Retained local process state used to restart a benchmark service without
/// changing its identity or endpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalLaunchSpec {
    pub program: String,
    /// Private credentials remain in a bounded file, never in the launch registry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_file: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    pub workdir: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RackEntry {
    pub id: RackId,
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeEntry {
    pub id: NodeId,
    pub rack_id: RackId,
    /// Default `127.0.0.1` for local simulated nodes.
    pub host: String,
    /// SSH port. Defaults to 22.
    #[serde(default = "default_ssh_port")]
    pub ssh_port: u16,
    /// SSH user for lifecycle ops. Empty string disables SSH and falls
    /// back to local-fork lifecycle (C3 path) for tests.
    #[serde(default)]
    pub ssh_user: String,
    /// Local secret-store lookup key shared across consoles, never secret material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_credential_ref: Option<String>,
    /// Optional explicit private-key path. `None` falls back to
    /// `~/.ssh/id_ed25519` then `~/.ssh/id_rsa`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_key: Option<String>,
    /// Optional password for password auth. Mutually exclusive with
    /// `ssh_key`. Plaintext on disk — operators are expected to rely on
    /// key auth in practice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_password: Option<String>,
}

fn default_ssh_port() -> u16 {
    22
}

impl NodeEntry {
    /// `true` if this node is configured to use SSH for lifecycle ops.
    #[must_use]
    pub fn ssh_enabled(&self) -> bool {
        !self.ssh_user.is_empty()
    }
}

/// Console-side disk-group entry. Mirrors the group-0 `DiskGroupKey`
/// placement (`rack_id`, `node_id`, `disk_group_id`) plus a human-readable
/// name. The console config is the operator's intent; group-0 sysdata is
/// the derived view synced by the console handlers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskGroupEntry {
    pub id: DiskGroupId,
    pub rack_id: RackId,
    pub node_id: NodeId,
    #[serde(default)]
    pub name: String,
}

/// Console-side disk entry. `disk_id` is a UUID hex string (stable across
/// moves). `disk_type` is `"Hdd"` or `"Ssd"`. Capacity / zone / unit sizes
/// are the physical disk's parameters, captured at add time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskEntry {
    pub disk_id: String,
    pub disk_group_id: DiskGroupId,
    pub rack_id: RackId,
    pub node_id: NodeId,
    pub disk_type: String,
    pub capacity_bytes: u64,
    pub zone_size_bytes: u64,
    pub unit_size_bytes: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub device_path: String,
}

/// Discriminator for ephemeral console deployment inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ServiceType {
    #[default]
    #[serde(rename = "paxos-kv")]
    PaxosKv,
    Diskdb,
    Chunkdb,
    Diskio,
    ChunkKv,
    AccessServer,
    /// Standalone crowdb-rpc-fb-server (C++ echo server for RPC bench).
    /// Not a full KV server — no management port or sysdata.
    Rpc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerEntry {
    /// Console-side identifier; must be unique within an operation context.
    pub id: String,
    /// Service URL. For KV this is the `crowdb-kv-server` management base
    /// URL; for `DiskDB` this is its public crowdb-rpc endpoint.
    pub url: String,
    /// Owning node id; populated for console-deployed instances. `None`
    /// for plain "registered external server" entries from C2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<NodeId>,
    /// crowdb-rpc base URL, e.g. `http://127.0.0.1:10100`. Populated for
    /// console-deployed instances.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rest_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_port: Option<u16>,
    #[serde(default)]
    pub auto_start: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub election_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// Service type discriminator. KV is the default for local fixtures.
    #[serde(default, skip_serializing_if = "is_default_service_type")]
    pub service_type: ServiceType,
    /// `--rpc-workers` value passed to the spawned `crowdb-kv-server`.
    /// `None` means the server's default (2) is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_workers: Option<u32>,
    /// `--no-fsync` flag passed to the spawned `crowdb-kv-server`.
    /// Persisted so restart reuses the same value.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub no_fsync: bool,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_default_service_type(st: &ServiceType) -> bool {
    *st == ServiceType::PaxosKv
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreEntry {
    pub store_id: u64,
    #[serde(default)]
    pub nodes: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupEntry {
    pub store_id: u64,
    pub group_id: u64,
    #[serde(default)]
    pub replicas: Vec<ReplicaEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaEntry {
    pub replica_id: u64,
    pub node_id: NodeId,
}

impl ServerEntry {
    /// Convenience constructor for a plain registered server (C2 style).
    #[must_use]
    pub fn new(id: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            url: url.into(),
            node_id: None,
            rpc_url: None,
            rest_port: None,
            rpc_port: None,
            auto_start: false,
            binary: None,
            election_profile: None,
            pid: None,
            service_type: ServiceType::PaxosKv,
            rpc_workers: None,
            no_fsync: false,
        }
    }
}

impl ConsoleConfig {
    /// Add a server entry. Rejects duplicate `id` and duplicate `url`.
    ///
    /// # Errors
    /// Returns `Error::Conflict` on duplicate id; `Error::Validation` on
    /// duplicate url.
    pub fn add_server(&mut self, entry: ServerEntry) -> Result<()> {
        if self.servers.iter().any(|s| s.id == entry.id) {
            return Err(Error::Conflict {
                kind: "server".into(),
                id: entry.id,
            });
        }
        if self.servers.iter().any(|s| s.url == entry.url) {
            return Err(Error::Validation {
                field: "url".into(),
                message: format!("url {} already registered", entry.url),
            });
        }
        self.servers.push(entry);
        Ok(())
    }

    /// Remove a server entry by id.
    ///
    /// # Errors
    /// Returns `Error::NotFound` if no entry has that id.
    pub(crate) fn remove_server(&mut self, id: &str) -> Result<ServerEntry> {
        let pos = self
            .servers
            .iter()
            .position(|s| s.id == id)
            .ok_or_else(|| Error::NotFound {
                kind: "server".into(),
                id: id.to_string(),
            })?;
        let server = self.servers.remove(pos);
        self.local_launches.remove(&server.id);
        Ok(server)
    }

    /// All server URLs in registration order.
    #[must_use]
    pub fn server_urls(&self) -> Vec<String> {
        self.servers.iter().map(|s| s.url.clone()).collect()
    }

    pub fn record_store(&mut self, store_id: u64, mut nodes: Vec<NodeId>) {
        nodes.sort_unstable();
        nodes.dedup();
        if let Some(store) = self.stores.iter_mut().find(|s| s.store_id == store_id) {
            store.nodes = nodes;
        } else {
            self.stores.push(StoreEntry { store_id, nodes });
        }
        self.stores.sort_by_key(|s| s.store_id);
    }

    pub fn ensure_store_node(&mut self, store_id: u64, node_id: NodeId) {
        if let Some(store) = self.stores.iter_mut().find(|s| s.store_id == store_id) {
            if !store.nodes.contains(&node_id) {
                store.nodes.push(node_id);
                store.nodes.sort_unstable();
            }
        } else {
            self.stores.push(StoreEntry {
                store_id,
                nodes: vec![node_id],
            });
            self.stores.sort_by_key(|s| s.store_id);
        }
    }

    pub fn remove_store_record(&mut self, store_id: u64) {
        self.stores.retain(|s| s.store_id != store_id);
        self.groups.retain(|g| g.store_id != store_id);
    }

    pub fn record_group(&mut self, store_id: u64, group_id: u64, mut replicas: Vec<ReplicaEntry>) {
        replicas.sort_by_key(|r| r.replica_id);
        if let Some(group) = self
            .groups
            .iter_mut()
            .find(|g| g.store_id == store_id && g.group_id == group_id)
        {
            group.replicas = replicas;
        } else {
            self.groups.push(GroupEntry {
                store_id,
                group_id,
                replicas,
            });
        }
        self.groups.sort_by_key(|g| (g.store_id, g.group_id));
    }

    pub fn remove_group_record(&mut self, store_id: u64, group_id: u64) {
        self.groups
            .retain(|g| !(g.store_id == store_id && g.group_id == group_id));
    }

    pub fn add_group_replica(&mut self, store_id: u64, group_id: u64, replica: ReplicaEntry) {
        if let Some(group) = self
            .groups
            .iter_mut()
            .find(|g| g.store_id == store_id && g.group_id == group_id)
        {
            if let Some(existing) = group
                .replicas
                .iter_mut()
                .find(|r| r.replica_id == replica.replica_id)
            {
                *existing = replica;
            } else {
                group.replicas.push(replica);
                group.replicas.sort_by_key(|r| r.replica_id);
            }
        } else {
            self.groups.push(GroupEntry {
                store_id,
                group_id,
                replicas: vec![replica],
            });
            self.groups.sort_by_key(|g| (g.store_id, g.group_id));
        }
    }

    pub fn remove_group_replica(&mut self, store_id: u64, group_id: u64, replica_id: u64) {
        if let Some(group) = self
            .groups
            .iter_mut()
            .find(|g| g.store_id == store_id && g.group_id == group_id)
        {
            group.replicas.retain(|r| r.replica_id != replica_id);
        }
        self.groups
            .retain(|g| !(g.store_id == store_id && g.group_id == group_id && g.replicas.is_empty()));
    }

    #[must_use]
    pub fn group(&self, store_id: u64, group_id: u64) -> Option<&GroupEntry> {
        self.groups
            .iter()
            .find(|g| g.store_id == store_id && g.group_id == group_id)
    }

    pub fn purge_node_topology(&mut self, node_id: NodeId) {
        for store in &mut self.stores {
            store.nodes.retain(|n| *n != node_id);
        }
        self.stores.retain(|s| !s.nodes.is_empty());
        for group in &mut self.groups {
            group.replicas.retain(|r| r.node_id != node_id);
        }
        self.groups.retain(|g| !g.replicas.is_empty());
    }

    /// Add a rack. Rejects duplicate id.
    ///
    /// # Errors
    /// `Error::Conflict` on duplicate id.
    pub fn add_rack(&mut self, entry: RackEntry) -> Result<()> {
        if self.racks.iter().any(|r| r.id == entry.id) {
            return Err(Error::Conflict {
                kind: "rack".into(),
                id: entry.id.to_string(),
            });
        }
        self.racks.push(entry);
        Ok(())
    }

    /// Remove a rack by id.
    ///
    /// # Errors
    /// `Error::NotFound` if no rack with that id; `Error::Conflict` if any
    /// node still references the rack.
    pub fn remove_rack(&mut self, id: RackId) -> Result<RackEntry> {
        if self.nodes.iter().any(|n| n.rack_id == id) {
            return Err(Error::Conflict {
                kind: "rack".into(),
                id: format!("{id}: rack still referenced by nodes"),
            });
        }
        let pos = self
            .racks
            .iter()
            .position(|r| r.id == id)
            .ok_or_else(|| Error::NotFound {
                kind: "rack".into(),
                id: id.to_string(),
            })?;
        Ok(self.racks.remove(pos))
    }

    /// Add a node. Rejects duplicate id and unknown rack.
    ///
    /// # Errors
    /// `Error::Conflict` on duplicate id; `Error::Validation` on unknown rack.
    pub fn add_node(&mut self, entry: NodeEntry) -> Result<()> {
        if self.nodes.iter().any(|n| n.id == entry.id) {
            return Err(Error::Conflict {
                kind: "node".into(),
                id: entry.id.to_string(),
            });
        }
        if !self.racks.iter().any(|r| r.id == entry.rack_id) {
            return Err(Error::Validation {
                field: "rack_id".into(),
                message: format!("unknown rack {}", entry.rack_id),
            });
        }
        self.nodes.push(entry);
        Ok(())
    }

    /// Remove a node by id.
    ///
    /// # Errors
    /// `Error::NotFound` if no node; `Error::Conflict` if a server is
    /// still deployed to the node.
    pub fn remove_node(&mut self, id: NodeId) -> Result<NodeEntry> {
        if self.servers.iter().any(|s| s.node_id == Some(id)) {
            return Err(Error::Conflict {
                kind: "node".into(),
                id: format!("{id}: node still hosts a deployed server"),
            });
        }
        let pos = self
            .nodes
            .iter()
            .position(|n| n.id == id)
            .ok_or_else(|| Error::NotFound {
                kind: "node".into(),
                id: id.to_string(),
            })?;
        Ok(self.nodes.remove(pos))
    }

    /// Look up a node by id.
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&NodeEntry> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Look up a server entry by id.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn server(&self, id: &str) -> Option<&ServerEntry> {
        self.servers.iter().find(|s| s.id == id)
    }

    /// Look up the server deployed on a given node.
    #[must_use]
    pub fn server_for_node(&self, node_id: NodeId) -> Option<&ServerEntry> {
        self.servers
            .iter()
            .find(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::PaxosKv)
    }

    /// Look up the server deployed on a given node (mutable).
    pub fn server_for_node_mut(&mut self, node_id: NodeId) -> Option<&mut ServerEntry> {
        self.servers
            .iter_mut()
            .find(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::PaxosKv)
    }

    /// Remove the KV server entry deployed on a given node.
    ///
    /// # Errors
    /// `Error::NotFound` if no KV server is deployed on this node.
    pub fn remove_server_for_node(&mut self, node_id: NodeId) -> Result<ServerEntry> {
        let pos = self
            .servers
            .iter()
            .position(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::PaxosKv)
            .ok_or_else(|| Error::NotFound {
                kind: "server".into(),
                id: format!("no server on node {node_id}"),
            })?;
        Ok(self.servers.remove(pos))
    }

    /// Mutable look-up for in-place updates (e.g. `pid` after restart).
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn server_mut(&mut self, id: &str) -> Option<&mut ServerEntry> {
        self.servers.iter_mut().find(|s| s.id == id)
    }

    // ── disk-group ────────────────────────────────────────────────

    /// Add a disk-group. Rejects duplicate id and unknown node.
    ///
    /// # Errors
    /// `Error::Conflict` on duplicate id; `Error::Validation` on unknown
    /// node.
    pub fn add_disk_group(&mut self, entry: DiskGroupEntry) -> Result<()> {
        if self.disk_groups.iter().any(|dg| dg.id == entry.id) {
            return Err(Error::Conflict {
                kind: "disk_group".into(),
                id: entry.id.to_string(),
            });
        }
        if !self.nodes.iter().any(|n| n.id == entry.node_id) {
            return Err(Error::Validation {
                field: "node_id".into(),
                message: format!("unknown node {}", entry.node_id),
            });
        }
        self.disk_groups.push(entry);
        Ok(())
    }

    /// Remove a disk-group by id.
    ///
    /// # Errors
    /// `Error::NotFound` if no disk-group; `Error::Conflict` if any
    /// disk still references the disk-group.
    pub fn remove_disk_group(&mut self, id: DiskGroupId) -> Result<DiskGroupEntry> {
        if self.disks.iter().any(|d| d.disk_group_id == id) {
            return Err(Error::Conflict {
                kind: "disk_group".into(),
                id: format!("{id}: disk_group still has disks"),
            });
        }
        let pos = self
            .disk_groups
            .iter()
            .position(|dg| dg.id == id)
            .ok_or_else(|| Error::NotFound {
                kind: "disk_group".into(),
                id: id.to_string(),
            })?;
        Ok(self.disk_groups.remove(pos))
    }

    /// Look up a disk-group by id.
    #[must_use]
    pub fn disk_group(&self, id: DiskGroupId) -> Option<&DiskGroupEntry> {
        self.disk_groups.iter().find(|dg| dg.id == id)
    }

    /// List disk-groups on a node.
    #[must_use]
    pub fn disk_groups_on_node(&self, node_id: NodeId) -> Vec<&DiskGroupEntry> {
        self.disk_groups
            .iter()
            .filter(|dg| dg.node_id == node_id)
            .collect()
    }

    // ── disk ──────────────────────────────────────────────────────

    /// Add a disk. Rejects duplicate `disk_id` and unknown `disk_group`.
    ///
    /// # Errors
    /// `Error::Conflict` on duplicate `disk_id`; `Error::Validation` on
    /// unknown `disk_group`.
    pub fn add_disk(&mut self, entry: DiskEntry) -> Result<()> {
        if self.disks.iter().any(|d| d.disk_id == entry.disk_id) {
            return Err(Error::Conflict {
                kind: "disk".into(),
                id: entry.disk_id.clone(),
            });
        }
        if !self.disk_groups.iter().any(|dg| dg.id == entry.disk_group_id) {
            return Err(Error::Validation {
                field: "disk_group_id".into(),
                message: format!("unknown disk_group {}", entry.disk_group_id),
            });
        }
        self.disks.push(entry);
        Ok(())
    }

    /// Remove a disk by `disk_id`.
    ///
    /// # Errors
    /// `Error::NotFound` if no disk with that `disk_id`.
    pub fn remove_disk(&mut self, disk_id: &str) -> Result<DiskEntry> {
        let pos = self
            .disks
            .iter()
            .position(|d| d.disk_id == disk_id)
            .ok_or_else(|| Error::NotFound {
                kind: "disk".into(),
                id: disk_id.to_string(),
            })?;
        Ok(self.disks.remove(pos))
    }

    /// Look up a disk by `disk_id`.
    #[must_use]
    pub fn disk(&self, disk_id: &str) -> Option<&DiskEntry> {
        self.disks.iter().find(|d| d.disk_id == disk_id)
    }

    /// List disks in a disk-group.
    #[must_use]
    pub fn disks_in_group(&self, dg_id: DiskGroupId) -> Vec<&DiskEntry> {
        self.disks.iter().filter(|d| d.disk_group_id == dg_id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{ConsoleConfig, ServerEntry};

    #[test]
    fn duplicate_id_rejected() {
        let mut cfg = ConsoleConfig::default();
        cfg.add_server(ServerEntry::new("a", "http://1")).unwrap();
        let err = cfg.add_server(ServerEntry::new("a", "http://2")).unwrap_err();
        assert!(matches!(err, crate::error::Error::Conflict { .. }));
    }

    #[test]
    fn duplicate_url_rejected() {
        let mut cfg = ConsoleConfig::default();
        cfg.add_server(ServerEntry::new("a", "http://1")).unwrap();
        let err = cfg.add_server(ServerEntry::new("b", "http://1")).unwrap_err();
        assert!(matches!(err, crate::error::Error::Validation { .. }));
    }

    #[test]
    fn remove_missing_is_not_found() {
        let mut cfg = ConsoleConfig::default();
        let err = cfg.remove_server("ghost").unwrap_err();
        assert!(matches!(err, crate::error::Error::NotFound { .. }));
    }
}
