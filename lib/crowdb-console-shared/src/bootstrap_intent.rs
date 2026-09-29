// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable pre-Group-0 topology intent for interrupted bootstrap recovery.

use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::config::{ConsoleConfig, NodeEntry, RackEntry, ServerEntry, ServiceType};
use crate::error::{Error, Result};

const VERSION: u32 = 1;
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapIntent {
    version: u32,
    members: Vec<u64>,
    racks: Vec<IntentRack>,
    nodes: Vec<IntentNode>,
    servers: Vec<IntentServer>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntentRack {
    id: u64,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntentNode {
    id: u64,
    rack_id: u64,
    host: String,
    ssh_port: u16,
    ssh_user: String,
    ssh_credential_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntentServer {
    id: String,
    node_id: u64,
    management_url: String,
    rpc_url: Option<String>,
}

impl BootstrapIntent {
    /// Capture immutable hardware and KV endpoint identities before bootstrap.
    ///
    /// # Errors
    /// Rejects missing members, missing management endpoints and inline SSH secrets.
    pub fn capture(config: &ConsoleConfig, members: &[u64]) -> Result<Self> {
        if members.is_empty()
            || members
                .iter()
                .enumerate()
                .any(|(index, node)| members[..index].contains(node))
        {
            return Err(Error::Config(
                "bootstrap members must be nonempty and distinct".into(),
            ));
        }
        let nodes: Vec<_> = config
            .nodes
            .iter()
            .map(|node| {
                if node.ssh_key.is_some() || node.ssh_password.is_some() {
                    return Err(Error::Config(
                        "bootstrap intent cannot store inline SSH secrets".into(),
                    ));
                }
                Ok(IntentNode {
                    id: node.id,
                    rack_id: node.rack_id,
                    host: node.host.clone(),
                    ssh_port: node.ssh_port,
                    ssh_user: node.ssh_user.clone(),
                    ssh_credential_ref: node.ssh_credential_ref.clone(),
                })
            })
            .collect::<Result<_>>()?;
        let servers: Vec<_> = config
            .servers
            .iter()
            .filter(|server| server.service_type == ServiceType::Kv)
            .map(|server| {
                let node_id = server
                    .node_id
                    .ok_or_else(|| Error::Config("bootstrap KV server has no node id".into()))?;
                Ok(IntentServer {
                    id: server.id.clone(),
                    node_id,
                    management_url: server.url.clone(),
                    rpc_url: server.rpc_url.clone(),
                })
            })
            .collect::<Result<_>>()?;
        let mut rack_ids = HashSet::new();
        if config.racks.iter().any(|rack| !rack_ids.insert(rack.id)) {
            return Err(Error::Config("duplicate bootstrap rack id".into()));
        }
        let mut node_ids = HashSet::new();
        if nodes
            .iter()
            .any(|node| !node_ids.insert(node.id) || !rack_ids.contains(&node.rack_id))
        {
            return Err(Error::Config("duplicate or orphan bootstrap node".into()));
        }
        let mut endpoint_nodes = HashSet::new();
        if servers.iter().any(|server| {
            !endpoint_nodes.insert(server.node_id)
                || !node_ids.contains(&server.node_id)
                || server.management_url.is_empty()
        }) {
            return Err(Error::Config("duplicate or invalid bootstrap KV endpoint".into()));
        }
        for member in members {
            if !nodes.iter().any(|node| node.id == *member)
                || !servers.iter().any(|server| server.node_id == *member)
            {
                return Err(Error::Config(format!(
                    "bootstrap member {member} lacks node or KV endpoint"
                )));
            }
        }
        Ok(Self {
            version: VERSION,
            members: members.to_vec(),
            racks: config
                .racks
                .iter()
                .map(|rack| IntentRack {
                    id: rack.id,
                    name: rack.name.clone(),
                })
                .collect(),
            nodes,
            servers,
        })
    }

    #[must_use]
    pub fn members(&self) -> &[u64] {
        &self.members
    }

    /// Create the intent once, accepting an identical interrupted attempt.
    ///
    /// # Errors
    /// Rejects changed intent, symlinks, invalid content or I/O errors.
    pub fn seal(&self, path: &Path) -> Result<()> {
        if path.exists() || fs::symlink_metadata(path).is_ok() {
            return if Self::load(path)? == *self {
                Ok(())
            } else {
                Err(Error::Conflict {
                    kind: "bootstrap intent".into(),
                    id: path.display().to_string(),
                })
            };
        }
        let parent = path
            .parent()
            .ok_or_else(|| Error::Config("bootstrap intent has no parent".into()))?;
        fs::create_dir_all(parent)?;
        let data = toml::to_string(self).map_err(|error| Error::Config(error.to_string()))?;
        let temporary = path.with_extension(format!(
            "bootstrap-tmp-{}-{}",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)?;
            file.write_all(data.as_bytes())?;
            file.sync_all()?;
            match fs::hard_link(&temporary, path) {
                Ok(()) => {
                    fs::File::open(parent)?.sync_all()?;
                    Ok(())
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if Self::load(path)? == *self {
                        Ok(())
                    } else {
                        Err(Error::Conflict {
                            kind: "bootstrap intent".into(),
                            id: path.display().to_string(),
                        })
                    }
                }
                Err(error) => Err(Error::Io(error)),
            }
        })();
        let _ = fs::remove_file(&temporary);
        result
    }

    /// Read a previously sealed intent without accepting a legacy console file.
    ///
    /// # Errors
    /// Rejects symlinks, unknown fields, invalid version or I/O errors.
    pub fn load(path: &Path) -> Result<Self> {
        if !fs::symlink_metadata(path)?.file_type().is_file() {
            return Err(Error::Config("bootstrap intent is not a regular file".into()));
        }
        let intent: Self =
            toml::from_str(&fs::read_to_string(path)?).map_err(|error| Error::Config(error.to_string()))?;
        if intent.version != VERSION {
            return Err(Error::Config("unsupported bootstrap intent version".into()));
        }
        if Self::capture(&intent.to_config(), &intent.members)? != intent {
            return Err(Error::Config("bootstrap intent contents are inconsistent".into()));
        }
        Ok(intent)
    }

    /// Restore only the bootstrap inputs into a fresh in-memory console context.
    #[must_use]
    pub fn to_config(&self) -> ConsoleConfig {
        ConsoleConfig {
            racks: self
                .racks
                .iter()
                .map(|rack| RackEntry {
                    id: rack.id,
                    name: rack.name.clone(),
                })
                .collect(),
            nodes: self
                .nodes
                .iter()
                .map(|node| NodeEntry {
                    id: node.id,
                    rack_id: node.rack_id,
                    host: node.host.clone(),
                    ssh_port: node.ssh_port,
                    ssh_user: node.ssh_user.clone(),
                    ssh_key: None,
                    ssh_password: None,
                    ssh_credential_ref: node.ssh_credential_ref.clone(),
                })
                .collect(),
            servers: self
                .servers
                .iter()
                .map(|server| ServerEntry {
                    id: server.id.clone(),
                    url: server.management_url.clone(),
                    node_id: Some(server.node_id),
                    rpc_url: server.rpc_url.clone(),
                    rest_port: None,
                    rpc_port: None,
                    auto_start: false,
                    binary: None,
                    election_profile: None,
                    pid: None,
                    service_type: ServiceType::Kv,
                    rpc_workers: None,
                    no_fsync: false,
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Remove the intent only after Group 0 contents were verified.
    ///
    /// # Errors
    /// Returns an I/O error if removal fails.
    pub(crate) fn clear_verified(path: &Path) -> Result<()> {
        fs::remove_file(path)?;
        if let Some(parent) = path.parent() {
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}
