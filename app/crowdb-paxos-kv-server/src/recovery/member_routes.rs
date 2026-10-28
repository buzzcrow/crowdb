// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Monitor-owned endpoint evidence changes connections, never membership.

use crate::store_registry::PaxosKvStoreRegistry;
use crowdb_paxos_kv::cluster::group::PxGroup;
use crowdb_protocol::mgmt::{
    node::recovery::{MemberEndpoint, MemberEndpoints, RecoveryGroups},
    GroupCreationStatus, SystemBootstrapIdentity,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, Read, Write},
    net::SocketAddr,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    root: PathBuf,
    boot_id: String,
}

#[derive(Deserialize)]
struct Accepted {
    node_id: u64,
    bootstrap: SystemBootstrapIdentity,
}

#[derive(Deserialize)]
struct BootstrapOwner {
    identity: SystemBootstrapIdentity,
    replica_id: u64,
}

/// # Errors
/// Rejects invalid local monitor state and private-file publication failures.
pub fn configure(config_root: &Path, root: &Path) -> io::Result<()> {
    let endpoint: MemberEndpoint = read(&root.join("node-endpoint.json"))?;
    fs::create_dir_all(config_root)?;
    let source = Source {
        root: root.to_path_buf(),
        boot_id: endpoint.boot_id,
    };
    let temporary = config_root.join(format!(".member-source-{}.tmp", uuid::Uuid::new_v4()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&serde_json::to_vec(&source)?)?;
    file.sync_all()?;
    fs::rename(temporary, config_root.join("member-route-source.json"))?;
    File::open(config_root)?.sync_all()
}

/// Fence old addresses before this group can start its election driver.
///
/// # Errors
/// Rejects corrupt local recovery ownership; absent evidence leaves routes blocked.
pub fn apply(group: &PxGroup, config_root: &Path, store_id: u64) -> io::Result<()> {
    let source: Source = match read(&config_root.join("member-route-source.json")) {
        Ok(source) => source,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            group.set_recovery_routes(&BTreeMap::new());
            return Err(error);
        }
    };
    let routes = resolve(group, config_root, store_id, &source);
    match routes {
        Ok(routes) => {
            group.set_recovery_routes(&routes);
            Ok(())
        }
        Err(error) => {
            group.set_recovery_routes(&BTreeMap::new());
            Err(error)
        }
    }
}

fn resolve(
    group: &PxGroup,
    config_root: &Path,
    store: u64,
    source: &Source,
) -> io::Result<BTreeMap<u64, SocketAddr>> {
    let accepted: Accepted = read(&source.root.join("accepted-node.json"))?;
    let groups: RecoveryGroups = read(&source.root.join("recovery-groups.json"))?;
    if groups.bootstrap != accepted.bootstrap {
        return Err(invalid("recovery ownership differs"));
    }
    let mapping = groups
        .groups
        .iter()
        .find(|owner| owner.store_id == store && owner.group_id == group.group_id())
        .ok_or_else(|| invalid("replica ownership is not available for recovery"))?;
    if !mapping
        .members
        .iter()
        .any(|member| member.replica_id == group.local_replica().id && member.node_id == accepted.node_id)
    {
        return Err(invalid("local recovery replica identity differs"));
    }
    if store == 0 && group.group_id() == 0 {
        let owner: BootstrapOwner = read(&config_root.join("system-bootstrap.json"))?;
        if owner.identity != accepted.bootstrap || owner.replica_id != group.local_replica().id {
            return Err(invalid("system recovery owner differs"));
        }
    } else {
        match read::<GroupCreationStatus>(
            &config_root.join(format!("group-creation-{store}-{}.json", group.group_id())),
        ) {
            Ok(owner)
                if !owner.retired
                    && owner.deployment == mapping.deployment
                    && owner.replica_id == group.local_replica().id => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound && mapping.deployment.is_none() => {}
            _ => return Err(invalid("group recovery owner differs")),
        }
    }
    let endpoints: MemberEndpoints = match read(&source.root.join("member-endpoints.json")) {
        Ok(endpoints) => endpoints,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error),
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_millis();
    if endpoints.version != 1
        || endpoints.bootstrap != accepted.bootstrap
        || endpoints.observer_boot_id != source.boot_id
        || now.saturating_sub(u128::from(endpoints.observed_at_unix_ms)) > 15000
        || u128::from(endpoints.observed_at_unix_ms) > now + 1000
        || !endpoints
            .members
            .iter()
            .any(|member| member.node_id == accepted.node_id && member.boot_id == source.boot_id)
    {
        return Ok(BTreeMap::new());
    }
    let mut routes = BTreeMap::new();
    for (replica, endpoint, _) in group.remote_replica_info() {
        let Some(owner) = mapping.members.iter().find(|member| member.replica_id == replica) else {
            continue;
        };
        let Some(member) = endpoints.members.iter().find(|member| {
            member.node_id == owner.node_id && !endpoints.conflicts.contains(&member.discovery_id)
        }) else {
            continue;
        };
        let port = endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .parse::<SocketAddr>()
            .map_err(|_| invalid("invalid original replica endpoint"))?
            .port();
        let Some(address) = member
            .addresses
            .iter()
            .find(|address| address.is_ipv4())
            .or_else(|| member.addresses.first())
        else {
            continue;
        };
        if address.is_unspecified() || address.is_multicast() {
            return Err(invalid("invalid verified address"));
        }
        routes.insert(replica, SocketAddr::new(*address, port));
    }
    Ok(routes)
}

pub struct Worker(tokio::task::JoinHandle<()>);
impl Drop for Worker {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[must_use]
pub fn start(registry: Arc<PaxosKvStoreRegistry>) -> Worker {
    Worker(tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            for (store_id, store) in registry.stores_snapshot().iter() {
                for group_id in store.group_ids() {
                    if let Some(group) = store.get_group(group_id) {
                        if let Err(error) = apply(&group, &registry.config.config_root, *store_id) {
                            tracing::debug!(store_id, group_id, %error, "member connection routes remain blocked");
                        }
                    }
                }
            }
        }
    }))
}

fn read<T: serde::de::DeserializeOwned>(path: &Path) -> io::Result<T> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o777 != 0o600 || metadata.len() > 1024 * 1024 {
        return Err(invalid("invalid private recovery record"));
    }
    let mut bytes = Vec::new();
    File::open(path)?.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
    serde_json::from_slice(&bytes).map_err(io::Error::other)
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
