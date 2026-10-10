// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Recover processes from local launch inputs, then reload confirmed Group 0 topology.

use std::path::Path;

use crowdb_console_shared::clients::http::ServerClient;
use crowdb_console_shared::config::{
    GroupEntry, NodeEntry, ReplicaEntry, ServerEntry, ServiceType, StoreEntry,
};
use crowdb_console_shared::error::{Error, Result};
use crowdb_console_shared::lifecycle::{self, DeployRequest};
use crowdb_console_shared::ops::hardware;

use crate::state::AppState;

fn owns_process(pid: u32, workspace: &Path) -> bool {
    lifecycle::process_is_alive(pid)
        && std::fs::read_link(format!("/proc/{pid}/cwd")).is_ok_and(|cwd| cwd == workspace)
}

impl AppState {
    /// Recover standalone services and read initialized topology from Group 0.
    ///
    /// # Errors
    /// Reports failed process recovery or unavailable initialized Group 0 authority.
    pub async fn recover_standalone(&self) -> Result<()> {
        if self.config_path.is_none() {
            return Ok(());
        }
        let config = self
            .config
            .read()
            .map_err(|error| Error::Config(error.to_string()))?
            .clone();
        let seeds: Vec<String> = config
            .servers
            .iter()
            .filter(|server| server.service_type == ServiceType::PaxosKv)
            .map(|server| server.url.clone())
            .collect();
        for server in config
            .servers
            .iter()
            .filter(|server| server.service_type == ServiceType::PaxosKv)
        {
            let Some(node_id) = server.node_id else { continue };
            let node = config
                .node(node_id)
                .ok_or_else(|| Error::Config(format!("server {} has no node", server.id)))?;
            self.recover_kv(node, server, &seeds).await?;
        }
        self.reseed_kv_client().await;
        let intent_path = self.runtime_root.join("bootstrap-intent.toml");
        if intent_path.is_file() {
            let intent = crowdb_console_shared::bootstrap_intent::BootstrapIntent::load(&intent_path)?;
            let ctx = self.op_context().await?;
            crowdb_console_shared::ops::cluster::init_with_intent(&ctx, intent.members(), &intent_path)
                .await?;
            self.commit_op_context(&ctx)?;
        }
        let initialized = config.group(0, 0).is_some();
        let mut found_group0 = initialized;
        for server in config
            .servers
            .iter()
            .filter(|server| server.service_type == ServiceType::PaxosKv)
        {
            if ServerClient::new(server.url.clone())?
                .list_stores()
                .await
                .is_ok_and(|stores| stores.iter().any(|store| store.store_id == 0))
            {
                found_group0 = true;
            }
        }
        if found_group0 {
            self.confirm_kv_registrations().await?;
            self.reload_group0().await?;
        }
        for server in config
            .servers
            .iter()
            .filter(|server| server.service_type != ServiceType::PaxosKv)
        {
            self.recover_auxiliary(server).await?;
        }
        self.persist()?;
        tracing::info!(
            servers = config.servers.len(),
            group0 = found_group0,
            "standalone console recovered"
        );
        Ok(())
    }

    async fn recover_kv(&self, node: &NodeEntry, server: &ServerEntry, seeds: &[String]) -> Result<()> {
        let client = ServerClient::new(server.url.clone())?;
        let workspace = self.node_workspace_dir(node.id);
        if client.health().await.is_ok() {
            if let Some(pid) = server
                .pid
                .filter(|pid| node.ssh_enabled() || owns_process(*pid, &workspace))
            {
                self.set_runtime_pid(node.id, pid);
            }
            crate::mgmt::refresh_node_cache(self, node.id).await;
            return Ok(());
        }
        if !server.auto_start {
            return Ok(());
        }
        let request = DeployRequest {
            server_id: server.id.clone(),
            rest_port: server
                .rest_port
                .ok_or_else(|| Error::Config(format!("server {} missing rest_port", server.id)))?,
            rpc_port: server
                .rpc_port
                .ok_or_else(|| Error::Config(format!("server {} missing rpc_port", server.id)))?,
            binary: server.binary.as_ref().map(std::path::PathBuf::from),
            election_profile: server.election_profile.clone(),
            rpc_workers: server.rpc_workers,
            no_fsync: server.no_fsync,
            group0_management_seeds: seeds.to_vec(),
            ..Default::default()
        };
        let deployed = if node.ssh_enabled() {
            let binary = server.binary.as_deref().unwrap_or("crowdb-kv-server");
            crowdb_console_shared::ssh::deploy_via_ssh(&request, node, binary).await?
        } else {
            let workspace = self.prepare_node_workspace(node.id)?;
            lifecycle::deploy_local_in_dir(&request, node, &workspace).await?
        };
        self.set_runtime_pid(node.id, deployed.pid);
        {
            let mut config = self
                .config
                .write()
                .map_err(|error| Error::Config(error.to_string()))?;
            if let Some(entry) = config.servers.iter_mut().find(|entry| entry.id == server.id) {
                entry.pid = Some(deployed.pid);
            }
        }
        self.persist()?;
        crate::mgmt::refresh_node_cache(self, node.id).await;
        Ok(())
    }

    async fn recover_auxiliary(&self, server: &ServerEntry) -> Result<()> {
        let spec = self
            .config
            .read()
            .map_err(|error| Error::Config(error.to_string()))?
            .local_launches
            .get(&server.id)
            .cloned();
        let Some(spec) = spec else {
            if server.auto_start {
                return Err(Error::Config(format!(
                    "server {} missing local launch inputs",
                    server.id
                )));
            }
            return Ok(());
        };
        let alive = server
            .pid
            .filter(|pid| owns_process(*pid, Path::new(&spec.workdir)));
        let pid = if let Some(pid) = alive {
            pid
        } else if server.auto_start {
            lifecycle::restart_local_service(&server.id, 0, &spec).await?
        } else {
            return Ok(());
        };
        if server.service_type == ServiceType::Diskdb {
            if let Some(node_id) = server.node_id {
                self.set_diskdb_runtime_pid(node_id, pid);
            }
        }
        let mut config = self
            .config
            .write()
            .map_err(|error| Error::Config(error.to_string()))?;
        if let Some(entry) = config.servers.iter_mut().find(|entry| entry.id == server.id) {
            entry.pid = Some(pid);
        }
        Ok(())
    }

    pub(crate) async fn reload_group0(&self) -> Result<()> {
        let ctx = self.op_context().await?;
        let racks = hardware::list_racks_from_group0(&ctx).await?;
        let mut nodes = hardware::list_nodes_from_group0(&ctx, None).await?;
        // Private SSH material remains local; Group 0 owns connection identity.
        let previous = ctx.config().nodes.clone();
        for node in &mut nodes {
            if let Some(local) = previous
                .iter()
                .find(|local| local.id == node.id && local.ssh_credential_ref == node.ssh_credential_ref)
            {
                node.ssh_key.clone_from(&local.ssh_key);
                node.ssh_password.clone_from(&local.ssh_password);
            }
        }
        let sysmd = ctx.sysmd();
        let mut stores = Vec::new();
        let mut groups = Vec::new();
        for store in sysmd.list_stores().await? {
            for group in sysmd.list_groups_in_store(store.store_id).await? {
                let replicas = sysmd
                    .list_replicas_in_group(store.store_id, group.group_id)
                    .await?
                    .into_iter()
                    .map(|replica| ReplicaEntry {
                        replica_id: replica.replica_id,
                        node_id: replica.node_id,
                    })
                    .collect();
                groups.push(GroupEntry {
                    store_id: store.store_id,
                    group_id: group.group_id,
                    replicas,
                });
            }
            stores.push(StoreEntry {
                store_id: store.store_id,
                nodes: store.node_ids,
            });
        }
        let mut disk_groups = Vec::new();
        let mut disks = Vec::new();
        for node in &nodes {
            for group in hardware::list_disk_groups_from_group0(&ctx, node.id).await? {
                disks.extend(hardware::list_disks_from_group0(&ctx, node.id, group.id).await?);
                disk_groups.push(group);
            }
        }
        if self.node_monitor_url.is_some() {
            crate::services::remote::refresh(self).await?;
        }
        let mut config = self
            .config
            .write()
            .map_err(|error| Error::Config(error.to_string()))?;
        config.racks = racks;
        config.nodes = nodes;
        config.stores = stores;
        config.groups = groups;
        config.disk_groups = disk_groups;
        config.disks = disks;
        Ok(())
    }
}
