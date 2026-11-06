// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded physical checks run outside the monitor's private control loop.

use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use crowdb_console_shared::deployment::registry;
use crowdb_paxos_kv_client::{ClientConfig, HardwareClient, PaxosKvClient};
use crowdb_protocol::{common::HwStatus, key::DiskKey, mgmt::node::NodeBinding};

use super::{durable, Result};

#[derive(Default)]
pub(super) struct StorageHealth {
    task: Option<tokio::task::JoinHandle<Result<Option<String>>>>,
    after: Option<String>,
    next_check: Option<Instant>,
}

impl StorageHealth {
    pub(super) async fn tick(&mut self, root: &Path) {
        if self
            .task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            match self.task.take().expect("finished health task").await {
                Ok(Ok(next)) => self.after = next,
                Ok(Err(error)) => eprintln!("disk health observation deferred: {error}"),
                Err(error) => eprintln!("disk health worker failed: {error}"),
            }
            self.next_check = Some(Instant::now() + Duration::from_secs(5));
        }
        if self.task.is_some()
            || self.next_check.is_some_and(|next| Instant::now() < next)
            || !root.join("node-binding.json").exists()
        {
            return;
        }
        let root = root.to_path_buf();
        let after = self.after.clone();
        self.task = Some(tokio::spawn(observe(root, after)));
    }

    pub(super) async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            match task.await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => eprintln!("disk health observation stopped: {error}"),
                Err(error) => eprintln!("disk health worker stopped: {error}"),
            }
        }
    }
}

async fn observe(root: PathBuf, after: Option<String>) -> Result<Option<String>> {
    let binding: NodeBinding =
        durable::read(&root.join("node-binding.json"))?.ok_or("disk health binding is absent")?;
    tokio::time::timeout(Duration::from_secs(3), super::verify_binding(&binding)).await??;
    let client = PaxosKvClient::new(ClientConfig::new(binding.management_seeds.clone()));
    let (registry, _) = registry::read(&client)
        .await?
        .ok_or("disk health registry is absent")?;
    let node = registry
        .nodes
        .iter()
        .find(|node| node.node_id == binding.node_id && node.confirmed && !node.cancelled)
        .ok_or("disk health admission is absent")?;
    if registry.cluster_id != binding.bootstrap.cluster_id {
        return Err("disk health registry belongs to another cluster".into());
    }
    let hardware = HardwareClient::new(client);
    let prefix = crowdb_protocol::disk_storage::DiskStorageRecord::node_prefix(node.rack_id, node.node_id);
    let after = after.as_deref().filter(|cursor| cursor.starts_with(&prefix));
    let (records, next) = hardware
        .disk_storage_page(node.rack_id, node.node_id, after, 8)
        .await?;
    let inputs = std::env::var("CROWDB_DISK_INPUTS").unwrap_or_else(|_| "[]".into());
    let inputs: Vec<crowdb_protocol::mgmt::node::storage::DiskInput> = serde_json::from_str(&inputs)?;
    for record in records.into_iter().filter(|record| !record.removed) {
        let key = DiskKey {
            rack_id: record.rack_id,
            node_id: record.node_id,
            disk_group_id: record.disk_group_id,
            disk_id: record.disk_id,
        };
        let Some((disk, revision)) = hardware.get_disk_with_revision(&key).await? else {
            continue;
        };
        if disk.status == HwStatus::Offline as i32 {
            continue;
        }
        let observation_root = root.clone();
        let observation_inputs = inputs.clone();
        let observation_record = record.clone();
        let observation = tokio::task::spawn_blocking(move || {
            super::super::storage::retained::check(
                &observation_root,
                &observation_inputs,
                &observation_record,
            )
        })
        .await?;
        if let Err(error) = observation {
            // A completed old check cannot close a later lifecycle generation.
            if durable::read::<NodeBinding>(&root.join("node-binding.json"))?.as_ref() != Some(&binding) {
                return Err("disk health binding changed during observation".into());
            }
            hardware.quarantine_disk_storage(&record, revision).await?;
            eprintln!("disk quarantined Offline after physical check: disk={:?} revision={revision} reason={error}; explicit validated recovery required", record.disk_id);
        }
    }
    Ok(next)
}
