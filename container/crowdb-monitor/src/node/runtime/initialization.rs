// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One monitor-owned bootstrap, independent of a control connection's lifetime.

mod peer_control;

use std::path::Path;

use crowdb_console_shared::{deployment::PreparedBootstrap, ops::OpContext};
use crowdb_protocol::mgmt::node::{NodeBinding, NodeControl, NodeServiceCredentials};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{durable, preparation, Result};
use crate::{DeploymentProfile, NodeIdentity, ServerCredentials};

#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct Request {
    operation: PreparedBootstrap,
    create_data_group: bool,
}

#[derive(Default)]
pub(super) struct Initialization {
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Initialization {
    pub(super) fn running(&self) -> bool {
        self.task.as_ref().is_some_and(|task| !task.is_finished())
    }

    pub(super) fn status(root: &Path) -> Result<Value> {
        Ok(durable::read(&root.join("initialization-status.json"))?
            .unwrap_or_else(|| json!({"phase": "idle"})))
    }

    pub(super) fn resume(&mut self, profile: &DeploymentProfile) -> Result<()> {
        if let Some(request) = durable::read::<Request>(&profile.paths.data_root.join("initialization.json"))?
        {
            let phase = Self::status(&profile.paths.data_root)?;
            if phase["phase"] != "active" && phase["phase"] != "failed" {
                self.start(profile, request)?;
            }
        }
        Ok(())
    }

    pub(super) fn accept(
        &mut self,
        profile: &DeploymentProfile,
        manifest: Value,
        credentials: NodeServiceCredentials,
        create_data_group: Option<bool>,
    ) -> Result<Value> {
        let root = &profile.paths.data_root;
        let operation: PreparedBootstrap = serde_json::from_value(manifest)?;
        operation.validate()?;
        let local = local_member(root, &operation)?;
        let path = root.join("initialization.json");
        let existing = durable::read::<Request>(&path)?;
        let request = Request {
            operation,
            create_data_group: create_data_group
                .unwrap_or_else(|| existing.as_ref().is_some_and(|request| request.create_data_group)),
        };
        if let Some(existing) = existing {
            if existing != request {
                return Err("initialization inputs differ from accepted intent".into());
            }
            if self.running() || Self::status(root)?["phase"] == "active" {
                return Self::status(root);
            }
        }
        preparation::accept(
            root,
            local,
            request.operation.identity.clone(),
            Some(serde_json::to_value(&request.operation)?),
            Some(credentials),
            None,
        )?;
        durable::write(&path, &request)?;
        self.start(profile, request)?;
        Self::status(root)
    }

    fn start(&mut self, profile: &DeploymentProfile, request: Request) -> Result<()> {
        request.operation.validate()?;
        let local = local_member(&profile.paths.data_root, &request.operation)?;
        progress(&profile.paths.data_root, &request, "preparing", None)?;
        let profile = profile.clone();
        self.task = Some(tokio::spawn(async move {
            if let Err(error) = execute(&profile, &request, local).await {
                let message = error.to_string();
                eprintln!("cluster initialization failed; explicit retry required: {message}");
                if let Err(error) = progress(&profile.paths.data_root, &request, "failed", Some(&message)) {
                    eprintln!("cannot persist initialization failure: {error}");
                }
            }
        }));
        Ok(())
    }

    pub(super) async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

fn local_member(root: &Path, operation: &PreparedBootstrap) -> Result<u64> {
    let identity = NodeIdentity::load_or_create(root)?.uuid().to_string();
    operation
        .nodes
        .iter()
        .find(|node| node.discovery_id == identity && operation.intent.members().contains(&node.node_id))
        .map(|node| node.node_id)
        .ok_or_else(|| "initialization must run on a selected member".into())
}

fn progress(root: &Path, request: &Request, phase: &str, error: Option<&str>) -> Result<()> {
    durable::write(
        &root.join("initialization-status.json"),
        &json!({
            "operation_id": request.operation.identity.operation_id,
            "phase": phase, "members": request.operation.intent.members(), "error": error,
        }),
    )?;
    Ok(())
}

async fn control(
    profile: &DeploymentProfile,
    operation: &PreparedBootstrap,
    local: u64,
    member: u64,
    request: NodeControl,
) -> Result<()> {
    if member == local {
        super::control_node(&profile.paths.run_root.join("node-control.sock"), &request).await?;
    } else {
        let node = operation
            .intent
            .to_config()
            .node(member)
            .cloned()
            .ok_or("missing bootstrap member")?;
        peer_control::execute(&profile.paths.data_root, &node, &request).await?;
    }
    Ok(())
}

async fn execute(profile: &DeploymentProfile, request: &Request, local: u64) -> Result<()> {
    let root = &profile.paths.data_root;
    let operation = &request.operation;
    let credentials = ServerCredentials::load_existing(root)?.server_env();
    let results = futures::future::join_all(operation.intent.members().iter().map(async |member| {
        control(
            profile,
            operation,
            local,
            *member,
            NodeControl::StartKv {
                node_id: *member,
                bootstrap: operation.identity.clone(),
                manifest: Some(serde_json::to_value(operation)?),
                credentials: Some(NodeServiceCredentials {
                    environment: credentials.clone(),
                }),
                admission: None,
            },
        )
        .await
    }))
    .await;
    for result in results {
        result?;
    }
    progress(root, request, "initializing", None)?;
    let config = operation.intent.to_config();
    let seeds: Vec<_> = config.servers.iter().map(|server| server.url.clone()).collect();
    let endpoint = config
        .servers
        .iter()
        .find_map(|server| server.rpc_url.clone())
        .ok_or("bootstrap RPC endpoint missing")?;
    let ctx = OpContext::new(endpoint, seeds.clone(), config);
    operation.execute(&ctx).await?;
    progress(root, request, "binding", None)?;
    let results = futures::future::join_all(operation.intent.members().iter().map(async |member| {
        control(
            profile,
            operation,
            local,
            *member,
            NodeControl::Bind {
                binding: NodeBinding {
                    node_id: *member,
                    bootstrap: operation.identity.clone(),
                    management_seeds: seeds.clone(),
                },
            },
        )
        .await
    }))
    .await;
    for result in results {
        result?;
    }
    if request.create_data_group {
        progress(root, request, "data-group", None)?;
        ensure_data_group(&ctx, operation.intent.members()).await?;
    }
    progress(root, request, "active", None)
}

async fn ensure_data_group(ctx: &OpContext, nodes: &[u64]) -> Result<()> {
    if ctx.sysmd().get_group(0, 1).await?.is_some() {
        let mut actual: Vec<_> = ctx
            .sysmd()
            .list_replicas_in_group(0, 1)
            .await?
            .iter()
            .map(|replica| replica.node_id)
            .collect();
        let mut expected = nodes.to_vec();
        actual.sort_unstable();
        expected.sort_unstable();
        if actual != expected {
            return Err("existing Group 1 membership differs from initialization nodes".into());
        }
        return Ok(());
    }
    crowdb_console_shared::ops::kv_logical::add_group(ctx, 0, 1, 1, nodes).await?;
    Ok(())
}

/// Access to the monitor-owned lifecycle without starting production services.
#[cfg(feature = "test-util")]
#[derive(Default)]
pub struct InitializationForTests(Initialization);

#[cfg(feature = "test-util")]
impl InitializationForTests {
    /// # Errors
    /// Propagates the real acceptance validation and persistence failures.
    pub fn accept_for_tests(
        &mut self,
        profile: &DeploymentProfile,
        manifest: Value,
        credentials: NodeServiceCredentials,
        create_data_group: Option<bool>,
    ) -> Result<Value> {
        self.0.accept(profile, manifest, credentials, create_data_group)
    }

    /// # Errors
    /// Propagates durable status read failures.
    pub fn status_for_tests(root: &Path) -> Result<Value> {
        Initialization::status(root)
    }

    /// # Errors
    /// Propagates durable resume validation failures.
    pub fn resume_for_tests(&mut self, profile: &DeploymentProfile) -> Result<()> {
        self.0.resume(profile)
    }

    pub async fn stop_for_tests(&mut self) {
        self.0.stop().await;
    }
}
