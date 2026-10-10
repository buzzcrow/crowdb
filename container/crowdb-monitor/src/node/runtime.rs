// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Node-first process ownership and authenticated private control.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use crowdb_protocol::mgmt::node::{NodeBinding, NodeControl};
use crowdb_protocol::mgmt::SystemBootstrapIdentity;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};

use super::{durable, serve_node_management, trust, DiscoveryConfig, NodeIdentity};
use crate::{DeploymentProfile, ProcessManager, ServiceProfile};

mod cleanup;
mod health;
mod ipc;
mod kv;
mod probes;
mod services;
use kv::start_kv;
mod admission;
mod credentials;
mod preparation;
pub use ipc::control_node;
use ipc::read_request;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Debug)]
pub struct NodeRuntimeConfig {
    pub profile: PathBuf,
    pub bind: std::net::SocketAddr,
    pub discovery: DiscoveryConfig,
    pub physical_host_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct AcceptedNode {
    pub(super) node_id: u64,
    pub(super) bootstrap: SystemBootstrapIdentity,
    #[serde(default)]
    pub(super) prepared: bool,
    #[serde(default)]
    pub(super) admission: Option<crowdb_protocol::mgmt::node::NodeAdmissionGrant>,
}

/// Start UI/discovery without creating disks or consensus resources.
///
/// # Errors
/// Rejects conflicting persistent state and failed monitor/service startup.
pub async fn run_node(config: NodeRuntimeConfig) -> Result<()> {
    let profile = kv::node_profile(&config)?;
    let root = profile.paths.data_root.clone();
    NodeIdentity::load_or_create(&root)?;
    std::fs::create_dir_all(&profile.paths.run_root)?;
    std::fs::create_dir_all(&profile.paths.log_root)?;
    let socket = profile.paths.run_root.join("node-control.sock");
    if socket.exists() {
        if UnixStream::connect(&socket).await.is_ok() {
            return Err("node monitor already running".into());
        }
        std::fs::remove_file(&socket)?;
    }
    let listener = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let _liveness = crate::LivenessServer::start(&profile.paths.run_root)?;
    let mut health = health::NodeHealth::new(&profile)?;
    let mut processes = ProcessManager::new(profile.paths.log_root.clone(), profile.logs.clone()).await?;
    let web = web_service(&profile, config.bind.port())?;
    processes.start(&web, &BTreeMap::new()).await?;
    if let Some(accepted) = durable::read::<AcceptedNode>(&root.join("accepted-node.json"))? {
        if accepted.prepared
            && !admission::is_cancelled(&root, &accepted)
            && !services::kv_paused(&profile)?
            && !root
                .join(format!(
                    "retired-bootstrap-{}.json",
                    accepted.bootstrap.operation_id
                ))
                .exists()
        {
            start_kv(&profile, &mut processes, &accepted).await?;
        }
    }
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let discovery_root = root.clone();
    let mut discovery = tokio::spawn(async move {
        serve_node_management(
            &discovery_root,
            config.bind,
            &config.discovery,
            config.physical_host_id,
            async {
                let _ = stopped.await;
            },
        )
        .await
    });
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    let result = async {
        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    health.tick(&profile, &web, &mut processes).await?;
                }
                connection = listener.accept() => {
                    let (mut stream, _) = connection?;
                    let reply = match read_request(&mut stream).await {
                        Ok(request) => match execute(&profile, &mut processes, request).await {
                            Ok(value) => json!({"ok": true, "value": value}),
                            Err(error) => json!({"ok": false, "error": error.to_string()}),
                        },
                        Err(error) => json!({"ok": false, "error": error.to_string()}),
                    };
                    if let Err(error) = stream.write_all(&serde_json::to_vec(&reply)?).await {
                        eprintln!("node control reply failed: {error}");
                    }
                    let _ = stream.shutdown().await;
                }
                _ = terminate.recv() => break,
                _ = tokio::signal::ctrl_c() => break,
                outcome = &mut discovery => { outcome??; return Err("discovery unexpectedly stopped".into()); },
            }
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }.await;
    let _ = stop.send(());
    if !discovery.is_finished() {
        discovery.await??;
    }
    let mut order = vec!["web".into(), "kv".into()];
    order.extend(
        services::retained(&profile)?
            .into_iter()
            .map(|intent| intent.service_id),
    );
    processes.stop_all(&order, Duration::from_secs(10)).await?;
    std::fs::remove_file(socket)?;
    result
}

async fn execute(
    profile: &DeploymentProfile,
    processes: &mut ProcessManager,
    request: NodeControl,
) -> Result<Value> {
    let root = &profile.paths.data_root;
    match request {
        NodeControl::ProvisionServiceCredentials {
            bootstrap,
            credentials,
        } => credentials::provision(root, &bootstrap, &credentials),
        NodeControl::ServiceCredentials { cluster_id } => credentials::read(root, &cluster_id).await,
        NodeControl::Service { intent } => services::execute(profile, processes, intent).await,
        NodeControl::CancelAdmission {
            bootstrap,
            node_id,
            admission,
        } => admission::cancel(profile, processes, bootstrap, node_id, admission).await,
        NodeControl::Identity => Ok(json!({"discovery_id": NodeIdentity::load_or_create(root)?.uuid(),
            "public_key": std::fs::read_to_string(root.join("ssh/id_ed25519.pub"))?,
            "host_key": std::fs::read_to_string(root.join("ssh/ssh_host_ed25519_key.pub"))?})),
        NodeControl::InstallKey {
            operation_id,
            public_key,
        } => {
            authorize_bound(root).await?;
            trust::install(root, &operation_id, &public_key)?;
            Ok(json!({}))
        }
        NodeControl::RemoveKey { operation_id } => {
            trust::remove(root, &operation_id)?;
            Ok(json!({}))
        }
        NodeControl::TrustHost {
            host,
            port,
            public_key,
        } => {
            authorize_bound(root).await?;
            trust::host(root, &host, port, &public_key)?;
            Ok(json!({}))
        }
        NodeControl::StartKv {
            node_id,
            bootstrap,
            manifest,
            credentials,
            admission,
        } => {
            if let Some(grant) = &admission {
                admission::validate(root, &bootstrap, node_id, grant, false).await?;
            }
            authorize_bound(root).await?;
            if root.join("node-binding.json").exists()
                && services::desired(profile).await?.iter().any(|intent| {
                    intent.kind == "kv"
                        && matches!(
                            intent.action,
                            crowdb_protocol::mgmt::node::NodeServiceAction::Stop
                                | crowdb_protocol::mgmt::node::NodeServiceAction::Delete
                        )
                })
            {
                return Err("KV is stopped by the current cluster intent".into());
            }
            let accepted = preparation::accept(root, node_id, bootstrap, manifest, credentials, admission)?;
            start_kv(profile, processes, &accepted).await?;
            Ok(json!({"node_id": node_id, "management_port": 10000, "rpc_port": 10100}))
        }
        NodeControl::Bind { binding } => kv::bind(profile, processes, binding).await,
        NodeControl::Cleanup {
            bootstrap,
            confirm_delete_system_store,
        } => cleanup::execute(profile, processes, bootstrap, confirm_delete_system_store).await,
    }
}

async fn authorize_bound(root: &std::path::Path) -> Result<()> {
    if let Some(binding) = durable::read::<NodeBinding>(&root.join("node-binding.json"))? {
        tokio::time::timeout(Duration::from_secs(3), verify_binding(&binding)).await??;
    }
    Ok(())
}

async fn verify_binding(binding: &NodeBinding) -> Result<()> {
    if binding.management_seeds.is_empty() || binding.management_seeds.len() > 16 {
        return Err("invalid authority seeds".into());
    }
    let client = crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(
        binding.management_seeds.clone(),
    ));
    let found = client
        .get(
            0,
            0,
            b"/deployment/cluster",
            crowdb_kv_client::ReadMode::Linearizable,
            None,
        )
        .await?;
    let crowdb_kv_client::GetOutcome::Found { value, .. } = found else {
        return Err("cluster publication not complete".into());
    };
    let publication: Value = serde_json::from_slice(&value)?;
    if publication["identity"] != serde_json::to_value(&binding.bootstrap)? {
        return Err("cluster authority identity differs".into());
    }
    let registry = client
        .get(
            0,
            0,
            b"/deployment/nodes",
            crowdb_kv_client::ReadMode::Linearizable,
            None,
        )
        .await?;
    let crowdb_kv_client::GetOutcome::Found { value, .. } = registry else {
        return Err("node registry publication incomplete".into());
    };
    let registry: Value = serde_json::from_slice(&value)?;
    if registry["cluster_id"] != binding.bootstrap.cluster_id
        || !registry["nodes"].as_array().is_some_and(|nodes| {
            nodes.iter().any(|node| {
                node["node_id"] == binding.node_id && node["confirmed"] == true && node["cancelled"] != true
            })
        })
    {
        return Err("node admission is not confirmed".into());
    }
    Ok(())
}

fn service(profile: &DeploymentProfile, id: &str) -> Result<ServiceProfile> {
    profile
        .services
        .iter()
        .find(|service| service.id == id)
        .cloned()
        .ok_or_else(|| format!("missing {id} service").into())
}

fn web_service(profile: &DeploymentProfile, monitor_port: u16) -> Result<ServiceProfile> {
    let root = &profile.paths.data_root;
    let mut web = service(profile, "web")?;
    web.args = vec![
        "--bind".into(),
        "0.0.0.0".into(),
        "--port".into(),
        "9090".into(),
        "--runtime-dir".into(),
        root.join("console").to_string_lossy().into_owned(),
        "--node-monitor".into(),
        format!("http://127.0.0.1:{}", monitor_port),
        "--skip-startup-restore".into(),
        "--ui-root".into(),
        profile
            .paths
            .install_root
            .join("ui")
            .to_string_lossy()
            .into_owned(),
        "--log-dir".into(),
        profile.paths.log_root.join("web").to_string_lossy().into_owned(),
    ];
    web.env.insert(
        "CROWDB_KV_KNOWN_HOSTS".into(),
        root.join("ssh/console-known_hosts")
            .to_string_lossy()
            .into_owned(),
    );
    Ok(web)
}
