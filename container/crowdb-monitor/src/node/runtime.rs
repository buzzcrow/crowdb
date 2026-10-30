// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Node-first process ownership and authenticated private control.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use crowdb_protocol::mgmt::node::deployment::NodeDeploymentOverrides;
use crowdb_protocol::mgmt::node::{NodeBinding, NodeControl};
use crowdb_protocol::mgmt::SystemBootstrapIdentity;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;

use super::{durable, serve_node_management, trust, DiscoveryConfig, NodeIdentity};
use crate::{DeploymentProfile, ProcessManager, ServiceProfile};

mod cleanup;
mod control;
mod dispatch;
#[cfg(feature = "test-util")]
pub use dispatch::NodeDispatch as NodeDispatchForTests;
mod health;
#[cfg(feature = "test-util")]
pub use health::node_health_for_tests;
mod ipc;
mod kv;
mod observation;
#[cfg(feature = "test-util")]
pub use observation::runtime_observation_for_tests;
mod probes;
mod services;
use kv::start_kv;
mod admission;
mod credentials;
mod file_activation;
mod file_admission;
#[cfg(feature = "test-util")]
pub use file_admission::publish_file_for_tests;
mod file_preparation;
#[cfg(feature = "test-util")]
pub use file_preparation::resume_file_operations_for_tests;
mod initialization;
mod retained_storage;
mod setup;
mod storage_health;
mod supplied_storage;
#[cfg(feature = "test-util")]
pub use initialization::InitializationForTests;
#[cfg(feature = "test-util")]
pub use setup::NodeSetupForTests;
#[cfg(feature = "test-util")]
pub use supplied_storage::prepare_supplied_disk_for_tests;
mod preparation;
mod recovery;
pub use ipc::control_node;
use ipc::read_request;
#[cfg(feature = "test-util")]
pub use recovery::unique_member_endpoints_for_tests;
#[cfg(feature = "test-util")]
pub use recovery::{
    accept_member_trust_for_tests, check_member_trust_for_tests, member_trust_ready_for_tests,
};

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
    run_node_with_inputs(
        config,
        &NodeDeploymentOverrides::default(),
        &NodeDeploymentOverrides::default(),
    )
    .await
}

/// Start a node with explicit/file inputs sealed before node effects.
///
/// # Errors
/// Rejects incompatible policy, concurrent root use and runtime startup failures.
pub async fn run_node_with_inputs(
    config: NodeRuntimeConfig,
    file: &NodeDeploymentOverrides,
    explicit: &NodeDeploymentOverrides,
) -> Result<()> {
    if config.physical_host_id.is_empty() || config.physical_host_id.len() > 256 {
        return Err("node requires a bounded physical host identity".into());
    }
    DiscoveryConfig::validate_seed_origins(&config.discovery.seeds)?;
    let profile = kv::node_profile(&config)?;
    let root = profile.paths.data_root.clone();
    super::startup::accept_node_policy(&root, file, explicit)?;
    validate_retained_startup(&profile)?;
    // Reuse the existing socket ownership guard on the shared durable root,
    // independently of each container's private runtime directory.
    let _root_liveness = crate::LivenessServer::start(&root)?;
    NodeIdentity::load_or_create(&root)?;
    std::fs::create_dir_all(&profile.paths.run_root)?;
    std::fs::create_dir_all(&profile.paths.log_root)?;
    let socket = profile.paths.run_root.join("node-control.sock");
    let listener = ipc::listener(&socket).await?;
    let _liveness = crate::LivenessServer::start(&profile.paths.run_root)?;
    let mut health = health::NodeHealth::new(&profile)?;
    let mut processes = ProcessManager::new(profile.paths.log_root.clone(), profile.logs.clone()).await?;
    let web = web_service(&profile, config.bind.port())?;
    processes.start(&web, &BTreeMap::new()).await?;
    recovery::publish_local(&root, &config.discovery.addresses)?;
    let endpoint_recovery = recovery::start(root.clone(), config.bind.port());
    kv::resume(&profile, &mut processes).await?;
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
    let mut initialization = initialization::Initialization::default();
    initialization.resume(&profile)?;
    let mut file_allocator = file_preparation::FileAllocator::default();
    file_allocator.resume(&root)?;
    let mut supplied_storage = supplied_storage::SuppliedStorage::default();
    supplied_storage.resume(&root)?;
    let mut storage_health = storage_health::StorageHealth::default();
    let mut setup = setup::NodeSetup::default();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    let mut dispatch = dispatch::NodeDispatch::default();
    let result = async {
        loop {
            tokio::select! {
                biased;
                _ = terminate.recv() => break,
                _ = tokio::signal::ctrl_c() => break,
                outcome = &mut discovery => { outcome??; return Err("discovery unexpectedly stopped".into()); },
                work = dispatch.next(&listener, &mut heartbeat) => if let Some(mut stream) = work? {
                    let reply = match read_request(&mut stream).await {
                        Ok(request) => match control::execute(&profile, &mut processes, &mut initialization, &mut file_allocator, &mut supplied_storage, &mut setup, request).await {
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
                else {
                    file_allocator.tick(&root)?;
                    supplied_storage.tick(&root)?;
                    storage_health.tick(&root).await;
                    setup.tick(&profile).await?;
                    health.tick(&profile, &web, &mut processes).await?;
                }
            }
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }.await;
    drop(endpoint_recovery);
    initialization.stop().await;
    setup.stop().await;
    storage_health.stop().await;
    file_allocator.stop().await;
    supplied_storage.stop().await;
    let _ = stop.send(());
    if !discovery.is_finished() {
        discovery.await??;
    }
    stop_services(&profile, &mut processes).await?;
    std::fs::remove_file(socket)?;
    result
}

fn validate_retained_startup(profile: &DeploymentProfile) -> Result<()> {
    let root = &profile.paths.data_root;
    // KV-only admissions can precede credential provisioning. Once secrets or
    // an Access intent are retained, validate them before launching children.
    if root.join("secrets").try_exists()?
        || services::retained(profile)?
            .iter()
            .any(|intent| intent.kind == "access")
    {
        crate::ServerCredentials::load_existing(root)?;
    }
    if let Some(root) = std::env::var_os("CROWDB_CORE_DIR") {
        let crashes = crate::CrashRetention::open(root.into())?;
        std::env::set_current_dir(crashes.root())?;
    }
    Ok(())
}

async fn stop_services(profile: &DeploymentProfile, processes: &mut ProcessManager) -> Result<()> {
    let mut order = vec!["web".into(), "kv".into()];
    order.extend(
        services::retained(profile)?
            .into_iter()
            .map(|intent| intent.service_id),
    );
    processes.stop_all(&order, Duration::from_secs(10)).await?;
    Ok(())
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
    let web_port = profile
        .public_endpoints
        .iter()
        .find(|endpoint| endpoint.id == "web")
        .map(|endpoint| endpoint.port)
        .ok_or("web public endpoint is absent from node profile")?;
    web.args = vec![
        "--bind".into(),
        "0.0.0.0".into(),
        "--port".into(),
        web_port.to_string(),
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

fn disk_inventory(root: &std::path::Path) -> Result<Value> {
    let inputs = std::env::var("CROWDB_DISK_INPUTS").unwrap_or_else(|_| "[]".into());
    let inputs: Vec<crowdb_protocol::mgmt::node::storage::DiskInput> = serde_json::from_str(&inputs)?;
    Ok(serde_json::to_value(super::discover_disks(root, &inputs)?)?)
}

#[cfg(feature = "test-util")]
pub use services::NodeServiceForTests;

#[cfg(feature = "test-util")]
pub use recovery::rebind_recovery_services_for_tests;

pub use recovery::read_recovery_identity;

#[cfg(feature = "test-util")]
pub use cleanup::cleanup_node_for_tests;

#[cfg(feature = "test-util")]
pub use recovery::capture_recovery_owners_for_tests;
