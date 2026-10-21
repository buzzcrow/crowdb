// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authenticated private control dispatch; long storage work stays in workers.

use super::{
    admission, authorize_bound, cleanup, credentials, disk_inventory, file_preparation, initialization, kv,
    observation, preparation, retained_storage, services, setup, start_kv, supplied_storage, trust,
    DeploymentProfile, NodeControl, NodeIdentity, ProcessManager, Result, Value,
};
use serde_json::json;

pub(super) async fn execute(
    profile: &DeploymentProfile,
    processes: &mut ProcessManager,
    initialization: &mut initialization::Initialization,
    file_allocator: &mut file_preparation::FileAllocator,
    supplied_storage: &mut supplied_storage::SuppliedStorage,
    setup: &mut setup::NodeSetup,
    request: NodeControl,
) -> Result<Value> {
    let root = &profile.paths.data_root;
    match request {
        NodeControl::DeploymentPolicy => Ok(serde_json::to_value(crate::node::read_node_policy(root)?)?),
        NodeControl::AcceptNodeSetup { request } => Ok(serde_json::to_value(setup.accept(root, request)?)?),
        NodeControl::NodeSetup => Ok(serde_json::to_value(setup::NodeSetup::status(root)?)?),
        NodeControl::NodeSetupIntent => Ok(serde_json::to_value(setup::NodeSetup::intent(root)?)?),
        NodeControl::CheckRetainedDisk { request } => Ok(serde_json::to_value(
            retained_storage::check(root, request).await?,
        )?),
        request @ (NodeControl::PrepareDiskFile { .. }
        | NodeControl::PrepareSuppliedDisk { .. }
        | NodeControl::SuppliedDiskPreparation { .. }
        | NodeControl::SuppliedDiskPreparations { .. }
        | NodeControl::DiskFilePreparation { .. }
        | NodeControl::DiskFilePreparations { .. }) => {
            storage(root, file_allocator, supplied_storage, request).await
        }
        NodeControl::Initialize {
            manifest,
            credentials,
            create_data_group,
        } => {
            authorize_bound(root).await?;
            initialization.accept(profile, manifest, credentials, create_data_group)
        }
        NodeControl::Initialization => initialization::Initialization::status(root),
        NodeControl::DiskInventory => disk_inventory(root),
        NodeControl::Processes { cluster_id, node_id } => {
            observation::processes(root, processes, &cluster_id, node_id)
        }
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
        NodeControl::Identity => identity(root),
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
            kv::ensure_start_allowed(profile).await?;
            let accepted = preparation::accept(root, node_id, bootstrap, manifest, credentials, admission)?;
            start_kv(profile, processes, &accepted).await?;
            Ok(json!({"node_id": node_id, "management_port": 10000, "rpc_port": 10100}))
        }
        NodeControl::Bind { binding } => kv::bind(profile, processes, binding).await,
        NodeControl::Cleanup {
            bootstrap,
            confirm_delete_system_store,
        } => {
            supplied_storage::SuppliedStorage::retire(root, &bootstrap, confirm_delete_system_store)?;
            if initialization.running() {
                return Err("initialization is still running".into());
            }
            if file_allocator.running() {
                return Err("disk file preparation is still running".into());
            }
            if supplied_storage.running() {
                return Err("supplied disk provisioning is still running".into());
            }
            setup.prepare_cleanup(root, &bootstrap, confirm_delete_system_store)?;
            cleanup::execute(profile, processes, bootstrap, confirm_delete_system_store).await
        }
    }
}

async fn storage(
    root: &std::path::Path,
    file_allocator: &mut file_preparation::FileAllocator,
    supplied_storage: &mut supplied_storage::SuppliedStorage,
    request: NodeControl,
) -> Result<Value> {
    match request {
        NodeControl::PrepareDiskFile { request } => {
            Ok(serde_json::to_value(file_allocator.accept(root, request).await?)?)
        }
        NodeControl::PrepareSuppliedDisk { request } => Ok(serde_json::to_value(
            supplied_storage.accept(root, request).await?,
        )?),
        NodeControl::SuppliedDiskPreparation { operation_id } => Ok(serde_json::to_value(
            crate::node::storage::supplied::lookup(root, &operation_id)?,
        )?),
        NodeControl::SuppliedDiskPreparations { after, limit } => Ok(serde_json::to_value(
            crate::node::storage::supplied::list(root, after.as_deref(), limit)?,
        )?),
        NodeControl::DiskFilePreparation { operation_id } => Ok(serde_json::to_value(
            crate::node::storage::file_preparation::status(root, &operation_id)?,
        )?),
        NodeControl::DiskFilePreparations { after, limit } => Ok(serde_json::to_value(
            crate::node::storage::file_operations::list(root, after.as_deref(), limit)?,
        )?),
        _ => Err("unsupported storage control request".into()),
    }
}

fn identity(root: &std::path::Path) -> Result<Value> {
    Ok(json!({"discovery_id": NodeIdentity::load_or_create(root)?.uuid(),
        "public_key": std::fs::read_to_string(root.join("ssh/id_ed25519.pub"))?,
        "host_key": std::fs::read_to_string(root.join("ssh/ssh_host_ed25519_key.pub"))?}))
}
