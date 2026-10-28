// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/default_preparation.rs"]
mod common;
use common::TestPreparationAuthority;
use crowdb_console_shared::{deployment::preparation, ops::cluster::readiness::collect};
use crowdb_protocol::mgmt::{
    cluster::DependencyState,
    node::{deployment::NodeDeploymentPolicy, observation::NodeRuntimeObservation, NodeBinding},
};

fn local(authority: &TestPreparationAuthority) -> NodeRuntimeObservation {
    NodeRuntimeObservation {
        version: 1,
        discovery_id: uuid::Uuid::new_v4().to_string(),
        policy: NodeDeploymentPolicy::default(),
        binding: Some(NodeBinding {
            node_id: 2,
            bootstrap: authority.bootstrap.identity.clone(),
            management_seeds: Vec::new(),
        }),
        setup: None,
        observed_at_unix_ms: 0,
        services: Vec::new(),
        protocols: Vec::new(),
    }
}

async fn put(authority: &TestPreparationAuthority, path: &str, value: &impl serde::Serialize) {
    authority
        .ctx
        .kv()
        .put(0, 0, path.as_bytes(), &serde_json::to_vec(value).unwrap(), None)
        .await
        .unwrap();
}

async fn storage_layer(authority: &TestPreparationAuthority, suffix: &str, code: Option<&str>) -> String {
    let report = collect(&authority.ctx, local(authority)).await.unwrap();
    let id = format!("storage/4/2/1{suffix}");
    let fact = report
        .observation
        .dependencies
        .iter()
        .find(|fact| fact.id == id)
        .unwrap();
    assert!(
        !fact.detail.is_empty(),
        "Every storage result needs concrete evidence"
    );
    assert_eq!(
        fact.reason.as_ref().map(|reason| reason.code.as_str()),
        code,
        "{fact:?}"
    );
    assert_eq!(
        fact.state,
        if code.is_none() {
            DependencyState::Ready
        } else if code.is_some_and(|code| code.starts_with("storage-waiting-for-")) {
            DependencyState::Waiting
        } else {
            DependencyState::Unknown
        }
    );
    if let Some(reason) = &fact.reason {
        assert_eq!(reason.detail, fact.detail);
    }
    fact.detail.clone()
}

async fn ready_disks(
    authority: &TestPreparationAuthority,
) -> (crowdb_protocol::diskdb::rpc::DiskValue, String) {
    use crowdb_protocol::{
        common::{BindMapValue, DiskId, HwStatus},
        diskdb::rpc::{DiskGroupValue, DiskValue},
        key::{DiskGroupKey, DiskKey, TextKey},
    };
    preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap();
    storage_layer(authority, "", Some("storage-group-missing")).await;
    storage_layer(authority, "/owner", Some("storage-waiting-for-disks")).await;
    storage_layer(authority, "/space", Some("storage-waiting-for-owner")).await;
    let disk_id = DiskId { high: 17, low: 23 };
    let group_path = DiskGroupKey {
        rack_id: 4,
        node_id: 2,
        disk_group_id: 1,
    }
    .to_path();
    let disk_path = DiskKey {
        rack_id: 4,
        node_id: 2,
        disk_group_id: 1,
        disk_id,
    }
    .to_path();
    let mut group = DiskGroupValue {
        status: HwStatus::Init as i32,
        disk_ids: vec![disk_id],
        ..Default::default()
    };
    put(authority, &group_path, &group).await;
    storage_layer(authority, "", Some("storage-group-not-up")).await;
    group.status = HwStatus::Up as i32;
    put(authority, &group_path, &group).await;
    storage_layer(authority, "", Some("storage-disk-missing")).await;
    let mut disk = DiskValue {
        status: HwStatus::Init as i32,
        capacity_units: 1024,
        ..Default::default()
    };
    put(authority, &disk_path, &disk).await;
    storage_layer(authority, "", Some("storage-disk-not-up")).await;
    disk.status = HwStatus::Up as i32;
    put(authority, &disk_path, &disk).await;
    storage_layer(authority, "", Some("storage-metadata-binding-missing")).await;
    put(
        authority,
        "/hw/dg_bind/4/2/1",
        &BindMapValue {
            store_id: 0,
            group_id: 1,
        },
    )
    .await;
    assert!(storage_layer(authority, "", None)
        .await
        .contains("all 1 member disks Up"));
    (disk, disk_path)
}

#[tokio::test]
async fn storage_diagnostics_follow_disk_owner_space_dependencies() {
    use crowdb_protocol::common::{DiskGroupUsageSummary, HwStatus, OwnerMapValue};
    let authority = TestPreparationAuthority::start(&[2]).await;
    let (mut disk, disk_path) = ready_disks(&authority).await;
    storage_layer(&authority, "/owner", Some("storage-owner-missing")).await;
    let mut owner = OwnerMapValue {
        instance_id: 42,
        lease_expiry_ms: 1,
        ..Default::default()
    };
    put(&authority, "/hw/dg_owner/4/2/1", &owner).await;
    storage_layer(&authority, "/owner", Some("storage-owner-lease-expired")).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    owner.lease_expiry_ms = u64::try_from(now).unwrap() + 120_000;
    put(&authority, "/hw/dg_owner/4/2/1", &owner).await;
    storage_layer(&authority, "/owner", Some("storage-owner-not-live")).await;
    let registry = crowdb_paxos_kv_client::ServiceRegistryClient::from_shared(authority.ctx.kv_arc().clone());
    registry
        .register_diskdb(42, "127.0.0.1:7200", &[], &[])
        .await
        .unwrap();
    storage_layer(&authority, "/owner", Some("storage-owner-group-not-loaded")).await;
    registry
        .register_diskdb(42, "127.0.0.1:7200", &[1], &[])
        .await
        .unwrap();
    assert!(storage_layer(&authority, "/owner", None)
        .await
        .contains("instance 42"));
    storage_layer(&authority, "/space", Some("storage-space-missing")).await;
    let mut usage = DiskGroupUsageSummary {
        disk_group_id: 1,
        ..Default::default()
    };
    registry
        .heartbeat_diskdb(42, "127.0.0.1:7200", &[1], std::slice::from_ref(&usage))
        .await
        .unwrap();
    storage_layer(&authority, "/space", Some("storage-space-unknown")).await;
    usage.usage_known = true;
    storage_layer(&authority, "/owner", None).await;
    registry
        .heartbeat_diskdb(42, "127.0.0.1:7200", &[1], std::slice::from_ref(&usage))
        .await
        .unwrap();
    storage_layer(&authority, "/space", Some("storage-space-not-current")).await;
    usage.sampled_at_ms = u64::try_from(now).unwrap();
    registry
        .heartbeat_diskdb(42, "127.0.0.1:7200", &[1], std::slice::from_ref(&usage))
        .await
        .unwrap();
    storage_layer(&authority, "/space", Some("storage-no-allocatable-disks")).await;
    usage.allocatable_disk_count = 1;
    registry
        .heartbeat_diskdb(42, "127.0.0.1:7200", &[1], std::slice::from_ref(&usage))
        .await
        .unwrap();
    storage_layer(&authority, "/space", Some("storage-space-exhausted")).await;
    usage.allocatable_free_bytes = 4096;
    usage.allocatable_capacity_bytes = 8192;
    registry
        .heartbeat_diskdb(42, "127.0.0.1:7200", &[1], std::slice::from_ref(&usage))
        .await
        .unwrap();
    assert!(storage_layer(&authority, "/space", None)
        .await
        .contains("4096 free bytes"));
    disk.status = HwStatus::Offline as i32;
    put(&authority, &disk_path, &disk).await;
    storage_layer(&authority, "/space", Some("storage-waiting-for-owner")).await;
}

#[tokio::test]
async fn space_must_come_from_the_current_owner_instead_of_a_detached_summary() {
    use crowdb_protocol::common::{DiskGroupUsageSummary, OwnerMapValue};
    let authority = TestPreparationAuthority::start(&[2]).await;
    ready_disks(&authority).await;
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    put(
        &authority,
        "/hw/dg_owner/4/2/1",
        &OwnerMapValue {
            instance_id: 42,
            lease_expiry_ms: now + 120_000,
            ..Default::default()
        },
    )
    .await;
    let registry = crowdb_paxos_kv_client::ServiceRegistryClient::from_shared(authority.ctx.kv_arc().clone());
    registry
        .register_diskdb(42, "127.0.0.1:7200", &[1], &[])
        .await
        .unwrap();
    let usage = DiskGroupUsageSummary {
        disk_group_id: 1,
        usage_known: true,
        sampled_at_ms: now,
        allocatable_disk_count: 1,
        allocatable_free_bytes: 4096,
        ..Default::default()
    };
    registry
        .register_diskdb(43, "127.0.0.1:7201", &[1], std::slice::from_ref(&usage))
        .await
        .unwrap();
    put(&authority, "/hw/dg_usage/1", &usage).await;
    storage_layer(&authority, "/owner", None).await;
    storage_layer(&authority, "/space", Some("storage-space-missing")).await;
    registry
        .heartbeat_diskdb(42, "127.0.0.1:7200", &[1], &[usage])
        .await
        .unwrap();
    storage_layer(&authority, "/space", None).await;
}

#[tokio::test]
async fn diskless_member_does_not_require_an_unused_storage_reservation() {
    use crowdb_console_shared::deployment::registry::{self, NodeRecord, NodeRegistry};
    use crowdb_protocol::mgmt::{cluster::DependencyKind, node::setup::SetupStorage};
    let authority = TestPreparationAuthority::start(&[2]).await;
    preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap();
    let mut node = local(&authority);
    node.policy.setup.storage = SetupStorage::Diskless;
    node.observed_at_unix_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    registry::publish(
        authority.ctx.kv(),
        &NodeRegistry {
            cluster_id: authority.bootstrap.identity.cluster_id.clone(),
            nodes: vec![NodeRecord {
                discovery_id: node.discovery_id.clone(),
                node_id: 2,
                rack_id: 4,
                physical_host_id: "owned".into(),
                host: "node-2".into(),
                ssh_port: 2222,
                ssh_user: "crowdb".into(),
                operation_id: uuid::Uuid::new_v4().to_string(),
                confirmed: true,
                host_key: None,
                cancelled: false,
            }],
        },
    )
    .await
    .unwrap();
    let report = collect(&authority.ctx, node).await.unwrap();
    assert!(report
        .observation
        .dependencies
        .iter()
        .any(|fact| fact.id == "node/2" && fact.state == DependencyState::Ready));
    assert!(
        !report
            .observation
            .dependencies
            .iter()
            .any(|fact| fact.kind == DependencyKind::Storage),
        "Diskless is an accepted choice, not missing disk evidence: {:?}",
        report.observation.dependencies
    );
}
