// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/default_preparation.rs"]
mod common;
use common::TestPreparationAuthority;
use crowdb_console_shared::{
    deployment::{
        preparation,
        registry::{self, NodeRecord, NodeRegistry},
    },
    ops::cluster::readiness::collect,
};
use crowdb_protocol::{
    common::{BindMapValue, DiskGroupUsageSummary, DiskId, HwStatus, OwnerMapValue},
    diskdb::rpc::{DiskGroupValue, DiskValue},
    key::{DiskGroupKey, DiskKey, TextKey},
    mgmt::{
        cluster::{DependencyKind, DependencyState},
        node::{
            deployment::NodeDeploymentPolicy,
            observation::NodeRuntimeObservation,
            setup::{
                NodeSetupPhase, NodeSetupStatus, SetupDiskStatus, SetupMedium, SetupStepPhase, SetupStorage,
                SetupStorageMode,
            },
            storage::DiskBackend,
            NodeBinding,
        },
    },
    DiskIdExt,
};

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

async fn put(authority: &TestPreparationAuthority, path: &str, value: &impl serde::Serialize) {
    authority
        .ctx
        .kv()
        .put(0, 0, path.as_bytes(), &serde_json::to_vec(value).unwrap(), None)
        .await
        .unwrap();
}

async fn setup() -> (TestPreparationAuthority, NodeRuntimeObservation) {
    let authority = TestPreparationAuthority::start(&[2]).await;
    preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap();
    let discovery_id = uuid::Uuid::new_v4().to_string();
    let operation_id = uuid::Uuid::new_v4().to_string();
    registry::publish(
        authority.ctx.kv(),
        &NodeRegistry {
            cluster_id: authority.bootstrap.identity.cluster_id.clone(),
            nodes: vec![NodeRecord {
                discovery_id: discovery_id.clone(),
                node_id: 2,
                physical_host_id: "owned-host".into(),
                rack_id: 4,
                host: "node-2".into(),
                ssh_port: 2222,
                ssh_user: "crowdb".into(),
                operation_id: operation_id.clone(),
                confirmed: true,
                host_key: None,
                cancelled: false,
            }],
        },
    )
    .await
    .unwrap();
    let mut policy = NodeDeploymentPolicy::default();
    let mut statuses = Vec::new();
    let mut media = Vec::new();
    let mut usage = Vec::new();
    for group in [7, 9] {
        let (medium, status, summary) = test_medium(&authority, group).await;
        media.push(medium);
        statuses.push(status);
        usage.push(summary);
    }
    crowdb_kv_client::ServiceRegistryClient::from_shared(authority.ctx.kv_arc().clone())
        .register_diskdb(42, "127.0.0.1:7200", &[7, 9], &usage)
        .await
        .unwrap();
    policy.setup.storage = SetupStorage::Supplied { disks: media };
    policy.validate().unwrap();
    let node = NodeRuntimeObservation {
        version: 1,
        discovery_id: discovery_id.clone(),
        policy,
        binding: Some(NodeBinding {
            node_id: 2,
            bootstrap: authority.bootstrap.identity.clone(),
            management_seeds: vec![],
        }),
        setup: Some(NodeSetupStatus {
            version: 1,
            discovery_id,
            operation_id,
            phase: NodeSetupPhase::Ready,
            storage_mode: Some(SetupStorageMode::Supplied),
            node_id: Some(2),
            cluster_id: Some(authority.bootstrap.identity.cluster_id.clone()),
            disks: statuses,
            services: vec![],
            error: None,
        }),
        observed_at_unix_ms: now(),
        services: vec![],
        protocols: vec![],
    };
    (authority, node)
}

async fn test_medium(
    authority: &TestPreparationAuthority,
    group: u64,
) -> (SetupMedium, SetupDiskStatus, DiskGroupUsageSummary) {
    let id = DiskId { high: 17, low: group };
    let path = format!("/data/disks/{group}");
    let medium = SetupMedium {
        path: path.clone(),
        backend: DiskBackend::File,
        backing_identity: format!("file:{group}"),
        resource_identity: Some(format!("file:1:{group}")),
        capacity_bytes: 64 * 1024 * 1024,
        media_type: None,
        initialize: true,
    };
    let status = SetupDiskStatus {
        path: path.clone(),
        phase: SetupStepPhase::Ready,
        detail: None,
        operation_id: uuid::Uuid::new_v4().to_string(),
        disk_id: Some(id.to_display_string()),
    };
    put(
        authority,
        &DiskGroupKey {
            rack_id: 4,
            node_id: 2,
            disk_group_id: group,
        }
        .to_path(),
        &DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: vec![id],
            ..Default::default()
        },
    )
    .await;
    put(
        authority,
        &DiskKey {
            rack_id: 4,
            node_id: 2,
            disk_group_id: group,
            disk_id: id,
        }
        .to_path(),
        &DiskValue {
            status: HwStatus::Up as i32,
            capacity_units: 64,
            device_path: path,
            ..Default::default()
        },
    )
    .await;
    put(
        authority,
        &format!("/hw/dg_bind/4/2/{group}"),
        &BindMapValue {
            store_id: 0,
            group_id: 1,
        },
    )
    .await;
    put(
        authority,
        &format!("/hw/dg_owner/4/2/{group}"),
        &OwnerMapValue {
            instance_id: 42,
            lease_expiry_ms: now() + 120_000,
            ..Default::default()
        },
    )
    .await;
    let usage = DiskGroupUsageSummary {
        disk_group_id: group,
        usage_known: true,
        sampled_at_ms: now(),
        allocatable_disk_count: 1,
        allocatable_free_bytes: 4096,
        allocatable_capacity_bytes: 8192,
        ..Default::default()
    };
    (medium, status, usage)
}

#[tokio::test]
async fn supplied_storage_checks_every_actual_group_and_never_requires_the_automatic_reservation() {
    let (authority, node) = setup().await;
    let _ = authority.ctx.kv().drain_window();
    let report = collect(&authority.ctx, node.clone()).await.unwrap();
    let facts: Vec<_> = report
        .observation
        .dependencies
        .iter()
        .filter(|fact| fact.kind == DependencyKind::Storage)
        .collect();
    assert_eq!(
        facts.len(),
        6,
        "two accepted media classes require disk, owner and space evidence each"
    );
    assert!(
        facts.iter().all(|fact| fact.state == DependencyState::Ready),
        "{facts:?}"
    );
    assert!(!facts.iter().any(|fact| fact.id.starts_with("storage/4/2/1")));
    let work = authority.ctx.kv().drain_window();
    assert_eq!(
        work.put.len() + work.delete.len() + work.batch_write.len(),
        0,
        "status must never repair placement"
    );
    let key = DiskKey {
        rack_id: 4,
        node_id: 2,
        disk_group_id: 9,
        disk_id: DiskId { high: 17, low: 9 },
    };
    put(
        &authority,
        &key.to_path(),
        &DiskValue {
            status: HwStatus::Offline as i32,
            capacity_units: 64,
            ..Default::default()
        },
    )
    .await;
    let report = collect(&authority.ctx, node).await.unwrap();
    assert!(report
        .observation
        .dependencies
        .iter()
        .any(|fact| fact.id == "storage/4/2/9" && fact.state != DependencyState::Ready));
}

#[tokio::test]
async fn missing_or_duplicate_supplied_identity_and_conflicting_setup_never_become_ready() {
    let (authority, node) = setup().await;
    for mutation in 0..4 {
        let mut changed = node.clone();
        let setup = changed.setup.as_mut().unwrap();
        match mutation {
            0 => setup.disks[1].disk_id = None,
            1 => setup.disks[1].disk_id = setup.disks[0].disk_id.clone(),
            2 => setup.cluster_id = Some(uuid::Uuid::new_v4().to_string()),
            _ => setup.disks[1].phase = SetupStepPhase::Failed,
        }
        let report = collect(&authority.ctx, changed).await.unwrap();
        assert!(
            report
                .observation
                .dependencies
                .iter()
                .any(|fact| fact.kind == DependencyKind::Storage && fact.state != DependencyState::Ready),
            "mutation {mutation}"
        );
    }
}
