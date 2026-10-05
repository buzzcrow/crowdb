// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use bytes::Bytes;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::common::config::CrowDBConfig;
use crowdb_kv_server::background::domain_monitor::{DiskdbPlacementMonitorDriver, DomainMonitorDriver};
use crowdb_kv_server::group0_control_plane::Group0ControlPlane;
use crowdb_kv_server::store_registry::KvStoreRegistry;
use crowdb_protocol::chunk_kv::{DomainFailurePolicy, DomainMonitorDescriptor};
use crowdb_protocol::common::{BindMapValue, InstanceValue, OwnerMapValue};
use crowdb_protocol::key::{BindMapKey, DiskGroupKey, InstanceKey, OwnerMapKey, TextKey};
use std::sync::Arc;
fn descriptor() -> DomainMonitorDescriptor {
    DomainMonitorDescriptor {
        domain: "diskdb-ownership".into(),
        service_registry_name: "diskdb".into(),
        driver_version: 1,
        capability_version: 1,
        heartbeat_interval_ms: 10,
        suspect_after_ms: 30,
        dead_after_ms: 50,
        lease_duration_ms: 70,
        max_clock_skew_ms: 5,
        self_fence_margin_ms: 5,
        failure_policy: DomainFailurePolicy::AutomaticSharedStorage,
        balance_policy: "disk-group-count-v1".into(),
        chunk_kv_range_balance: None,
    }
}

fn registry_with_group_zero(role: PxLocalReplicaRole) -> (Arc<KvStoreRegistry>, Arc<PxKvStore>) {
    let registry = Arc::new(KvStoreRegistry::with_config(CrowDBConfig::for_tests()));
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(0, PxLocalReplica::new(1, role)));
    registry.add_store(0, &store);
    (registry, store)
}

async fn put_json<T: serde::Serialize>(control: &Group0ControlPlane, path: String, value: &T) {
    control
        .compare_and_put(
            Bytes::from(path),
            Bytes::from(serde_json::to_vec(value).unwrap()),
            0,
        )
        .await
        .unwrap();
}

async fn get_json<T: serde::de::DeserializeOwned>(control: &Group0ControlPlane, path: &str) -> T {
    serde_json::from_slice(&control.get(path.as_bytes()).await.unwrap().value.unwrap()).unwrap()
}

async fn instance(control: &Group0ControlPlane, id: u64, heartbeat: u64) {
    let key = InstanceKey {
        service: "diskdb".into(),
        instance_id: id,
    }
    .to_path();
    let revision = control.get(key.as_bytes()).await.unwrap().revision;
    control
        .compare_and_put(
            Bytes::from(key),
            Bytes::from(
                serde_json::to_vec(&InstanceValue {
                    instance_id: id,
                    rpc_endpoint: format!("127.0.0.1:{}", 17000 + id),
                    last_heartbeat_ms: heartbeat,
                    extra: None,
                })
                .unwrap(),
            ),
            revision,
        )
        .await
        .unwrap();
    let key = format!("/diskdb/ownership-capability/{id}");
    if control.get(key.as_bytes()).await.unwrap().value.is_none() {
        put_json(control, key, &1).await;
    }
}

#[tokio::test]
async fn assigns_balances_and_replaces_dead_owners_without_changing_bindings() {
    let (_, store) = registry_with_group_zero(PxLocalReplicaRole::Leader);
    let control = Group0ControlPlane::acquire(&store).await.unwrap();
    instance(&control, 1, u64::MAX).await;
    for dg in 1..=5 {
        put_json(
            &control,
            DiskGroupKey {
                rack_id: 1,
                node_id: 1,
                disk_group_id: dg,
            }
            .to_path(),
            &serde_json::json!({"status": 0, "disk_ids": [], "name": ""}),
        )
        .await;
        if dg != 5 {
            put_json(
                &control,
                BindMapKey {
                    rack_id: 1,
                    node_id: 1,
                    disk_group_id: dg,
                }
                .to_path(),
                &BindMapValue {
                    store_id: 7,
                    group_id: 9,
                },
            )
            .await;
        }
    }
    let driver = DiskdbPlacementMonitorDriver;
    let policy = descriptor();
    driver.tick(&control, &policy).await.unwrap();
    let owner_key = |dg| {
        OwnerMapKey {
            rack_id: 1,
            node_id: 1,
            disk_group_id: dg,
        }
        .to_path()
    };
    for dg in 1..=4 {
        let owner: OwnerMapValue = get_json(&control, &owner_key(dg)).await;
        assert_eq!(owner.instance_id, 1);
    }
    assert!(control
        .get(owner_key(5).as_bytes())
        .await
        .unwrap()
        .value
        .is_none());
    instance(&control, 2, u64::MAX).await;
    driver.tick(&control, &policy).await.unwrap();
    driver.tick(&control, &policy).await.unwrap();
    let mut counts = [0; 2];
    for dg in 1..=4 {
        let owner: OwnerMapValue = get_json(&control, &owner_key(dg)).await;
        counts[owner.instance_id as usize - 1] += 1;
    }
    assert_eq!(counts, [2, 2]);
    instance(&control, 1, 1).await;
    driver.tick(&control, &policy).await.unwrap();
    for dg in 1..=4 {
        let owner: OwnerMapValue = get_json(&control, &owner_key(dg)).await;
        assert_eq!(owner.instance_id, 2);
        let key = BindMapKey {
            rack_id: 1,
            node_id: 1,
            disk_group_id: dg,
        }
        .to_path();
        let binding: BindMapValue = get_json(&control, &key).await;
        assert_eq!((binding.store_id, binding.group_id), (7, 9));
    }
    let key = BindMapKey {
        rack_id: 1,
        node_id: 1,
        disk_group_id: 5,
    }
    .to_path();
    assert!(control.get(key.as_bytes()).await.unwrap().value.is_none());
    assert!(control
        .get(owner_key(5).as_bytes())
        .await
        .unwrap()
        .value
        .is_none());
}
