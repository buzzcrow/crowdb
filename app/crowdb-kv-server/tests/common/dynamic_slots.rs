// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv_server::group0_control_plane::Group0ControlPlane;
use crowdb_protocol::chunk_kv::{DomainFailurePolicy, DomainMonitorDescriptor};
use crowdb_protocol::chunk_slot::{ChunkSlot, ChunkSlotBinding, ChunkSlotMapHead, ChunkStorageGroup};
use crowdb_protocol::common::InstanceValue;
use crowdb_protocol::key::{
    ChunkServiceSlotsKey, ChunkSlotMapHeadKey, ChunkStorageSlotsKey, InstanceKey, TextKey,
};

pub fn descriptor() -> DomainMonitorDescriptor {
    DomainMonitorDescriptor {
        domain: "chunkdb".into(),
        service_registry_name: "chunkdb".into(),
        driver_version: 3,
        failure_policy: DomainFailurePolicy::AutomaticSharedStorage,
        dead_after_ms: 60_000,
        suspect_after_ms: 30_000,
        balance_policy: "dynamic-service-slots-v1".into(),
        ..Default::default()
    }
}

pub async fn control() -> (Arc<PxKvStore>, Group0ControlPlane) {
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(
        0,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    let control = Group0ControlPlane::acquire(&store).await.unwrap();
    (store, control)
}

pub fn entry(key: String, value: &impl serde::Serialize) -> (Bytes, Bytes) {
    (Bytes::from(key), Bytes::from(serde_json::to_vec(value).unwrap()))
}

pub async fn initialize(control: &Group0ControlPlane) {
    let mut records = vec![
        entry(
            ChunkSlotMapHeadKey::Service.to_path(),
            &ChunkSlotMapHead {
                layout_version: 1,
                generation: 1,
                owner_count: 3,
            },
        ),
        entry(
            ChunkSlotMapHeadKey::Storage.to_path(),
            &ChunkSlotMapHead {
                layout_version: 1,
                generation: 9,
                owner_count: 3,
            },
        ),
    ];
    for owner in 1..=3 {
        records.push(entry(
            ChunkServiceSlotsKey { instance_id: owner }.to_path(),
            &ChunkSlotBinding {
                generation: 1,
                owner,
                slots: ChunkSlot::all()
                    .filter(|slot| u64::from(slot.value()) % 3 + 1 == owner)
                    .collect(),
            },
        ));
        records.push(entry(
            ChunkStorageSlotsKey {
                store_id: 0,
                group_id: owner,
            }
            .to_path(),
            &ChunkSlotBinding {
                generation: 9,
                owner: ChunkStorageGroup {
                    store_id: 0,
                    group_id: owner,
                },
                slots: ChunkSlot::all()
                    .filter(|slot| u64::from(slot.value() / 16) % 3 + 1 == owner)
                    .collect(),
            },
        ));
    }
    records.push(entry(
        ChunkSlotMapHeadKey::Authority.to_path(),
        &ChunkSlotMapHead {
            layout_version: 1,
            generation: 1,
            owner_count: 3,
        },
    ));
    for owner in 1..=3 {
        let authority = crowdb_protocol::chunk_slot::ChunkSlotAuthority::new(
            owner,
            crowdb_protocol::chunk_slot::ChunkServiceIncarnation::try_from([1; 16]).unwrap(),
            1,
        )
        .unwrap();
        records.push(entry(
            crowdb_protocol::key::ChunkServiceAuthorityKey { authority }.to_path(),
            &ChunkSlotBinding {
                generation: 1,
                owner: authority,
                slots: ChunkSlot::all()
                    .filter(|slot| u64::from(slot.value()) % 3 + 1 == owner)
                    .collect(),
            },
        ));
    }
    control.put_batch(records).await.unwrap();
}

pub struct TestDynamicOwners;
impl TestDynamicOwners {
    pub async fn register(control: &Group0ControlPlane, owner: u64, heartbeat: u64) {
        control
            .put_batch(vec![entry(
                InstanceKey {
                    service: "chunkdb-epoch-v1".into(),
                    instance_id: owner,
                }
                .to_path(),
                &InstanceValue {
                    instance_id: owner,
                    rpc_endpoint: format!("127.0.0.1:{}", 17000 + owner),
                    last_heartbeat_ms: heartbeat,
                    extra: None,
                },
            )])
            .await
            .unwrap();
    }
    pub async fn bindings(control: &Group0ControlPlane) -> Vec<ChunkSlotBinding<u64>> {
        control
            .scan_all_prefix(Bytes::from_static(b"/chunkdb/slot_service/"), 8)
            .await
            .unwrap()
            .into_iter()
            .map(|record| serde_json::from_slice(&record.value).unwrap())
            .collect()
    }
}
