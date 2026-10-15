// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_kv::cluster::{
    group::PxGroup, kv_server::KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient};
use crowdb_protocol::{
    common::{DiskId, HwStatus, NodeValue, RackValue},
    disk_storage::DiskStorageRecord,
    diskdb::rpc::{DiskGroupValue, DiskValue},
    key::DiskKey,
    mgmt::node::storage::DiskBackend,
};

pub struct TestStorage {
    store: Arc<PxKvStore>,
    pub hardware: HardwareClient,
}

impl TestStorage {
    pub async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let client = CrowdbKvClient::new(ClientConfig::new(vec![]));
        client.seed_leader(0, 0, store.listen_addr().unwrap().to_string());
        let hardware = HardwareClient::new(client);
        hardware
            .add_rack(
                1,
                &RackValue {
                    node_ids: vec![2],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        hardware
            .add_node(
                1,
                2,
                &NodeValue {
                    disk_group_ids: vec![3],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        hardware
            .add_disk_group(1, 2, 3, &DiskGroupValue::default())
            .await
            .unwrap();
        Self { store, hardware }
    }
}

impl Drop for TestStorage {
    fn drop(&mut self) {
        self.store.stop();
    }
}

pub fn record(low: u64) -> DiskStorageRecord {
    DiskStorageRecord {
        version: 1,
        operation_id: format!("operation-{low}"),
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
        disk_id: DiskId { high: 4, low },
        backend: DiskBackend::File,
        backing_identity: "file:10:20".into(),
        media_type: None,
        sparse: false,
        geometry: DiskValue {
            disk_type: 0,
            capacity_units: 4096,
            zone_size_units: 1024,
            unit_size_bytes: 4096,
            zone_count: 4,
            status: HwStatus::Init as i32,
            device_path: "/opt/crowdb/data/disks/original.img".into(),
        },
        removed: false,
    }
}

pub fn key(record: &DiskStorageRecord) -> DiskKey {
    DiskKey {
        rack_id: record.rack_id,
        node_id: record.node_id,
        disk_group_id: record.disk_group_id,
        disk_id: record.disk_id,
    }
}
