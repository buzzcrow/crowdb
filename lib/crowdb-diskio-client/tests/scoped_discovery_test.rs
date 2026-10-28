// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_diskio_client::{DiskioClient, DiskioClientConfig};
use crowdb_paxos_kv::cluster::{
    group::PxGroup, paxos_kv_server::PaxosKvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_paxos_kv_client::{ClientConfig, HardwareClient, PaxosKvClient, ServiceRegistryClient};
use crowdb_protocol::{
    common::{DiskId, HwStatus},
    diskdb::rpc::DiskValue,
    key::DiskKey,
};

struct TestDiscovery {
    store: Arc<PxKvStore>,
    kv: Arc<PaxosKvClient>,
}
impl TestDiscovery {
    async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(vec![])));
        kv.seed_leader(0, 0, store.listen_addr().unwrap().to_string());
        Self { store, kv }
    }
    fn hardware(&self) -> HardwareClient {
        HardwareClient::from_shared(Arc::clone(&self.kv))
    }
    fn services(&self) -> ServiceRegistryClient {
        ServiceRegistryClient::from_shared(Arc::clone(&self.kv))
    }
    async fn disk(&self, group: u64, status: HwStatus) -> DiskKey {
        let key = DiskKey {
            rack_id: 1,
            node_id: 2,
            disk_group_id: group,
            disk_id: DiskId { high: 0, low: group },
        };
        self.hardware()
            .add_disk(
                1,
                2,
                group,
                &key.disk_id,
                &DiskValue {
                    status: status as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        key
    }
    async fn owner(&self, group: u64) {
        self.services()
            .heartbeat_diskio_at(
                group,
                &self.store.listen_addr().unwrap().to_string(),
                1,
                2,
                &[group],
                &[],
            )
            .await
            .unwrap();
    }
}
impl Drop for TestDiscovery {
    fn drop(&mut self) {
        self.store.stop();
    }
}

#[tokio::test]
async fn provisional_and_offline_membership_do_not_disable_existing_routes() {
    let fixture = TestDiscovery::start().await;
    fixture.disk(10, HwStatus::Up).await;
    fixture.owner(10).await;
    fixture.disk(20, HwStatus::Init).await;
    fixture.disk(30, HwStatus::Offline).await;
    let client = DiskioClient::connect_with_clients(
        fixture.services(),
        fixture.hardware(),
        DiskioClientConfig::default(),
    )
    .await
    .unwrap();
    assert_eq!(client.status().disks, 1);
}

#[tokio::test]
async fn administrative_probe_resolves_its_init_disk_without_unrelated_owner_dependencies() {
    let fixture = TestDiscovery::start().await;
    fixture.disk(10, HwStatus::Up).await;
    let target = fixture.disk(20, HwStatus::Init).await;
    fixture.owner(20).await;
    assert!(DiskioClient::connect_with_clients(
        fixture.services(),
        fixture.hardware(),
        DiskioClientConfig::default()
    )
    .await
    .is_err());
    let client = DiskioClient::connect_disk_with_clients_for_tests(
        fixture.services(),
        fixture.hardware(),
        DiskioClientConfig::default(),
        target,
    )
    .await
    .unwrap();
    assert_eq!(client.status().disks, 1);
    assert_eq!(client.status().endpoints, 1);
}
