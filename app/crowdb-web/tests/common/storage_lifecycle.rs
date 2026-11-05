// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::memory_kv::TestKvCluster;
use crowdb_console_shared::{
    config::{NodeEntry, RackEntry, ServerEntry},
    deployment::{
        registry::{NodeRegistry, NODES_KEY},
        PreparedBootstrap,
    },
    ConsoleConfig,
};
use crowdb_paxos_kv_client::{ClientConfig, HardwareClient, PaxosKvClient, ServiceRegistryClient};
use crowdb_protocol::{
    common::{DiskId, DiskioHardwareScope, HwStatus, NodeValue, RackValue},
    disk_storage::DiskStorageRecord,
    diskdb::rpc::{DiskGroupValue, DiskValue},
    key::DiskKey,
    mgmt::node::{storage::DiskBackend, NodeBinding},
};
use crowdb_web::{router, AppState};
use std::sync::Arc;

pub struct TestStorageApi {
    pub app: axum::Router,
    pub hardware: HardwareClient,
    pub disk: DiskKey,
    _cluster: TestKvCluster,
    _root: crowdb_test_harness::test_dirs::TestDir,
}

impl TestStorageApi {
    pub async fn start() -> Self {
        let cluster = TestKvCluster::start().await;
        let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(
            cluster.mgmt_endpoints.clone(),
        )));
        kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
        let hardware = HardwareClient::from_shared(kv.clone());
        let disk = seed_hardware(&hardware).await;
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("storage-lifecycle-api");
        let config = configuration(&cluster.mgmt_endpoints[0]);
        let prepared = PreparedBootstrap::open(&root.path().join("prepared.json"), &config, &[2]).unwrap();
        kv.put(
            0,
            0,
            b"/deployment/cluster",
            &serde_json::to_vec(&prepared).unwrap(),
            None,
        )
        .await
        .unwrap();
        kv.put(
            0,
            0,
            NODES_KEY,
            &serde_json::to_vec(&NodeRegistry {
                cluster_id: prepared.identity.cluster_id.clone(),
                nodes: vec![],
            })
            .unwrap(),
            None,
        )
        .await
        .unwrap();
        std::fs::write(
            root.path().join("node-binding.json"),
            serde_json::to_vec(&NodeBinding {
                node_id: 2,
                bootstrap: prepared.identity,
                management_seeds: cluster.mgmt_endpoints.clone(),
            })
            .unwrap(),
        )
        .unwrap();
        std::fs::create_dir_all(root.path().join("console")).unwrap();
        let state = AppState::with_runtime_root(config, root.path().join("console"))
            .with_node_monitor("http://127.0.0.1:1")
            .unwrap();
        *state.kv_client.write().await = Some(kv);
        ServiceRegistryClient::from_shared(hardware.shared_kv())
            .heartbeat_diskio_scoped(
                7,
                "a:7200",
                &DiskioHardwareScope {
                    version: 1,
                    rack_id: 1,
                    node_id: 2,
                    disk_group_id: 0,
                },
                &[],
                &[],
            )
            .await
            .unwrap();
        Self {
            app: router(state),
            hardware,
            disk,
            _cluster: cluster,
            _root: root,
        }
    }
}

fn configuration(endpoint: &str) -> ConsoleConfig {
    let mut server = ServerEntry::new("kv-2", endpoint);
    server.node_id = Some(2);
    ConsoleConfig {
        racks: vec![RackEntry {
            id: 1,
            name: "rack".into(),
        }],
        nodes: vec![NodeEntry {
            id: 2,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_credential_ref: None,
            ssh_key: None,
            ssh_password: None,
        }],
        servers: vec![server],
        ..Default::default()
    }
}

async fn seed_hardware(hw: &HardwareClient) -> DiskKey {
    hw.add_rack(
        1,
        &RackValue {
            node_ids: vec![2],
            status: HwStatus::Up as i32,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    hw.add_node(
        1,
        2,
        &NodeValue {
            management_host: "127.0.0.1".into(),
            disk_group_ids: vec![3],
            status: HwStatus::Up as i32,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    hw.add_disk_group(
        1,
        2,
        3,
        &DiskGroupValue {
            status: HwStatus::Up as i32,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let disk_id = DiskId {
        high: u64::MAX,
        low: u64::MAX,
    };
    hw.admit_disk_storage(&DiskStorageRecord {
        version: 1,
        physical_backing: None,
        operation_id: "operation".into(),
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
        disk_id,
        backend: DiskBackend::File,
        backing_identity: "file:1:2".into(),
        media_type: None,
        sparse: false,
        removed: false,
        geometry: DiskValue {
            status: HwStatus::Init as i32,
            disk_type: 0,
            device_path: "/opt/crowdb/data/disks/file.img".into(),
            unit_size_bytes: 4096,
            capacity_units: 128,
            zone_size_units: 128,
            zone_count: 1,
        },
    })
    .await
    .unwrap();
    hw.set_disk_status(1, 2, 3, &disk_id, HwStatus::Up).await.unwrap();
    DiskKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
        disk_id,
    }
}
