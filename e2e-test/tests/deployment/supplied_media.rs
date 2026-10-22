// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_diskio_client::{
    DiskId, DiskioClient, DiskioClientConfig, Durability, OperationOptions, SegmentTarget,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient};
use crowdb_protocol::{common::HwStatus, key::DiskKey, mgmt::node::storage::DiskBackend, DiskIdExt};
use serde_json::Value;
use std::time::Duration;

async fn assert_selected_host(kv: &CrowdbKvClient, specification: &Value, host: &str) {
    let (registry, _) = crowdb_console_shared::deployment::registry::read(kv)
        .await
        .unwrap()
        .expect("current confirmed node registry");
    assert_eq!(registry.cluster_id, specification["cluster_id"].as_str().unwrap());
    let node = registry
        .nodes
        .iter()
        .find(|node| {
            node.node_id == specification["node_id"].as_u64().unwrap() && node.confirmed && !node.cancelled
        })
        .expect("confirmed supplied-media host");
    assert_eq!(node.physical_host_id, host);
}

#[tokio::test]
#[ignore = "requires an owned Linux host profile and its accepted disposable media"]
async fn retained_data_probe() {
    let path = std::env::var("CROWDB_SUPPLIED_MEDIA_PROBE").expect("owned media probe specification");
    let specification: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(specification["version"], 1);
    let mode = specification["mode"].as_str().unwrap();
    assert!(matches!(mode, "write" | "verify"));
    let host = specification["physical_host_id"].as_str().unwrap();
    let seeds: Vec<String> = serde_json::from_value(specification["seeds"].clone()).unwrap();
    let kv = CrowdbKvClient::new(ClientConfig::new(seeds.clone()));
    assert_selected_host(&kv, &specification, host).await;
    let hardware = HardwareClient::new(kv);
    let disks = specification["disks"].as_array().unwrap();
    assert_eq!(disks.len(), 2);
    let unit = 1024 * 1024;
    let expected = Bytes::from(vec![b'Z'; unit as usize]);
    for disk in disks {
        let id =
            crowdb_protocol::common::DiskId::from_display_string(disk["disk_id"].as_str().unwrap()).unwrap();
        let key = DiskKey {
            rack_id: specification["rack_id"].as_u64().unwrap(),
            node_id: specification["node_id"].as_u64().unwrap(),
            disk_group_id: specification["disk_group_id"].as_u64().unwrap(),
            disk_id: id,
        };
        let record = hardware.disk_storage(&key).await.unwrap().unwrap();
        assert_eq!(record.version, 2);
        assert_eq!(record.operation_id, disk["operation_id"].as_str().unwrap());
        assert_eq!(
            record.geometry.device_path,
            disk["medium"]["path"].as_str().unwrap()
        );
        assert!(matches!(
            record.geometry.device_path.as_str(),
            "/opt/crowdb/data/disks/supplied-file.img" | "/opt/crowdb/data/disks/supplied-loop"
        ));
        assert_eq!(
            record.backend,
            if record.geometry.device_path.ends_with("supplied-loop") {
                DiskBackend::BlockDevice
            } else {
                DiskBackend::File
            }
        );
        assert_eq!(record.physical_backing.as_ref().unwrap().physical_host_id, host);
        assert_eq!(record.geometry.unit_size_bytes, unit);
        assert_eq!(record.geometry.capacity_units, 64);
        assert_eq!(
            hardware
                .get_disk_with_revision(&key)
                .await
                .unwrap()
                .unwrap()
                .0
                .status,
            HwStatus::Up as i32
        );
        let client = DiskioClient::connect_disk(
            DiskioClientConfig {
                management_seeds: seeds.clone(),
                retry_attempts: 1,
                rpc_workers: 1,
                normal_connections_per_endpoint: 1,
                priority_connections_per_endpoint: 1,
                ..DiskioClientConfig::default()
            },
            key,
        )
        .await
        .unwrap();
        let target = SegmentTarget::new(DiskId::new(id.high, id.low), 0, 1, 1, unit).unwrap();
        if mode == "write" {
            client
                .write(
                    target,
                    0,
                    expected.clone(),
                    Durability::Fsync,
                    OperationOptions::within(Duration::from_secs(3)),
                )
                .await
                .unwrap();
        }
        let actual = client
            .read(target, 0, unit, OperationOptions::within(Duration::from_secs(3)))
            .await
            .unwrap();
        assert_eq!(actual, expected);
        println!(
            "owned {:?} disk {}: durable {mode} probe passed",
            record.backend, disk["disk_id"]
        );
    }
}
