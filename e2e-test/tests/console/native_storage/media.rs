// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Trusted legacy bootstrap for freshly truncated, owned native fixture files.

use super::TestNativeContext;
use crowdb_paxos_kv_client::HardwareClient;
use crowdb_protocol::{
    common::{DiskId, HwStatus},
    disk_storage::DiskStorageRecord,
    diskdb::rpc::DiskValue,
    mgmt::node::storage::DiskBackend,
};
use reqwest::Method;
use serde_json::Value;

pub(super) async fn admit(context: &TestNativeContext, units: [u64; 3], profile: &Value) {
    let sizes = profile["disk_gib"].as_array().unwrap();
    let zones = profile["zone_gib"].as_array().unwrap();
    let backings = profile["backings"].as_array().unwrap();
    assert_eq!((sizes.len(), zones.len(), backings.len()), (3, 3, 3));
    let hardware = HardwareClient::from_shared(context.kv.clone());
    for node in 1..=3 {
        let index = usize::try_from(node - 1).unwrap();
        let capacity = sizes[index].as_u64().unwrap() * 1024_u64.pow(3);
        let zone = zones[index].as_u64().unwrap() * 1024_u64.pow(3);
        let path = format!("/opt/crowdb/data/disks/disk-{node}.img");
        let rack = hardware
            .list_nodes()
            .await
            .unwrap()
            .into_iter()
            .find(|(_, id, _)| *id == node)
            .unwrap()
            .0;
        assert_eq!(backings[index]["path"], path);
        assert_eq!(backings[index]["capacity_bytes"], capacity);
        let record = DiskStorageRecord {
            version: 1,
            physical_backing: None,
            operation_id: format!("native-file-{node}"),
            rack_id: rack,
            node_id: node,
            disk_group_id: node,
            disk_id: DiskId { high: 0, low: node },
            backend: DiskBackend::File,
            backing_identity: backings[index]["backing_identity"].as_str().unwrap().into(),
            media_type: Some(0),
            sparse: true,
            removed: false,
            geometry: DiskValue {
                disk_type: 0,
                capacity_units: capacity / units[index],
                zone_size_units: zone / units[index],
                unit_size_bytes: u32::try_from(units[index]).unwrap(),
                zone_count: u32::try_from(capacity / zone).unwrap(),
                status: HwStatus::Init as i32,
                device_path: path,
            },
        };
        record.validate().unwrap();
        hardware.admit_disk_storage(&record).await.unwrap();
        hardware.activate_disk_storage(&record).await.unwrap();
        let rows = context
            .api(
                Method::GET,
                &format!("/api/nodes/{node}/disk-groups/{node}/disks"),
                Value::Null,
                200,
            )
            .await;
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["capacity_bytes"], capacity);
    }
}
