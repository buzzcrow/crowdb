// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Hardware metadata seeding for E2E tests.

use crowdb_kv_client::HardwareClient;
use crowdb_protocol::common::{DiskId, HwStatus, NodeValue, RackValue};
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};

pub const RACK_ID: u64 = 1;
pub const NODE_ID: u64 = 10;
pub const DG_ID: u64 = 100;
pub const INSTANCE_ID: u64 = 999;
pub const STORE_ID: u64 = 0;
pub const DATA_GROUP_ID: u64 = 1;

// Small disks: 4 zones × 128 units × 1 MB = 512 MB per disk.
pub const ZONE_SIZE_UNITS: u64 = 128;
pub const UNIT_SIZE_BYTES: u32 = 1024 * 1024; // 1 MB
pub const ZONE_COUNT: u32 = 4;
pub const CAPACITY_UNITS: u64 = ZONE_SIZE_UNITS * ZONE_COUNT as u64;

pub fn make_disk_id(high: u64, low: u64) -> DiskId {
    DiskId { high, low }
}

/// Seed hardware metadata into group 0: rack, node, disk-group, disks,
/// owner lease, and KV-group bind. The `disk_ids` parameter controls
/// how many disks are created.
pub async fn seed_hardware(hw: &HardwareClient, disk_ids: &[DiskId]) {
    hw.add_rack(
        RACK_ID,
        &RackValue {
            status: HwStatus::Up as i32,
            node_ids: vec![NODE_ID],
            ..Default::default()
        },
    )
    .await
    .expect("add rack");

    hw.add_node(
        RACK_ID,
        NODE_ID,
        &NodeValue {
            status: HwStatus::Up as i32,
            last_used_dg_id: 0,
            disk_group_ids: vec![DG_ID],
            status_changed_at_ms: 0,
            temp_failure_since_ms: None,
            ..Default::default()
        },
    )
    .await
    .expect("add node");

    hw.add_disk_group(
        RACK_ID,
        NODE_ID,
        DG_ID,
        &DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: disk_ids.to_vec(),
            name: String::new(),
        },
    )
    .await
    .expect("add disk-group");

    for did in disk_ids {
        hw.add_disk(
            RACK_ID,
            NODE_ID,
            DG_ID,
            did,
            &DiskValue {
                disk_type: DiskType::BlockSsd as i32,
                capacity_units: CAPACITY_UNITS,
                zone_size_units: ZONE_SIZE_UNITS,
                unit_size_bytes: UNIT_SIZE_BYTES,
                zone_count: ZONE_COUNT,
                status: HwStatus::Up as i32,
                device_path: String::new(),
            },
        )
        .await
        .expect("add disk");
    }

    let lease_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
        + 3_600_000;
    hw.set_owner(RACK_ID, NODE_ID, DG_ID, INSTANCE_ID, lease_ms)
        .await
        .expect("set owner");

    hw.set_bind(RACK_ID, NODE_ID, DG_ID, STORE_ID, DATA_GROUP_ID)
        .await
        .expect("set bind");
}

pub fn standard_disk_ids_3() -> Vec<DiskId> {
    (1..=3).map(|low| make_disk_id(0, low)).collect()
}

pub fn standard_disk_ids_4() -> Vec<DiskId> {
    let mut ids = standard_disk_ids_3();
    ids.push(make_disk_id(0xAB, 4));
    ids
}

pub(crate) fn kv_client() -> crowdb_kv_client::CrowdbKvClient {
    super::logging::init();
    let addresses: Vec<String> = serde_json::from_str(&std::env::var("CROWDB_E2E_RPC").unwrap()).unwrap();
    crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(vec![format!(
        "http://{}:7000",
        addresses[0]
    )]))
}
