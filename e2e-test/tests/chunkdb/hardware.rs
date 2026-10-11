// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::HardwareClient;
use crowdb_protocol::common::{DiskId, HwStatus, NodeValue, RackValue};
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
const ZONE_SIZE_UNITS: u64 = 128;
const UNIT_SIZE_BYTES: u32 = 1024 * 1024;
const INSTANCE_ID: u64 = 999;
const STORE_ID: u64 = 0;
const DATA_GROUP_ID: u64 = 1;
fn make_disk_id(low: u64) -> DiskId {
    DiskId { high: 0, low }
}

#[tokio::test]
#[ignore = "requires the owned Chunkdb container fixture"]
async fn seed() {
    super::voting::register_actual_voters().await;
    seed_hardware_layout_from_disk_group(
        &HardwareClient::new(crate::common::hardware::kv_client()),
        &[(100, vec![10]), (101, vec![11]), (102, vec![12]), (103, vec![13])],
        4,
        1000,
    )
    .await;
}

async fn seed_hardware_layout_from_disk_group(
    hw: &HardwareClient,
    layout: &[(u64, Vec<u64>)],
    zone_count: u32,
    first_dg_id: u64,
) -> Vec<u64> {
    let capacity_units = ZONE_SIZE_UNITS.saturating_mul(u64::from(zone_count));
    let lease_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
        + 3_600_000;

    let mut disk_groups = Vec::new();
    for (rack_id, node_ids) in layout {
        let rack_first_dg_id =
            first_dg_id.saturating_add(u64::try_from(disk_groups.len()).unwrap_or(u64::MAX));

        hw.add_rack(
            *rack_id,
            &RackValue {
                status: HwStatus::Up as i32,
                node_ids: node_ids.clone(),
                ..Default::default()
            },
        )
        .await
        .expect("add rack");

        for (node_offset, node_id) in node_ids.iter().enumerate() {
            let dg_id = rack_first_dg_id.saturating_add(u64::try_from(node_offset).unwrap_or(u64::MAX));
            hw.add_node(
                *rack_id,
                *node_id,
                &NodeValue {
                    status: HwStatus::Up as i32,
                    last_used_dg_id: 0,
                    disk_group_ids: vec![dg_id],
                    status_changed_at_ms: 0,
                    temp_failure_since_ms: None,
                    ..Default::default()
                },
            )
            .await
            .expect("add node");

            let disk_ids = vec![
                make_disk_id(dg_id * 10 + 1),
                make_disk_id(dg_id * 10 + 2),
                make_disk_id(dg_id * 10 + 3),
            ];
            hw.add_disk_group(
                *rack_id,
                *node_id,
                dg_id,
                &DiskGroupValue {
                    status: HwStatus::Up as i32,
                    disk_ids: disk_ids.clone(),
                    name: String::new(),
                },
            )
            .await
            .expect("add disk-group");

            for did in &disk_ids {
                hw.add_disk(
                    *rack_id,
                    *node_id,
                    dg_id,
                    did,
                    &DiskValue {
                        disk_type: DiskType::BlockSsd as i32,
                        capacity_units,
                        zone_size_units: ZONE_SIZE_UNITS,
                        unit_size_bytes: UNIT_SIZE_BYTES,
                        zone_count,
                        status: HwStatus::Up as i32,
                        device_path: String::new(),
                    },
                )
                .await
                .expect("add disk");
            }

            hw.set_owner(*rack_id, *node_id, dg_id, INSTANCE_ID, lease_ms)
                .await
                .expect("set owner");
            hw.set_bind(*rack_id, *node_id, dg_id, STORE_ID, DATA_GROUP_ID)
                .await
                .expect("set bind");
            disk_groups.push(dg_id);
        }
    }
    disk_groups
}
