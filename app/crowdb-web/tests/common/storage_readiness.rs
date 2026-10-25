// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::HardwareClient;
use crowdb_protocol::{
    common::{DiskId, HwStatus, NodeValue, RackValue},
    diskdb::rpc::DiskValue,
};

pub async fn publish_disk(hardware: &HardwareClient) {
    hardware
        .add_rack(
            1,
            &RackValue {
                node_ids: vec![1],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    hardware
        .add_node(
            1,
            1,
            &NodeValue {
                disk_group_ids: vec![11, 12],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    hardware
        .add_disk(
            1,
            1,
            11,
            &DiskId { high: 0, low: 11 },
            &DiskValue {
                status: HwStatus::Up as i32,
                ..Default::default()
            },
        )
        .await
        .unwrap();
}
