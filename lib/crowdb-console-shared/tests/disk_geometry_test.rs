// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::config::{ConsoleConfig, DiskGroupEntry};
use crowdb_console_shared::error::Error;
use crowdb_console_shared::ops::{hardware, OpContext};

#[tokio::test]
async fn invalid_capacity_is_rejected_before_disk_membership_changes() {
    let mut config = ConsoleConfig::default();
    config.disk_groups.push(DiskGroupEntry {
        id: 1,
        rack_id: 1,
        node_id: 1,
        name: "storage".into(),
    });
    let ctx = OpContext::new_for_test("127.0.0.1:59999".into(), vec![], config);
    let mut input = hardware::AddDiskInput {
        disk_id: "0000000000000000-0000000000000001".into(),
        disk_type: "Ssd".into(),
        capacity_bytes: 8193,
        zone_size_bytes: 4096,
        unit_size_bytes: 4096,
        device_path: "/opt/crowdb/data/disks/test.img".into(),
    };
    for capacity in [8193, (u64::from(u32::MAX) + 1) * 4096] {
        input.capacity_bytes = capacity;
        let error = hardware::add_disk(&ctx, 1, 1, &input).await.unwrap_err();
        assert!(matches!(error, Error::Validation { field, .. } if field == "capacity_bytes"));
        assert!(ctx.config().disks.is_empty());
    }
    input.capacity_bytes = 8192;
    let disk = hardware::add_disk(&ctx, 1, 1, &input).await.unwrap();
    assert_eq!(disk.capacity_bytes, 8192);
    assert_eq!(ctx.config().disks.len(), 1);
}
