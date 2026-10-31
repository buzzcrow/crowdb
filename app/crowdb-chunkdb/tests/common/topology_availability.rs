// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::{common::DiskioHardwareScope, sysdata::DiskGroupEntry};

pub async fn heartbeat(registry: &ServiceRegistryClient, group: &DiskGroupEntry) {
    registry
        .heartbeat_diskio_scoped(
            group.dg_id,
            "127.0.0.1:13000",
            &DiskioHardwareScope {
                version: 1,
                rack_id: group.rack_id,
                node_id: group.node_id,
                disk_group_id: group.dg_id,
            },
            &[group.dg_id],
            &[],
        )
        .await
        .unwrap();
}
