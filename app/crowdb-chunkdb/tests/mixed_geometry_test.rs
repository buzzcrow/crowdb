// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;

use crowdb_chunkdb::allocator::{ChunkAllocator, DiskdbClientPool, StripAllocType};
use crowdb_chunkdb::selector::PlacementConstraints;
use crowdb_chunkdb::topology::build_snapshot;
use crowdb_kv_client::ServiceRegistryClient;
use std::sync::Arc;

#[tokio::test]
async fn changed_disk_geometry_is_rejected_before_allocating_any_fragment() {
    let cluster = common::cluster::KvCluster::start().await;
    let hardware = cluster.make_hardware_client();
    common::cluster::seed_hardware(&hardware).await;
    let disks = hardware.list_all_disks().await.unwrap();
    let mut disk = disks[0].clone();
    disk.value.unit_size_bytes *= 2;
    hardware
        .add_disk(
            disk.rack_id,
            disk.node_id,
            disk.disk_group_id,
            &disk.disk_id,
            &disk.value,
        )
        .await
        .unwrap();
    let snapshot = build_snapshot(&hardware).await.unwrap();
    let service = ServiceRegistryClient::from_shared(cluster.make_crowdb_client());
    let allocator = ChunkAllocator::new(Arc::new(DiskdbClientPool::new(service)));
    let owner = crowdb_protocol::common::ChunkId { high: 1, low: 1 };
    let error = allocator
        .allocate_strip(
            &snapshot,
            &owner,
            StripAllocType::Mirror { copy_count: 2 },
            1,
            0,
            &PlacementConstraints::default(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("uniform allocation unit"), "{error}");
    let error = allocator
        .allocate_replacement_segment(&snapshot, &owner, 1, &PlacementConstraints::default(), Vec::new())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("uniform allocation unit"), "{error}");
    let error = allocator
        .allocate_conversion_group(&snapshot, &owner, 1, 0, 2, 1, 1, &PlacementConstraints::default())
        .await
        .err()
        .expect("mixed conversion geometry rejected");
    assert!(error.to_string().contains("uniform allocation unit"), "{error}");
}
