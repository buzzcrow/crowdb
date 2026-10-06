// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! R74 §7 — `QueryCapacityStats` three shapes (disk-group, disk,
//! zone) + `GetDiskGroupInfo`/`GetDiskInfo` usage fields. Tests the
//! service handler logic via the `build_disk_info` helper + model
//! accessors directly (no crowdb-rpc server needed).

use std::sync::Arc;

use crowdb_diskdb::model::disk::DdbDisk;
use crowdb_diskdb::model::disk_group::DdbDiskGroup;
use crowdb_diskdb::model::zone::DdbZone;
use crowdb_protocol::common::{DiskId, HwStatus};
use crowdb_protocol::diskdb::rpc::DiskValue;

const DG: u64 = 200;
const ZONE_ROTATE: u32 = 4;
const ZONE_CAP: u32 = 64;
const ZONE_COUNT: u32 = 2;
const UNIT_SIZE: u32 = 4096;

fn disk_id(n: u64) -> DiskId {
    DiskId { high: 0, low: n }
}

fn make_disk_value() -> DiskValue {
    DiskValue {
        disk_type: 0,
        capacity_units: u64::from(ZONE_CAP) * u64::from(ZONE_COUNT),
        zone_size_units: u64::from(ZONE_CAP),
        unit_size_bytes: UNIT_SIZE,
        zone_count: ZONE_COUNT,
        status: 0,
        device_path: String::new(),
    }
}

fn make_disk(low: u64) -> Arc<DdbDisk> {
    let disk = Arc::new(DdbDisk::new(disk_id(low), DG, 10, 1, make_disk_value()));
    disk.set_effective_status(HwStatus::Up);
    for zi in 0..ZONE_COUNT {
        let zone = Arc::new(DdbZone::new(disk_id(low), zi, DG, ZONE_CAP));
        disk.add_zone(&zone);
    }
    disk.rebuild_active_zones(ZONE_ROTATE);
    disk
}

fn make_dg() -> Arc<DdbDiskGroup> {
    let dg = Arc::new(DdbDiskGroup::new(DG, 10, 1));
    // Default group status is Init; set to Up for allocation tests.
    dg.set_status(HwStatus::Up);
    dg.add_disk(make_disk(1));
    dg.add_disk(make_disk(2));
    dg.rebuild_allocating_disks();
    dg
}

/// Disk-group shape: `aggregate_usage` sums all disks.
#[test]
fn disk_group_shape_aggregate() {
    let dg = make_dg();
    // Allocate 10 units on disk 1.
    for _ in 0..10 {
        dg.allocate_block(1, &[], 100, ZONE_ROTATE).expect("alloc");
    }
    // Allocate 5 units on disk 2.
    for _ in 0..5 {
        dg.allocate_block(1, &[], 100, ZONE_ROTATE).expect("alloc");
    }

    let usage = dg.aggregate_usage();
    let total_cap = 2 * u64::from(ZONE_COUNT) * u64::from(ZONE_CAP) * u64::from(UNIT_SIZE);
    assert_eq!(usage.capacity_bytes, total_cap);
    assert_eq!(usage.busy_bytes, 15 * u64::from(UNIT_SIZE));
    assert_eq!(usage.free_bytes, total_cap - 15 * u64::from(UNIT_SIZE));
    assert_eq!(usage.disk_count, 2);
    assert_eq!(usage.allocatable_disk_count, 2);
}

/// Disk shape: `disk.usage()` + `disk.zone_usages()` per-zone brief.
/// Uses `disk.disk_allocate` directly to avoid disk-group round-robin.
#[test]
fn disk_shape_per_zone_brief() {
    let dg = make_dg();
    let disk = dg.get_disk(disk_id(1)).expect("disk exists");

    // Allocate 8 units directly on disk 1.
    for _ in 0..8 {
        disk.disk_allocate(1, 100, ZONE_ROTATE).expect("alloc");
    }

    let usage = disk.usage();
    assert_eq!(usage.busy_bytes, 8 * u64::from(UNIT_SIZE));
    assert_eq!(
        usage.capacity_bytes,
        u64::from(ZONE_CAP) * u64::from(ZONE_COUNT) * u64::from(UNIT_SIZE)
    );

    let zones = disk.zone_usages();
    assert_eq!(zones.len(), ZONE_COUNT as usize);
    // Total busy across zones = 8.
    let total_busy: u32 = zones.iter().map(|z| z.busy_block_count).sum();
    assert_eq!(total_busy, 8);
}

/// Zone shape: `dg.zone_usage(disk_id, zone_index)` returns one zone
/// with full detail. Uses `disk.disk_allocate` directly.
#[test]
fn zone_shape_single_zone_detail() {
    let dg = make_dg();
    let disk = dg.get_disk(disk_id(1)).expect("disk exists");

    // Allocate 4 units directly on disk 1.
    for _ in 0..4 {
        disk.disk_allocate(1, 100, ZONE_ROTATE).expect("alloc");
    }

    // Sum busy across all zones = 4.
    let zones = disk.zone_usages();
    let total_busy: u32 = zones.iter().map(|z| z.busy_block_count).sum();
    assert_eq!(total_busy, 4);

    // Verify each zone's usage matches.
    for zu in &zones {
        let dg_zu = dg.zone_usage(disk_id(1), zu.zone_index).expect("zone exists");
        assert_eq!(dg_zu.busy_block_count, zu.busy_block_count);
        assert_eq!(dg_zu.free_block_count, zu.free_block_count);
        assert_eq!(dg_zu.capacity_bytes, u64::from(ZONE_CAP) * u64::from(UNIT_SIZE));
        assert_eq!(
            dg_zu.busy_bytes,
            u64::from(zu.busy_block_count) * u64::from(UNIT_SIZE)
        );
        assert_eq!(
            dg_zu.free_bytes,
            u64::from(zu.free_block_count) * u64::from(UNIT_SIZE)
        );
    }
}

/// Zone shape: out-of-range zone returns `None`.
#[test]
fn zone_shape_out_of_range() {
    let dg = make_dg();
    assert!(dg.zone_usage(disk_id(1), 999).is_none());
    assert!(dg.zone_usage(disk_id(999), 0).is_none());
}

/// Persist-only free: allocate 10, free 4 — `busy_bytes` stays at 10
/// (the bitmap is not cleared; compaction is the sole bit-clearer).
#[test]
fn free_is_persist_only() {
    let dg = make_dg();
    // Allocate 10 units.
    let mut allocs = Vec::new();
    for _ in 0..10 {
        let (disk, zone, range) = dg.allocate_block(1, &[], 100, ZONE_ROTATE).expect("alloc");
        allocs.push((disk.disk_id, zone.zone_index, range.unit_offset, range.unit_count));
    }
    let usage_after_alloc = dg.aggregate_usage();
    assert_eq!(usage_after_alloc.busy_bytes, 10 * u64::from(UNIT_SIZE));

    // Free 4 units — persist-only: bitmap not cleared.
    for (did, zi, off, count) in allocs.iter().take(4) {
        assert!(dg.free_block(did, *zi, *off, *count));
    }
    let usage_after_free = dg.aggregate_usage();
    assert_eq!(usage_after_free.busy_bytes, 10 * u64::from(UNIT_SIZE));
    assert_eq!(
        usage_after_free.free_bytes,
        usage_after_free.capacity_bytes - 10 * u64::from(UNIT_SIZE)
    );
}

/// `get_disk` returns `None` for unknown disk.
#[test]
fn get_disk_unknown_returns_none() {
    let dg = make_dg();
    assert!(dg.get_disk(disk_id(999)).is_none());
    assert!(dg.get_disk(disk_id(1)).is_some());
}

/// Query responses must report current runtime status, not hardware status
/// copied before initialization or a later runtime failure.
#[tokio::test]
async fn disk_rpc_reports_effective_status() {
    use arc_swap::ArcSwap;
    use crowdb_diskdb::ddb_config::{DdbConfig, StorageDefaults};
    use crowdb_diskdb::ddb_kv_client::DdbKvClient;
    use crowdb_diskdb::metrics::{DiskdbMetrics, RecalcEngine};
    use crowdb_diskdb::model::disk_group_container::DdbDiskGroupContainer;
    use crowdb_diskdb::recovery::ZoneLoader;
    use crowdb_diskdb::scanner::ScanState;
    use crowdb_diskdb::service::DiskdbRpcService;
    use crowdb_diskdb_client::rpc_transport::DiskdbRpcTransport;
    use crowdb_kv_client::{ClientConfig, CrowdbKvClient};

    let dg = make_dg();
    let disk = dg.get_disk(disk_id(1)).unwrap();
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    container.replace_disk_group(&dg);
    let kv = Arc::new(DdbKvClient::new(CrowdbKvClient::new(ClientConfig::new(vec![]))));
    let service = Arc::new(DiskdbRpcService::new(
        container.clone(),
        kv.clone(),
        StorageDefaults::default(),
        Arc::new(ZoneLoader::new(kv.clone(), 4)),
        Arc::new(RecalcEngine::new(kv, container)),
        ScanState::new(),
        Arc::new(DiskdbMetrics::disabled()),
        Arc::new(ArcSwap::from_pointee(DdbConfig::default())),
        tokio::runtime::Handle::current(),
    ));
    let port = crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::DiskdbRpc);
    let server = Arc::new(crowdb_rpc_ffi::RpcServer::new(None));
    server.listen("127.0.0.1", i32::from(port)).unwrap();
    service.register_handlers(&server);
    server.start();
    let endpoint = format!("http://127.0.0.1:{port}");
    let transport = DiskdbRpcTransport::new();
    for status in [HwStatus::Init, HwStatus::Offline, HwStatus::Up] {
        disk.set_effective_status(status);
        let response = transport
            .get_disk_info(&endpoint, DG, disk.disk_id)
            .await
            .unwrap();
        assert_eq!(response.disk.unwrap().status, status as i32);
        let response = transport
            .query_capacity_stats(
                &endpoint,
                &crowdb_protocol::diskdb::rpc::QueryCapacityStatsRequest {
                    disk_group_id: DG,
                    disk_id: None,
                    zone_index: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            response.disk_groups[0]
                .disks
                .iter()
                .find(|info| info.disk_id == Some(disk.disk_id))
                .unwrap()
                .status,
            status as i32
        );
    }
    server.stop();
}
