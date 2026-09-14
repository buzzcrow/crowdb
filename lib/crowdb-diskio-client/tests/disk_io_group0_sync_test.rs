// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E group-0 sync test: verifies periodic group-0 heartbeat
//! (service registry entry appears) and disk-list reconciliation
//! (new disk added to group-0 after startup becomes writable).

use std::time::{Duration, Instant};

use crowdb_diskio_client::{
    DiskId as DioDiskId, DiskIoRetCode, DiskioClient as SemanticDiskioClient, DiskioClientConfig, Durability,
    SegmentTarget, TestWireDiskioClient as DiskioClient, TestWireDiskioError as DiskioError,
};
use crowdb_protocol::common::HwStatus;
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskio::*;
use crowdb_test_harness::hardware::{
    make_disk_id, seed_hardware, standard_disk_ids_4, CAPACITY_UNITS, DG_ID, INSTANCE_ID, NODE_ID, RACK_ID,
    UNIT_SIZE_BYTES, ZONE_COUNT, ZONE_SIZE_UNITS,
};

/// Verify group-0 periodic sync: (a) heartbeat registers the diskio
/// instance in the service registry, and (b) disk-list reconciliation
/// picks up a new disk added to group-0 after startup.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn disk_io_e2e_group0_sync() {
    if !check_binaries() {
        return;
    }

    eprintln!("=== group0-sync: starting kv cluster ===");
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw, &standard_disk_ids_4()).await;
    eprintln!("hardware metadata seeded (3 initial disks)");

    eprintln!("=== group0-sync: starting diskio ===");
    let diskio = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "mem",
        kv_seeds: &cluster.mgmt_endpoints,
        disks: &[],
        fault_error_rate: 0.0,
        fault_latency_ms: None,
        no_o_direct: false,
    });
    let (rpc_server, conn, dio_client) = connect_to_diskio(&diskio);
    diskio.wait_for_disks(&dio_client, &rpc_server, &conn).await;

    eprintln!("=== group0-sync: verifying heartbeat ===");
    let svc = cluster.make_service_registry_client();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut heartbeat_ok = false;
    while Instant::now() < deadline {
        if let Ok(Some(instance)) = svc.read_instance("diskio", INSTANCE_ID).await {
            eprintln!(
                "  service registry: found diskio instance {} at {}",
                instance.instance_id, instance.rpc_endpoint
            );
            assert!(
                !instance.rpc_endpoint.is_empty(),
                "heartbeat should register a non-empty rpc_endpoint"
            );
            heartbeat_ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        heartbeat_ok,
        "diskio should heartbeat to the service registry within 15s"
    );
    eprintln!("  heartbeat verified");

    let semantic = SemanticDiskioClient::connect_with_clients(
        svc.clone(),
        hw.clone(),
        DiskioClientConfig {
            normal_connections_per_endpoint: 2,
            priority_connections_per_endpoint: 1,
            ..DiskioClientConfig::default()
        },
    )
    .await
    .expect("semantic client should discover authoritative routes");
    assert_eq!(semantic.status().disks, 4);
    for _ in 0..100 {
        semantic
            .refresh()
            .await
            .expect("unchanged semantic route refresh");
    }
    assert_eq!(semantic.status().normal_connections, 2);
    assert_eq!(semantic.status().priority_connections, 1);

    let stable = semantic.status();
    svc.heartbeat_diskio_at(
        INSTANCE_ID + 1,
        "malformed-endpoint",
        RACK_ID,
        NODE_ID,
        &[DG_ID],
        &[],
    )
    .await
    .expect("inject malformed owner observation");
    assert!(semantic.refresh().await.is_err());
    let rejected = semantic.status();
    assert_eq!(rejected.route_generation, stable.route_generation);
    assert_eq!(rejected.disks, stable.disks);
    assert_eq!(rejected.normal_connections, stable.normal_connections);
    assert_eq!(rejected.priority_connections, stable.priority_connections);
    svc.unregister("diskio", INSTANCE_ID + 1)
        .await
        .expect("remove malformed owner observation");

    eprintln!("=== group0-sync: adding new disk to group-0 ===");
    let new_disk_id = make_disk_id(0, 42);
    hw.add_disk(
        RACK_ID,
        NODE_ID,
        DG_ID,
        &new_disk_id,
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
    .expect("add new disk");

    let all_disk_ids = vec![
        make_disk_id(0, 1),
        make_disk_id(0, 2),
        make_disk_id(0, 3),
        make_disk_id(0xAB, 4),
        new_disk_id,
    ];
    hw.add_disk_group(
        RACK_ID,
        NODE_ID,
        DG_ID,
        &DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: all_disk_ids,
        },
    )
    .await
    .expect("update disk-group");
    eprintln!("  added disk {new_disk_id:?} to group-0");

    eprintln!("=== group0-sync: waiting for reconciliation ===");
    let new_disk_dio = DioDiskId::new(0, 42);
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut reconciled = false;
    while Instant::now() < deadline {
        let wf = dio_client
            .write(&rpc_server, &conn, new_disk_dio, 0, 0, vec![0xCD; 4096])
            .expect("reconcile write send");
        match DiskioClient::await_write_response(wf).await {
            Ok(DiskIoRetCode::Success) => {
                reconciled = true;
                break;
            }
            Err(DiskioError::IoError(DiskIoRetCode::DiskNotExist)) => {
                // Not yet reconciled — keep waiting.
            }
            other => {
                eprintln!("  reconcile write unexpected result: {other:?}");
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        reconciled,
        "diskio should reconcile the new disk within 20s. Log:\n{}",
        diskio.log_content()
    );
    eprintln!("  new disk reconciled and writable");

    semantic.refresh().await.expect("refresh semantic routes");
    assert_eq!(semantic.status().disks, 5);
    let semantic_target =
        SegmentTarget::new(new_disk_dio, 0, 0, 1, UNIT_SIZE_BYTES).expect("semantic target");
    semantic
        .write(
            semantic_target,
            8192,
            bytes::Bytes::from_static(b"semantic-group0"),
            Durability::Fsync,
            semantic.normal_options(),
        )
        .await
        .expect("semantic routed durable write");
    let semantic_read = semantic
        .read(semantic_target, 8192, 15, semantic.normal_options())
        .await
        .expect("semantic routed read");
    assert_eq!(semantic_read.as_ref(), b"semantic-group0");

    let rf = dio_client
        .read(&rpc_server, &conn, new_disk_dio, 0, 0, 4096, 0)
        .expect("reconcile read send");
    let (rc, rd) = DiskioClient::await_read_response(rf)
        .await
        .expect("reconcile read IO");
    assert_eq!(rc, DiskIoRetCode::Success, "reconciled disk read should succeed");
    let rd = rd.expect("reconciled read data should be present");
    assert_eq!(rd, vec![0xCD; 4096], "reconciled disk data should match");
    eprintln!("  reconciled disk read-back verified");

    drop(diskio);
    rpc_server.stop();

    eprintln!();
    eprintln!("disk_io_e2e_group0_sync: ALL CHECKS PASSED");
}
