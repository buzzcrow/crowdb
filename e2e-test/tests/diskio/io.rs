// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E smoke test for both backends (`MemDisk` data integrity,
//! `NullDisk` pattern verification) across multiple IO sizes (including
//! 2 MB max and 0-byte boundary), plus read-before-write, overwrite,
//! non-zero `zone_offset`, non-zero `DiskId.high`, and a concurrent
//! benchmark with content verification.

use super::concurrency::{run_concurrent_benchmark, IO_SIZES};
use super::io_round::{test_io_round, test_read_before_write, Backend, IoRoundParams};
use crowdb_diskio_client::DiskId as DioDiskId;

#[tokio::test]
#[ignore = "requires the owned packaged Diskio fixture"]
async fn mem_disk_full_flow() {
    full_flow(Backend::Mem).await;
}

#[tokio::test]
#[ignore = "requires the owned packaged Diskio fixture"]
async fn null_disk_full_flow() {
    full_flow(Backend::Null).await;
}

async fn full_flow(backend: Backend) {
    let (rpc_server, conn, dio_client) = super::connect();
    super::wait_for_disks(&dio_client, &rpc_server, &conn).await;
    let disk2 = DioDiskId::new(0, 2);
    test_read_before_write(&dio_client, &rpc_server, &conn, backend, disk2).await;

    let disk1 = DioDiskId::new(0, 1);
    for &(size, label) in IO_SIZES {
        test_io_round(
            &dio_client,
            &rpc_server,
            &conn,
            &IoRoundParams {
                backend,
                disk_id: disk1,
                zone_index: 0,
                zone_offset: 0,
                size,
                label,
            },
        )
        .await;
    }

    test_io_round(
        &dio_client,
        &rpc_server,
        &conn,
        &IoRoundParams {
            backend,
            disk_id: disk2,
            zone_index: 1,
            zone_offset: 0,
            size: 4096,
            label: "middle-disk2",
        },
    )
    .await;

    test_io_round(
        &dio_client,
        &rpc_server,
        &conn,
        &IoRoundParams {
            backend,
            disk_id: disk1,
            zone_index: 0,
            zone_offset: 8192,
            size: 4096,
            label: "middle-nz-offset",
        },
    )
    .await;

    let disk_hi = DioDiskId::new(0xAB, 4);
    test_io_round(
        &dio_client,
        &rpc_server,
        &conn,
        &IoRoundParams {
            backend,
            disk_id: disk_hi,
            zone_index: 0,
            zone_offset: 0,
            size: 4096,
            label: "middle-high-disk",
        },
    )
    .await;

    eprintln!("=== {} concurrent benchmark ===", backend.name());
    run_concurrent_benchmark(&dio_client, &rpc_server, &conn, backend, disk1).await;

    rpc_server.stop();
}
