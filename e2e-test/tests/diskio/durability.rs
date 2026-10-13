// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E durability test: write + fsync + process restart + read with
//! `BlockDisk` on a temp file (I2 durability invariant).

use super::connect;
use crowdb_diskio_client::{DiskId as DioDiskId, DiskIoRetCode, TestWireDiskioClient as DiskioClient};

/// Verify I2 (durability): write + fsync + process restart + read
/// returns the same data. Uses `BlockDisk` on a temp file with
/// `--no-o-direct` (so unaligned small writes work).
#[tokio::test]
#[ignore = "requires the owned release-container Diskio fixture"]
#[allow(clippy::too_many_lines)]
async fn disk_io_e2e_durability() {
    let disk_id = DioDiskId::new(0, 1);
    let (rpc_server, conn, dio_client) = connect();
    let writing = std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("write");
    let mut write_data: Vec<u8> = (0..4096u32).map(|i| u8::try_from(i % 256).unwrap()).collect();
    write_data[0] = 0x01;
    write_data[1] = 0x03;
    let unaligned_offset = 25_220;
    let unaligned_data: Vec<u8> = (0..182u16)
        .map(|index| u8::try_from((index * 31) % 251).unwrap())
        .collect();
    if writing {
        let wf = dio_client
            .write(&rpc_server, &conn, disk_id, 0, 0, write_data.clone())
            .expect("write send");
        let wc = DiskioClient::await_write_response(wf).await.expect("write IO");
        assert_eq!(wc, DiskIoRetCode::Success, "durability write should succeed");

        // Stream journal frames commonly begin at an unaligned byte offset and
        // are shorter than one physical sector. Keep this case distinct from the
        // aligned smoke write above: it exercises AlignedWriter's read-modify-
        // write path across a process restart.
        let wf = dio_client
            .write(
                &rpc_server,
                &conn,
                disk_id,
                0,
                unaligned_offset,
                unaligned_data.clone(),
            )
            .expect("unaligned write send");
        let wc = DiskioClient::await_write_response(wf)
            .await
            .expect("unaligned write IO");
        assert_eq!(
            wc,
            DiskIoRetCode::Success,
            "unaligned durability write should succeed"
        );

        let ff = dio_client.fsync(&rpc_server, &conn, disk_id).expect("fsync send");
        let fc = DiskioClient::await_fsync_response(ff).await.expect("fsync IO");
        assert_eq!(fc, DiskIoRetCode::Success, "durability fsync should succeed");
        eprintln!("  wrote + fsync'd 4 KB");

        rpc_server.stop();
        return;
    }
    let fragment = dio_client
        .read(&rpc_server, &conn, disk_id, 0, 2048, 238, 0)
        .expect("fragment read send");
    let (code, fragment) = DiskioClient::await_read_response(fragment)
        .await
        .expect("fragment read IO");
    assert_eq!(code, DiskIoRetCode::Success);
    assert_eq!(fragment.as_deref(), Some(&write_data[2048..2286]));
    let rf = dio_client
        .read(&rpc_server, &conn, disk_id, 0, 0, 4096, 0)
        .expect("read send");
    let (rc, rd) = DiskioClient::await_read_response(rf).await.expect("read IO");
    assert_eq!(rc, DiskIoRetCode::Success, "durability read should succeed");
    let rd = rd.expect("read data should be present");
    assert_eq!(rd.len(), 4096, "durability read length mismatch");
    assert_eq!(
        rd, write_data,
        "durability: read after restart must match written data"
    );
    let rf = dio_client
        .read(
            &rpc_server,
            &conn,
            disk_id,
            0,
            unaligned_offset,
            u32::try_from(unaligned_data.len()).expect("unaligned length fits u32"),
            0,
        )
        .expect("unaligned read send");
    let (rc, rd) = DiskioClient::await_read_response(rf)
        .await
        .expect("unaligned read IO");
    assert_eq!(
        rc,
        DiskIoRetCode::Success,
        "unaligned durability read should succeed"
    );
    assert_eq!(rd.as_deref(), Some(unaligned_data.as_slice()));
    eprintln!("  read after restart: data matches");

    rpc_server.stop();
}
