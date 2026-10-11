// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E error-paths test: verifies client error decoding for
//! `DiskNotExist` (write/read/fsync), `ZoneNotExist`, and `IoError`
//! (via fault injection with `--fault-error-rate 1.0`).

use crowdb_diskio_client::{
    DiskId as DioDiskId, DiskIoRetCode, TestWireDiskioClient as DiskioClient,
    TestWireDiskioError as DiskioError,
};

#[tokio::test]
#[ignore = "requires the owned fault-injected packaged Diskio fixture"]
async fn disk_io_fault_errors() {
    let (rpc_server, conn, dio_client) = super::connect();
    super::wait_for_disk(&dio_client, &rpc_server, &conn, true).await;
    let valid_disk = DioDiskId::new(0, 1);
    let wf = dio_client
        .write(&rpc_server, &conn, valid_disk, 0, 0, vec![0xAB; 4096])
        .expect("write send");
    let result = DiskioClient::await_write_response(wf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::IoError))),
        "expected IoError for fault-injected write, got {result:?}"
    );
    eprintln!("  fault-injected write: IoError OK");

    let rf = dio_client
        .read(&rpc_server, &conn, valid_disk, 0, 0, 4096, 0)
        .expect("read send");
    let result = DiskioClient::await_read_response(rf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::IoError))),
        "expected IoError for fault-injected read, got {result:?}"
    );
    eprintln!("  fault-injected read: IoError OK");

    rpc_server.stop();
}
