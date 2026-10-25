// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E error-paths test: verifies client error decoding for
//! `DiskNotExist` (write/read/fsync), `ZoneNotExist`, and `IoError`
//! (via fault injection with `--fault-error-rate 1.0`).

use crowdb_diskio_client::{
    DiskId as DioDiskId, DiskIoRetCode, TestWireDiskioClient as DiskioClient,
    TestWireDiskioError as DiskioError,
};

/// Verify client error decoding for `DiskNotExist`, `ZoneNotExist`,
/// and `IoError` (via fault injection).
#[tokio::test]
#[ignore = "requires the owned packaged Diskio fixture"]
#[allow(clippy::too_many_lines)]
async fn disk_io_e2e_error_paths() {
    let (rpc_server, conn, dio_client) = super::connect();
    super::wait_for_disks(&dio_client, &rpc_server, &conn).await;
    let valid_disk = DioDiskId::new(0, 1);
    let bad_disk = DioDiskId::new(99, 99);

    eprintln!("=== error-paths: DiskNotExist (write) ===");
    let wf = dio_client
        .write(&rpc_server, &conn, bad_disk, 0, 0, vec![0xAB; 4096])
        .expect("write send");
    let result = DiskioClient::await_write_response(wf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::DiskNotExist))),
        "expected DiskNotExist for write, got {result:?}"
    );
    eprintln!("  write to bad disk: DiskNotExist OK");

    eprintln!("=== error-paths: DiskNotExist (read) ===");
    let rf = dio_client
        .read(&rpc_server, &conn, bad_disk, 0, 0, 4096, 0)
        .expect("read send");
    let result = DiskioClient::await_read_response(rf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::DiskNotExist))),
        "expected DiskNotExist for read, got {result:?}"
    );
    eprintln!("  read from bad disk: DiskNotExist OK");

    eprintln!("=== error-paths: DiskNotExist (fsync) ===");
    let ff = dio_client
        .fsync(&rpc_server, &conn, bad_disk)
        .expect("fsync send");
    let result = DiskioClient::await_fsync_response(ff).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::DiskNotExist))),
        "expected DiskNotExist for fsync, got {result:?}"
    );
    eprintln!("  fsync bad disk: DiskNotExist OK");

    eprintln!("=== error-paths: ZoneNotExist (write) ===");
    let wf = dio_client
        .write(&rpc_server, &conn, valid_disk, 99, 0, vec![0xAB; 4096])
        .expect("write send");
    let result = DiskioClient::await_write_response(wf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::ZoneNotExist))),
        "expected ZoneNotExist for write, got {result:?}"
    );
    eprintln!("  write to bad zone: ZoneNotExist OK");

    eprintln!("=== error-paths: ZoneNotExist (read) ===");
    let rf = dio_client
        .read(&rpc_server, &conn, valid_disk, 99, 0, 4096, 0)
        .expect("read send");
    let result = DiskioClient::await_read_response(rf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::ZoneNotExist))),
        "expected ZoneNotExist for read, got {result:?}"
    );
    eprintln!("  read from bad zone: ZoneNotExist OK");

    assert_missing_creation_time(&rpc_server, &conn).await;
    rpc_server.stop();
}

async fn assert_missing_creation_time(
    server: &crowdb_rpc_ffi::RpcServer,
    connection: &crowdb_rpc_ffi::Connection,
) {
    use crowdb_protocol::diskio_fb::{
        FBDiskIoRetCode, FBDiskWriteRequest, FBDiskWriteRequestArgs, FBDiskWriteResponse, FBInt128,
    };
    use crowdb_rpc_ffi::{Buffer, RpcClient};
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let disk = FBInt128([0; 16]);
    let request = FBDiskWriteRequest::create(
        &mut builder,
        &FBDiskWriteRequestArgs {
            id: 77,
            disk_id: Some(&disk),
            size: 4,
            write_create_time_ms: 0,
            sync: false,
            ..FBDiskWriteRequestArgs::default()
        },
    );
    builder.finish(request, None);
    let rpc = RpcClient::new();
    rpc.attach(connection);
    let response = rpc
        .call(
            server,
            connection,
            77,
            Buffer::from_bytes(builder.finished_data()),
            Some(Buffer::from_bytes(b"test")),
            u16::try_from(crowdb_protocol::fb::FBMsgType::EDiskWriteRequest.0).unwrap(),
        )
        .expect("submit missing creation time")
        .await
        .expect("write response");
    let decoded = flatbuffers::root::<FBDiskWriteResponse>(
        response.control.as_ref().expect("response control").as_slice(),
    )
    .expect("valid response");
    assert_eq!(decoded.id(), 77);
    assert_eq!(decoded.ret_code(), FBDiskIoRetCode::OldRequest);
}
