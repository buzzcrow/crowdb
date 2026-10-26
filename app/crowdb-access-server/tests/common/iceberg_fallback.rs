// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::body::{body, TestWriter};
use crowdb_access_iceberg::{
    catalog::CatalogContext,
    file::{FileGrant, FileOperation, FileOperations, TableLocation},
    key::{CatalogId, OperationId, TableId},
};
use crowdb_access_server::iceberg::{
    write_iceberg_fallback_body_for_tests, FileRequest, FileServiceLimits, FileTransferAdmission,
    UploadFlowSnapshot,
};
use hyper::{body::Bytes, Method};
use md5::{Digest, Md5};
use sha2::Sha256;

pub const TARGET: usize = 16 * crowdb_protocol::frame::MAX_FRAME_V2_PAYLOAD_BYTES;

pub fn admission(limit: u64) -> FileTransferAdmission {
    let grant = FileGrant {
        storage_namespace: None,
        context: CatalogContext {
            catalog: CatalogId::random(),
            activation_epoch: 1,
        },
        table: TableId::random(),
        principal: [5; 32],
        nonce: OperationId::random(),
        issued_ms: 100,
        expires_ms: 200,
        operations: FileOperations::new(&[FileOperation::Put]).unwrap(),
        max_request_bytes: limit,
        max_file_bytes: limit,
    };
    let location = TableLocation {
        storage_namespace: None,
        catalog: grant.context.catalog,
        table: grant.table,
    }
    .file("data/fallback.parquet")
    .unwrap();
    let uri = format!("/{}/{}", location.table().bucket(), location.object_key())
        .parse()
        .unwrap();
    let request = FileRequest::parse(&Method::PUT, &uri).unwrap();
    let limits = FileServiceLimits {
        max_request_bytes: limit,
        max_file_bytes: limit,
        max_part_bytes: limit,
        max_staged_bytes: limit,
    };
    FileTransferAdmission::authorize(&grant, &request, limits, None, 101).unwrap()
}

pub async fn checked(frames: Vec<Bytes>, declared: Option<usize>) -> (TestWriter, UploadFlowSnapshot) {
    let expected: Vec<_> = frames.iter().flat_map(|bytes| bytes.iter().copied()).collect();
    let expected_md5: [u8; 16] = Md5::digest(&expected).into();
    let sha: [u8; 32] = Sha256::digest(&expected).into();
    let mut body = body(frames, declared);
    let mut writer = TestWriter::default();
    let (length, md5, metrics) =
        write_iceberg_fallback_body_for_tests(&mut body, &mut writer, &admission(64 << 20), 2, Some(sha))
            .await
            .unwrap();
    assert_eq!(length, expected.len() as u64);
    assert_eq!(md5, expected_md5);
    assert_eq!(metrics.logical_bytes, length);
    assert_eq!(metrics.digest_enqueues, writer.buffers.len() as u64);
    assert_eq!(metrics.writer_feeds, writer.buffers.len() as u64);
    assert_eq!(metrics.frames_prepared, 0);
    assert!(metrics.queued_buffers_peak <= 2);
    let actual: Vec<_> = writer
        .buffers
        .iter()
        .flat_map(|bytes| bytes.iter().copied())
        .collect();
    assert_eq!(actual, expected);
    (writer, metrics)
}
