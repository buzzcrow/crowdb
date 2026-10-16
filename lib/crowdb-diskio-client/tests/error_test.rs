// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_diskio_client::{DiskIoRetCode, DiskioClient, DiskioError, TestWireDiskioError};

#[test]
fn a_missing_local_descriptor_is_distinct_from_io_failure_without_enabling_general_read_retries() {
    let absent = DiskioClient::classify_read_error_for_tests(TestWireDiskioError::IoError(
        DiskIoRetCode::DiskNotExist,
    ));
    assert_eq!(absent, DiskioError::DiskNotFound);
    assert!(!absent.is_retryable_read());
    let failed =
        DiskioClient::classify_read_error_for_tests(TestWireDiskioError::IoError(DiskIoRetCode::IoError));
    assert!(matches!(failed, DiskioError::DiskFailure(_)));
    assert!(!failed.is_retryable_read());
    let invalid = DiskioClient::classify_read_error_for_tests(TestWireDiskioError::IoError(
        DiskIoRetCode::InvalidAlignment,
    ));
    assert!(matches!(invalid, DiskioError::InvalidInput(_)));
}
