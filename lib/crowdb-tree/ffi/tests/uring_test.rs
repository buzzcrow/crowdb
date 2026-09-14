// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::io::{IoSlice, Read, Seek, SeekFrom};

use crowdb_tree_ffi::Uring;

#[tokio::test]
async fn standalone_uring_vectored_write_read_and_sync() {
    let Ok(ring) = Uring::new(32) else {
        return;
    };
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("ffi-uring");
    let path = dir.path().join("wal-segment");
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create_new(true);
    let file = ring.open(&path, &options).expect("register buffered file");

    let first = b"crowdb-";
    let second = b"uring";
    let slices = [IoSlice::new(first), IoSlice::new(second)];
    assert_eq!(
        file.write_vectored_at(&slices, 7).await.expect("writev"),
        first.len() + second.len()
    );
    file.sync_data().await.expect("data sync");
    file.sync_all().await.expect("full sync");

    let mut read = vec![0; first.len() + second.len()];
    assert_eq!(file.read_at(&mut read, 7).await.expect("read"), read.len());
    assert_eq!(read, b"crowdb-uring");

    drop(file);
    let mut persisted = std::fs::File::open(path).expect("reopen");
    persisted.seek(SeekFrom::Start(7)).expect("seek");
    let mut bytes = Vec::new();
    persisted.read_to_end(&mut bytes).expect("read persisted");
    assert_eq!(bytes, b"crowdb-uring");
}

#[tokio::test]
async fn dropping_pending_operation_preserves_buffer_lifetime() {
    let Ok(ring) = Uring::new(2) else {
        return;
    };
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("ffi-uring-cancel");
    let path = dir.path().join("wal-segment");
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create_new(true);
    let file = ring.open(&path, &options).expect("register buffered file");
    let payload = vec![0x5a; 1024 * 1024];
    let slice = [IoSlice::new(&payload)];

    {
        let operation = file.write_vectored_at(&slice, 0);
        tokio::pin!(operation);
        tokio::select! {
            result = &mut operation => { result.expect("write completion"); }
            () = tokio::task::yield_now() => {}
        }
    }
    drop(file);
}
