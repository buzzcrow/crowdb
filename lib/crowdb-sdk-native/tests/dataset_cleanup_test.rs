// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset.rs"]
mod dataset;
#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/read_cleanup.rs"]
mod read_cleanup;
#[path = "common/storage.rs"]
mod storage;

use crowdb_sdk_native::Error;
use read_cleanup::TestCleanupFixture;
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test]
async fn reading_and_eof_need_no_metadata_mutations_or_cleanup_wait() {
    let mut fixture = tokio::time::timeout(Duration::from_secs(2), TestCleanupFixture::new())
        .await
        .unwrap();
    assert_eq!(fixture.dataset.runtime.resources().status().requests, 1);
    assert!(tokio::time::timeout(Duration::from_secs(1), fixture.body.next())
        .await
        .unwrap()
        .unwrap()
        .is_none());
    assert!(fixture.body.is_complete());
    assert_eq!(fixture.gate.calls.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.dataset.runtime.resources().status().requests, 0);
    assert_eq!(fixture.held.as_slice(), &[0; 16]);
}

#[tokio::test]
async fn cancelled_or_dropped_reads_release_admission_without_writes_and_keep_owned_bytes() {
    for cancel in [false, true] {
        let mut fixture = tokio::time::timeout(Duration::from_secs(2), TestCleanupFixture::new())
            .await
            .unwrap();
        if cancel {
            fixture.body.cancel();
            assert!(matches!(fixture.body.next().await, Err(Error::Cancelled)));
        }
        drop(fixture.body);
        assert_eq!(fixture.dataset.runtime.resources().status().requests, 0);
        assert_eq!(fixture.gate.calls.load(Ordering::Relaxed), 0);
        assert_eq!(fixture.held.as_slice(), &[0; 16]);
    }
}

#[tokio::test(start_paused = true)]
async fn idle_read_has_no_background_lease_renewals() {
    let fixture = TestCleanupFixture::new().await;
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::task::yield_now().await;
    assert_eq!(fixture.gate.calls.load(Ordering::Relaxed), 0);
    drop(fixture.body);
    assert_eq!(fixture.dataset.runtime.resources().status().requests, 0);
    assert_eq!(fixture.held.as_slice(), &[0; 16]);
}
