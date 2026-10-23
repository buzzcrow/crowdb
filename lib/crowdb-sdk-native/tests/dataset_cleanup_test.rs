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

use crowdb_sdk_native::{CapacityKind, Error};
use read_cleanup::TestCleanupFixture;
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test]
async fn eof_keeps_admission_until_cleanup_and_dropped_wait_does_not_repeat_release() {
    let mut fixture = TestCleanupFixture::new().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), fixture.body.next())
            .await
            .is_err()
    );
    fixture.gate.entered.acquire().await.unwrap().forget();
    assert!(!fixture.body.is_complete());
    assert_eq!(fixture.dataset.runtime.resources().status().requests, 1);
    assert!(matches!(
        fixture.dataset.runtime.resources().admit(dataset::context(3)),
        Err(Error::Capacity(CapacityKind::Requests))
    ));
    fixture.gate.permits.add_permits(1);
    assert!(fixture.body.next().await.unwrap().is_none());
    assert!(fixture.body.next().await.unwrap().is_none());
    assert!(fixture.body.is_complete());
    assert_eq!(fixture.gate.calls.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.dataset.runtime.resources().status().requests, 0);
    assert_eq!(fixture.held.as_slice(), &[0; 16]);
}

#[tokio::test]
async fn cancelled_or_dropped_reads_keep_cleanup_charged_and_delivered_bytes_valid() {
    for cancel in [false, true] {
        let mut fixture = TestCleanupFixture::new().await;
        if cancel {
            fixture.body.cancel();
            assert!(matches!(fixture.body.next().await, Err(Error::Cancelled)));
        }
        drop(fixture.body);
        fixture.gate.entered.acquire().await.unwrap().forget();
        assert_eq!(fixture.dataset.runtime.resources().status().requests, 1);
        assert!(matches!(
            fixture.dataset.runtime.resources().admit(dataset::context(3)),
            Err(Error::Capacity(CapacityKind::Requests))
        ));
        fixture.gate.permits.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), async {
            while fixture.dataset.runtime.resources().status().requests != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(fixture.gate.calls.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.held.as_slice(), &[0; 16]);
    }
}

#[tokio::test(start_paused = true)]
async fn stalled_cleanup_releases_admission_at_the_existing_bound() {
    let fixture = TestCleanupFixture::new().await;
    drop(fixture.body);
    fixture.gate.entered.acquire().await.unwrap().forget();
    assert_eq!(fixture.dataset.runtime.resources().status().requests, 1);
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert_eq!(fixture.dataset.runtime.resources().status().requests, 0);
    assert_eq!(fixture.gate.calls.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.held.as_slice(), &[0; 16]);
}
