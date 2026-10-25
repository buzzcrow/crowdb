// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_inline_digest.rs"]
mod dataset_inline_digest;
#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/storage.rs"]
mod storage;

use crowdb_sdk_native::{dataset::FailureKind, Error, OperationContext};
use dataset_inline_digest::{context, TestInlineFixture};
use md5::{Digest, Md5};
use std::{
    num::NonZeroU64,
    time::{Duration, Instant},
};

#[tokio::test]
async fn inline_body_reuses_the_verified_digest_in_both_entrypoints() {
    for query in [false, true] {
        for length in [0, 1, 4096, 64 * 1024] {
            let fixture = TestInlineFixture::new(length).await;
            let mut read = fixture.read(query, context(2)).await.unwrap();
            assert_eq!(read.header.fields[0].md5, Some(Md5::digest(&fixture.data).into()));
            let before = fixture.counts();
            let held = read.body.next().await.unwrap();
            if length == 0 {
                assert!(held.is_none());
            } else {
                let held = held.unwrap().buffer;
                assert_eq!(held.as_slice(), fixture.data);
                assert_eq!(read.body.has_incremental_field_digest_for_tests(), Some(false));
                assert!(read.body.next().await.unwrap().is_none());
                assert_eq!(fixture.runtime.resources().status().requests, 0);
                assert!(fixture.runtime.resources().status().held_bytes >= length);
                fixture.runtime.shutdown().await.unwrap();
                assert_eq!(held.as_slice(), fixture.data);
                drop(held);
            }
            assert_eq!(fixture.counts(), before);
            assert!(read.body.is_complete());
            assert_eq!(fixture.runtime.resources().status().requests, 0);
            assert_eq!(fixture.runtime.resources().status().held_bytes, 0);
            assert_eq!(fixture.runtime.resources().status().compatibility_copy_bytes, 0);
            fixture.runtime.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn damaged_inline_content_or_binding_fails_before_a_body_is_returned() {
    for query in [false, true] {
        for length in [0, 1, 4096, 64 * 1024] {
            for fault in 0..4 {
                let fixture = TestInlineFixture::new(length).await;
                fixture.corrupt(fault).await;
                let writes = fixture.counts().2;
                let result = fixture.read(query, context(2)).await;
                let expected = if fault == 0 {
                    FailureKind::Invalid
                } else {
                    FailureKind::Integrity
                };
                assert!(
                    matches!(result, Err(Error::Dataset { kind, .. }) if kind == expected),
                    "query={query} length={length} fault={fault}: wrong corruption outcome"
                );
                assert_eq!(fixture.counts().2, writes);
                assert_eq!(fixture.runtime.resources().status().requests, 0);
                assert_eq!(fixture.runtime.resources().status().held_bytes, 0);
                fixture.runtime.shutdown().await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn cancelled_or_shutdown_inline_pulls_stay_sticky_and_keep_delivered_owners() {
    for query in [false, true] {
        for length in [0, 1] {
            for after_delivery in [false, true] {
                for shutdown in [false, true] {
                    if length == 0 && after_delivery {
                        continue;
                    }
                    let fixture = TestInlineFixture::new(length).await;
                    let request = context(2);
                    let cancellation = request.cancellation.clone();
                    let mut read = fixture.read(query, request).await.unwrap();
                    let held = if after_delivery {
                        Some(read.body.next().await.unwrap().unwrap().buffer)
                    } else {
                        None
                    };
                    let before = fixture.counts();
                    if shutdown {
                        fixture.runtime.shutdown().await.unwrap();
                    } else {
                        cancellation.cancel();
                    }
                    for _ in 0..2 {
                        let result = read.body.next().await;
                        assert!(if shutdown {
                            matches!(result, Err(Error::Shutdown))
                        } else {
                            matches!(result, Err(Error::Cancelled))
                        });
                    }
                    assert!(!read.body.is_complete());
                    assert_eq!(fixture.counts(), before);
                    assert_eq!(fixture.runtime.resources().status().requests, 0);
                    if let Some(held) = held {
                        assert_eq!(held.as_slice(), fixture.data);
                        drop(held);
                    }
                    assert_eq!(fixture.runtime.resources().status().held_bytes, 0);
                    fixture.runtime.shutdown().await.unwrap();
                }
            }
        }
    }
}

#[tokio::test]
async fn expired_inline_pulls_reject_empty_eof_and_preserve_delivered_owners() {
    for query in [false, true] {
        for after_delivery in [false, true] {
            let fixture = TestInlineFixture::new(usize::from(after_delivery)).await;
            let request = OperationContext::new(
                NonZeroU64::new(2).unwrap(),
                Instant::now() + Duration::from_secs(2),
            );
            let deadline = request.deadline;
            let mut read = fixture.read(query, request).await.unwrap();
            let held = if after_delivery {
                Some(read.body.next().await.unwrap().unwrap().buffer)
            } else {
                None
            };
            let before = fixture.counts();
            tokio::time::sleep_until(deadline.into()).await;
            for _ in 0..2 {
                assert!(matches!(read.body.next().await, Err(Error::Deadline)));
            }
            assert!(!read.body.is_complete());
            assert_eq!(fixture.counts(), before);
            assert_eq!(fixture.runtime.resources().status().requests, 0);
            if let Some(held) = held {
                assert_eq!(held.as_slice(), fixture.data);
                drop(held);
            }
            assert_eq!(fixture.runtime.resources().status().held_bytes, 0);
            fixture.runtime.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn simultaneous_shutdown_and_cancellation_win_before_first_data_and_eof() {
    for query in [false, true] {
        for (length, after_delivery) in [(0, false), (1, false), (1, true)] {
            let fixture = TestInlineFixture::new(length).await;
            let request = context(2);
            let cancellation = request.cancellation.clone();
            let mut read = fixture.read(query, request).await.unwrap();
            let held = if after_delivery {
                Some(read.body.next().await.unwrap().unwrap().buffer)
            } else {
                None
            };
            let before = fixture.counts();
            cancellation.cancel();
            fixture.runtime.shutdown().await.unwrap();
            assert_eq!(
                read.body
                    .next()
                    .await
                    .err()
                    .expect("shutdown must reject the pull"),
                Error::Shutdown,
                "query={query} length={length} after_delivery={after_delivery}"
            );
            read.body.cancel();
            assert_eq!(read.body.next().await.err().unwrap(), Error::Shutdown);
            assert!(!read.body.is_complete());
            assert_eq!(fixture.counts(), before);
            assert_eq!(fixture.runtime.resources().status().requests, 0);
            if let Some(held) = held {
                assert_eq!(held.as_slice(), fixture.data);
                drop(held);
            }
            assert_eq!(fixture.runtime.resources().status().held_bytes, 0);
        }
    }
}
