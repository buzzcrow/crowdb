// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/storage.rs"]
mod runtime_storage;
#[path = "common/read_storage.rs"]
mod storage;

use crowdb_access_s3::metadata::TenantId;
use crowdb_access_s3::S3ErrorCode;
use crowdb_sdk_native::s3::{DeleteObjectFailure, ListOptions, S3Client};
use crowdb_sdk_native::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use storage::{context, TestFixture};

async fn populate(fixture: &TestFixture, keys: &[&[u8]]) {
    for key in keys {
        let mut record = fixture.record(0);
        record.key = key.to_vec();
        fixture.publish(&record).await;
    }
}

#[tokio::test]
async fn native_listing_pages_share_scoped_cursors_across_runtime_handles() {
    let fixture = TestFixture::new(8192).await;
    populate(&fixture, &[b"a/one", b"a/two", b"z"]).await;
    let options = ListOptions {
        limit: 1,
        ..ListOptions::default()
    };
    let first = fixture
        .client
        .list_objects(context(2), b"bucket", &options)
        .await
        .unwrap();
    assert_eq!(first.objects[0].key, b"a/one");
    let continuation = first.next_continuation.unwrap();
    let sibling = S3Client::new(fixture.runtime.clone(), fixture.tenant.clone()).unwrap();
    let second = sibling
        .list_objects(
            context(3),
            b"bucket",
            &ListOptions {
                continuation: Some(continuation.clone()),
                ..options.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(second.objects[0].key, b"a/two");
    assert!(matches!(
        sibling
            .list_objects(
                context(4),
                b"bucket",
                &ListOptions {
                    prefix: b"z".to_vec(),
                    continuation: Some(continuation),
                    ..options
                }
            )
            .await,
        Err(Error::S3(S3ErrorCode::InvalidRequest))
    ));
    let grouped = sibling
        .list_objects(
            context(5),
            b"bucket",
            &ListOptions {
                delimiter: Some(b"/".to_vec()),
                ..ListOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(grouped.common_prefixes, vec![b"a/".to_vec()]);
    assert_eq!(grouped.objects[0].key, b"z");
    assert_eq!(fixture.backend.reads.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn native_batch_delete_is_lazy_ordered_and_dropping_it_keeps_the_tail() {
    let fixture = TestFixture::new(8192).await;
    populate(&fixture, &[b"a", b"b", b"c"]).await;
    let mut deletion = fixture
        .client
        .delete_objects(
            context(2),
            b"bucket",
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        )
        .await
        .unwrap();
    fixture
        .client
        .resolve_object(context(3), b"bucket", b"a")
        .await
        .unwrap();
    let (key, result) = deletion.next().await.unwrap().unwrap();
    assert_eq!(key, b"a");
    assert!(result.is_ok());
    drop(deletion);
    fixture
        .client
        .resolve_object(context(4), b"bucket", b"b")
        .await
        .unwrap();
    let mut duplicate = fixture
        .client
        .delete_objects(
            context(5),
            b"bucket",
            vec![b"a".to_vec(), b"a".to_vec(), b"c".to_vec()],
        )
        .await
        .unwrap();
    for key in [b"a", b"a", b"c"] {
        let result = duplicate.next().await.unwrap().unwrap();
        assert_eq!(result.0, key);
        assert!(result.1.is_ok());
    }
    assert!(duplicate.next().await.unwrap().is_none());
    assert!(matches!(
        fixture
            .client
            .delete_objects(context(6), b"bucket", vec![b"b".to_vec(), Vec::new()])
            .await,
        Err(Error::S3(S3ErrorCode::InvalidRequest))
    ));
    fixture
        .client
        .resolve_object(context(7), b"bucket", b"b")
        .await
        .unwrap();
    assert_eq!(fixture.runtime.resources().status().requests, 0);
}

#[tokio::test]
async fn native_batch_cancellation_stops_before_the_next_key_and_remains_terminal() {
    let fixture = TestFixture::new(8192).await;
    populate(&fixture, &[b"a", b"b"]).await;
    let request = context(2);
    let mut deletion = fixture
        .client
        .delete_objects(request.clone(), b"bucket", vec![b"a".to_vec(), b"b".to_vec()])
        .await
        .unwrap();
    deletion.next().await.unwrap().unwrap().1.unwrap();
    request.cancellation.cancel();
    assert!(matches!(deletion.next().await, Err(Error::Cancelled)));
    assert!(matches!(deletion.next().await, Err(Error::Cancelled)));
    assert!(deletion.pending_key().is_none());
    fixture
        .client
        .resolve_object(context(3), b"bucket", b"b")
        .await
        .unwrap();
}

#[tokio::test]
async fn lost_delete_replies_remain_uncertain_in_single_and_batch_native_results() {
    let loss = Arc::new(AtomicBool::new(false));
    let metadata = metadata::client_with_loss(loss.clone()).await;
    let runtime = runtime_storage::runtime_with_metadata(metadata);
    let client = S3Client::new(runtime, TenantId::new(b"tenant".to_vec()).unwrap()).unwrap();
    client.create_bucket(context(1), b"bucket").await.unwrap();
    loss.store(true, Ordering::Release);
    assert!(matches!(
        client.delete_object(context(2), b"bucket", b"missing").await,
        Err(Error::UncertainMutation {
            reason: crowdb_sdk_native::error::MutationInterruption::Transport,
            ..
        })
    ));
    let mut batch = client
        .delete_objects(context(3), b"bucket", vec![b"missing".to_vec()])
        .await
        .unwrap();
    let result = batch.next().await.unwrap().unwrap();
    assert_eq!(result.1, Err(DeleteObjectFailure::Uncertain));
    assert!(batch.next().await.unwrap().is_none());
}
