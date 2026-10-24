// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset.rs"]
mod dataset;
#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/storage.rs"]
mod storage;

use crowdb_access_dataset::{OperationId, ReadPartition, ReadSpec, Selection};
use crowdb_sdk_native::{
    dataset::{BatchRequest, FieldState},
    Error,
};
use dataset::{context, TestFixture};
use std::time::Duration;

fn spec() -> ReadSpec {
    ReadSpec {
        snapshot: "latest".into(),
        selection: Selection::Explicit(vec![vec![1], vec![2], vec![3]]),
        fields: vec!["value".into(), "optional".into()],
        batch_size: 1,
        partition: ReadPartition {
            worker: 0,
            workers: 1,
            rank: 0,
            world_size: 1,
        },
        shuffle: None,
        split: None,
    }
}

#[tokio::test]
async fn deletion_rejects_new_requests_without_interrupting_an_admitted_body() {
    use crowdb_access_dataset::{CommitAttempt, CommitTarget};
    let fixture = TestFixture::new(1024).await;
    let batch = || BatchRequest {
        snapshot: fixture.snapshot,
        sample_ids: vec![vec![1]],
        fields: vec!["value".into()],
    };
    let mut old = fixture.client.read_batch(context(2), batch()).await.unwrap();
    fixture
        .client
        .control(context(3), |scope| async move {
            let next = scope
                .authority
                .prepare_snapshot_candidate(&scope.identity, None, dataset::manifest(), OperationId::random())
                .await?;
            scope
                .authority
                .publish_snapshot_candidate(
                    &scope.identity,
                    &CommitAttempt {
                        version: 1,
                        id: OperationId::random(),
                        target: CommitTarget::Snapshot { snapshot: next },
                        expected_head: scope.authority.inspect_head(&scope.identity).await?.generation,
                    },
                )
                .await?;
            scope
                .authority
                .begin_reclaim(&scope.identity, fixture.snapshot)
                .await?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(matches!(
        fixture.client.read_batch(context(4), batch()).await,
        Err(Error::Dataset {
            kind: crowdb_sdk_native::dataset::FailureKind::NotFound,
            ..
        })
    ));
    let held = old.body.next().await.unwrap().unwrap().buffer;
    assert_eq!(held.as_slice(), &[0; 16]);
    assert!(old.body.next().await.unwrap().is_none());
    assert_eq!(held.as_slice(), &[0; 16]);
}

#[tokio::test]
async fn fetching_and_releasing_owners_never_acknowledge_progress() {
    let fixture = TestFixture::new(1024).await;
    let bound = fixture.client.bind_read(context(2), spec()).await.unwrap();
    let cursor = bound.cursor;
    let mut first = fixture.client.next_batch(context(3), &cursor).await.unwrap();
    assert_eq!(first.header.snapshot, fixture.snapshot);
    assert_eq!(first.header.fields[1].state, FieldState::Missing);
    let held = first.body.next().await.unwrap().unwrap().buffer;
    assert_eq!(held.as_slice(), &[0; 16]);
    assert!(first.body.next().await.unwrap().is_none());
    drop(held);
    let mut replay = fixture.client.next_batch(context(5), &cursor).await.unwrap();
    assert_eq!(replay.header.next, first.header.next);
    assert_eq!(
        replay.body.next().await.unwrap().unwrap().buffer.as_slice(),
        &[0; 16]
    );
    assert!(replay.body.next().await.unwrap().is_none());
    let mut second = fixture
        .client
        .next_batch(context(7), first.header.next.as_deref().unwrap())
        .await
        .unwrap();
    assert_eq!(second.header.sample_ids, vec![vec![2]]);
    assert_eq!(second.header.fields[1].state, FieldState::Tombstone);
    let retained = second.body.next().await.unwrap().unwrap().buffer;
    second.body.cancel();
    assert!(second.body.next().await.is_err());
    fixture.runtime.shutdown().await.unwrap();
    assert_eq!(retained.as_slice(), &[1; 16]);
    drop(retained);
    assert_eq!(fixture.runtime.resources().status().held_bytes, 0);
}

#[tokio::test]
async fn dropped_wait_resumes_pending_data_without_copy_or_unbounded_prefetch() {
    let fixture = TestFixture::new(16).await;
    let mut read = fixture
        .client
        .read_batch(
            context(2),
            BatchRequest {
                snapshot: fixture.snapshot,
                sample_ids: vec![vec![1], vec![2]],
                fields: vec!["value".into()],
            },
        )
        .await
        .unwrap();
    let first = read.body.next().await.unwrap().unwrap().buffer;
    assert!(tokio::time::timeout(Duration::from_millis(10), read.body.next())
        .await
        .is_err());
    assert_eq!(fixture.runtime.resources().status().held_bytes, 16);
    assert_eq!(fixture.runtime.resources().status().compatibility_copy_bytes, 0);
    drop(first);
    let second = read.body.next().await.unwrap().unwrap();
    assert_eq!(second.field, 1);
    assert_eq!(second.buffer.as_slice(), &[1; 16]);
    assert!(read.body.next().await.unwrap().is_none());
    assert!(read.body.is_complete());
    assert_eq!(fixture.runtime.resources().status().requests, 0);
}

#[tokio::test]
async fn cancellation_and_runtime_shutdown_preserve_delivered_owners() {
    let fixture = TestFixture::new(32).await;
    let context = context(2);
    let cancel = context.cancellation.clone();
    let mut read = fixture
        .client
        .read_batch(
            context,
            BatchRequest {
                snapshot: fixture.snapshot,
                sample_ids: vec![vec![1], vec![2]],
                fields: vec!["value".into()],
            },
        )
        .await
        .unwrap();
    let held = read.body.next().await.unwrap().unwrap().buffer;
    cancel.cancel();
    assert!(matches!(read.body.next().await, Err(Error::Cancelled)));
    assert!(matches!(read.body.next().await, Err(Error::Cancelled)));
    assert!(!read.body.is_complete());
    fixture.runtime.shutdown().await.unwrap();
    assert_eq!(held.as_slice(), &[0; 16]);
    drop(held);
    assert_eq!(fixture.runtime.resources().status().held_bytes, 0);
}
