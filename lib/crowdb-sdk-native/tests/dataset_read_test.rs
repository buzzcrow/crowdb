// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset.rs"]
mod dataset;
#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/storage.rs"]
mod storage;

use crowdb_access_dataset::{OperationId, Selection, SessionPartition, SessionSpec, SessionSpecV2};
use crowdb_sdk_native::{
    dataset::{BatchRequest, FieldState},
    Error,
};
use dataset::{context, TestFixture};
use std::time::Duration;

fn spec() -> SessionSpecV2 {
    SessionSpecV2 {
        read: SessionSpec {
            id: OperationId::random(),
            snapshot: "latest".into(),
            selection: Selection::Explicit(vec![vec![1], vec![2], vec![3]]),
            fields: vec!["value".into(), "optional".into()],
            batch_size: 1,
            partition: SessionPartition {
                worker: 0,
                workers: 1,
                rank: 0,
                world_size: 1,
            },
            shuffle: None,
        },
        split: None,
    }
}

#[tokio::test]
async fn fetching_and_releasing_owners_never_acknowledge_progress() {
    let fixture = TestFixture::new(1024).await;
    let spec = spec();
    let mut preparation = fixture.client.prepare_read(context(2), &spec, 0).await.unwrap();
    for _ in 0..16 {
        if preparation.complete {
            break;
        }
        preparation = fixture
            .client
            .prepare_read(context(2), &spec, preparation.next_page)
            .await
            .unwrap();
    }
    assert!(preparation.complete);
    let cursor = preparation.session.unwrap().acknowledged;
    let mut first = fixture.client.next_batch(context(3), spec.read.id).await.unwrap();
    assert_eq!(first.header.snapshot, fixture.snapshot);
    assert_eq!(first.header.fields[1].state, FieldState::Missing);
    let held = first.body.next().await.unwrap().unwrap().buffer;
    assert_eq!(held.as_slice(), &[0; 16]);
    assert!(first.body.next().await.unwrap().is_none());
    drop(held);
    assert_eq!(
        fixture
            .client
            .resume_read(context(4), spec.read.id)
            .await
            .unwrap()
            .acknowledged,
        cursor
    );
    let mut replay = fixture.client.next_batch(context(5), spec.read.id).await.unwrap();
    assert_eq!(replay.header.next, first.header.next);
    assert_eq!(
        replay.body.next().await.unwrap().unwrap().buffer.as_slice(),
        &[0; 16]
    );
    assert!(replay.body.next().await.unwrap().is_none());
    fixture
        .client
        .acknowledge(
            context(6),
            spec.read.id,
            &cursor,
            first.header.next.as_ref().unwrap(),
        )
        .await
        .unwrap();
    let mut second = fixture.client.next_batch(context(7), spec.read.id).await.unwrap();
    assert_eq!(second.header.sample_ids, vec![vec![2]]);
    assert_eq!(second.header.fields[1].state, FieldState::Tombstone);
    let retained = second.body.next().await.unwrap().unwrap().buffer;
    fixture.client.close_read(context(8), spec.read.id).await.unwrap();
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
