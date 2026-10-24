// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/composition.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/composition_resource.rs"]
mod resource;
#[path = "common/split_store.rs"]
mod split_store;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;
use bytes::Bytes;
use crowdb_access_dataset::{composition::*, split::*, *};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

fn request(parent: SnapshotId) -> CompositionRequest {
    CompositionRequest {
        parent: Some(parent),
        source: CompositionSource::All,
        schema: None,
        samples: vec![SampleChange::Upsert(common::sample(b"new", 9))],
        splits: split_store::partition_splits(),
        policy: SplitPolicy::default(),
    }
}

#[tokio::test]
async fn partial_members_are_invisible_and_retry_recomputes_without_draft_progress() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let parent = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let input = request(parent);
    let snapshot = SnapshotId::random();
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    store.lose_member_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .prepare_composition_resource(&identity, snapshot, &input, None, &ReadCancellation::new())
        .await
        .is_err());
    assert!(matches!(
        authority.get_snapshot(&identity, snapshot).await,
        Err(AuthorityError::SnapshotNotFound)
    ));
    let result = DatasetAuthority::new(store.clone())
        .prepare_composition_resource(&identity, snapshot, &input, None, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(result.sample_count, 46);
    let split = authority
        .get_split_revision(
            &identity,
            snapshot,
            result.initial_split.unwrap(),
            common::budget(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(split.splits.iter().map(|split| split.count).sum::<u64>(), 46);
    assert_eq!(
        authority.inspect_head(&identity).await.unwrap().latest,
        Some(parent)
    );
}

#[tokio::test]
async fn unknown_complete_reply_returns_without_a_confirmation_read() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    store.lose_complete_reply.store(true, Ordering::Relaxed);
    let input = request(parent);
    let snapshot = SnapshotId::random();
    assert!(authority
        .prepare_composition_resource(&identity, snapshot, &input, None, &ReadCancellation::new())
        .await
        .is_err());
    assert!(store.reject_after_unknown.load(Ordering::Relaxed));
    store.reject_after_unknown.store(false, Ordering::Relaxed);
    let before = store.mutations.load(Ordering::Relaxed);
    let result = DatasetAuthority::new(store.clone())
        .prepare_composition_resource(&identity, snapshot, &input, None, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(result.snapshot, snapshot);
    assert_eq!(result.sample_count, 2);
    assert_eq!(store.mutations.load(Ordering::Relaxed), before);
}

#[tokio::test]
async fn final_snapshot_owners_keep_shared_uploads_without_staging_or_cleanup_journals() {
    let (store, authority, identity, _) = split_store::fixture().await;
    let clock = Arc::new(AtomicU64::new(1000));
    let authority = authority.with_reclaim_clock_for_tests(clock.clone());
    let resource::TestUploadedResource {
        uploads,
        location,
        request: input,
    } = resource::TestUploadedResource::new(store.clone(), &identity).await;
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    let mut invalid = input.clone();
    let SampleChange::Upsert(sample) = &mut invalid.samples[0] else {
        panic!("upsert")
    };
    let FieldLocator::Chunk { length, .. } = &mut sample.fields[0].value else {
        panic!("chunk")
    };
    *length = 5;
    let before = store.mutations.load(Ordering::Relaxed);
    assert!(matches!(
        authority
            .prepare_composition_resource(
                &identity,
                SnapshotId::random(),
                &invalid,
                Some(&uploads),
                &ReadCancellation::new()
            )
            .await,
        Err(CompositionError::Upload(UploadError::Conflict))
    ));
    assert_eq!(store.mutations.load(Ordering::Relaxed), before);
    let a = SnapshotId::random();
    let b = SnapshotId::random();
    for snapshot in [a, b] {
        authority
            .prepare_composition_resource(
                &identity,
                snapshot,
                &input,
                Some(&uploads),
                &ReadCancellation::new(),
            )
            .await
            .unwrap();
    }
    assert!(matches!(
        uploads.abort(&location).await,
        Err(UploadError::Conflict)
    ));
    store.reject_workflow_writes.store(false, Ordering::Relaxed);
    for (snapshot, now) in [(a, 1300), (b, 1600)] {
        authority.begin_reclaim(&identity, snapshot).await.unwrap();
        clock.store(now, Ordering::Release);
        let mut complete = false;
        for _ in 0..10 {
            if authority
                .reclaim_snapshot_uploads_step(&identity, snapshot, &uploads)
                .await
                .unwrap()
                .complete
            {
                complete = true;
                break;
            }
        }
        assert!(complete);
        if snapshot == a {
            assert!(matches!(
                uploads.abort(&location).await,
                Err(UploadError::Conflict)
            ));
            assert_eq!(
                uploads.read_part_verified(&location, 0).await.unwrap().0,
                Bytes::from_static(b"data")
            );
            let before = store.mutations.load(Ordering::Relaxed);
            assert!(authority
                .prepare_composition_resource(
                    &identity,
                    snapshot,
                    &input,
                    Some(&uploads),
                    &ReadCancellation::new()
                )
                .await
                .is_err());
            assert_eq!(store.mutations.load(Ordering::Relaxed), before);
        }
    }
    assert!(uploads.abort(&location).await.unwrap().cleanup_complete);
}

#[tokio::test]
async fn unknown_owner_reply_keeps_the_resource_incomplete_until_client_retry() {
    let (store, authority, identity, _) = split_store::fixture().await;
    let resource = resource::TestUploadedResource::new(store.clone(), &identity).await;
    let snapshot = SnapshotId::random();
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    store.lose_claim_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .prepare_composition_resource(
            &identity,
            snapshot,
            &resource.request,
            Some(&resource.uploads),
            &ReadCancellation::new()
        )
        .await
        .is_err());
    assert!(store.reject_after_unknown.load(Ordering::Relaxed));
    store.reject_after_unknown.store(false, Ordering::Relaxed);
    assert!(matches!(
        authority.get_snapshot(&identity, snapshot).await,
        Err(AuthorityError::SnapshotNotFound)
    ));
    let before = store.mutations.load(Ordering::Relaxed);
    let restarted = DatasetAuthority::new(store.clone());
    let result = restarted
        .prepare_composition_resource(
            &identity,
            snapshot,
            &resource.request,
            Some(&resource.uploads),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(store.mutations.load(Ordering::Relaxed) - before, 2);
    assert_eq!(result.snapshot, snapshot);
    assert_eq!(result.sample_count, 1);
    assert_eq!(
        resource
            .uploads
            .read_part_verified(&resource.location, 0)
            .await
            .unwrap()
            .0,
        Bytes::from_static(b"data")
    );
}
