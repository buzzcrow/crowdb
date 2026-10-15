// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/split_session.rs"]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::*;
use std::sync::Arc;

#[tokio::test]
async fn split_session_pins_both_aliases_and_replays_only_acknowledged_progress() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let snapshot = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let first = split_store::prepared(&authority, &identity, snapshot, None).await;
    split_store::publish_revision(&authority, &identity, &first).await;
    let authority = Arc::new(authority);
    let sessions = DatasetSessions::new(authority.clone());
    let spec = common::spec("train");
    let started = sessions
        .prepare_split_read(&identity, &spec, 0, 10, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(!started.complete);
    assert_eq!(started.split.unwrap().revision, first.revision);
    let next = split_store::prepared(&authority, &identity, snapshot, Some(first.revision)).await;
    split_store::publish_revision(&authority, &identity, &next).await;
    authority
        .publish_snapshot(&identity, Some(snapshot), publication::publication_manifest(2))
        .await
        .unwrap();
    let opened = common::prepare(&sessions, &identity, &spec).await;
    assert_eq!(opened.snapshot, snapshot);
    assert_eq!(opened.split.unwrap().revision, first.revision);
    let session = opened.session.unwrap();
    let batch = sessions.next(&identity, spec.read.id, 11).await.unwrap();
    let restarted = DatasetSessions::new(authority);
    assert_eq!(
        restarted
            .resume(&identity, spec.read.id, 12)
            .await
            .unwrap()
            .acknowledged,
        session.acknowledged
    );
    assert_eq!(
        restarted.next(&identity, spec.read.id, 13).await.unwrap().ids,
        batch.ids
    );
    let ids = common::drain(&restarted, &identity, session, spec.read.id).await;
    assert_eq!(
        ids.len() as u64,
        first
            .splits
            .iter()
            .find(|s| s.name.as_str() == "train")
            .unwrap()
            .count
    );
    let changed = SessionSpecV2 {
        split: Some(SessionSplit {
            revision: Some(next.revision),
            name: split::SplitName::new("train").unwrap(),
        }),
        ..spec
    };
    assert!(matches!(
        restarted
            .prepare_split_read(&identity, &changed, 0, 14, &ReadCancellation::new())
            .await,
        Err(AuthorityError::CursorConflict)
    ));
}

#[tokio::test]
async fn split_read_filter_and_workers_partition_only_admitted_members() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let snapshot = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let revision = split_store::prepared(&authority, &identity, snapshot, None).await;
    split_store::publish_revision(&authority, &identity, &revision).await;
    let sessions = DatasetSessions::new(Arc::new(authority));
    let mut all = std::collections::BTreeSet::new();
    let mut total = 0;
    for worker in 0..2 {
        let mut spec = common::spec("train");
        spec.read.selection = Selection::Range {
            start: vec![10],
            end: vec![30],
        };
        spec.read.partition.workers = 2;
        spec.read.partition.worker = worker;
        spec.read.shuffle = Some(SessionShuffle {
            seed: 42,
            group_size: 8,
        });
        let prepared = common::prepare(&sessions, &identity, &spec).await;
        for id in common::drain(&sessions, &identity, prepared.session.unwrap(), spec.read.id).await {
            assert!((10..30).contains(&id[0]));
            assert!(all.insert(id));
            total += 1;
        }
    }
    let mut spec = common::spec("train");
    spec.read.selection = Selection::Range {
        start: vec![10],
        end: vec![30],
    };
    let prepared = common::prepare(&sessions, &identity, &spec).await;
    let expected = common::drain(&sessions, &identity, prepared.session.unwrap(), spec.read.id).await;
    assert_eq!(total, expected.len());
    assert_eq!(all, expected.into_iter().collect());
}

#[tokio::test]
async fn preparation_cancellation_and_version_collision_cannot_reopen_a_session() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let snapshot = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let revision = split_store::prepared(&authority, &identity, snapshot, None).await;
    split_store::publish_revision(&authority, &identity, &revision).await;
    let sessions = DatasetSessions::new(Arc::new(authority));
    let spec = common::spec("train");
    sessions
        .prepare_split_read(&identity, &spec, 0, 10, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(sessions.start(&identity, spec.read.clone(), 10).await.is_err());
    sessions.cancel_split_read(&identity, spec.read.id).await.unwrap();
    assert!(sessions
        .prepare_split_read(&identity, &spec, 1, 11, &ReadCancellation::new())
        .await
        .is_err());
    assert!(sessions.resume(&identity, spec.read.id, 12).await.is_err());
    let mut legacy = common::spec("train");
    legacy.read.id = OperationId::random();
    sessions.start(&identity, legacy.read.clone(), 10).await.unwrap();
    assert!(sessions
        .prepare_split_read(&identity, &legacy, 0, 10, &ReadCancellation::new())
        .await
        .is_err());
}
