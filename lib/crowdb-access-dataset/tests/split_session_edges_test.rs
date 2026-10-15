// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/split_session.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::*;
use std::sync::{atomic::Ordering, Arc};

#[tokio::test]
async fn empty_split_finishes_but_unknown_split_fails_without_manifest_reads() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let revision = split_store::prepared(&authority, &identity, snapshot, None).await;
    split_store::publish_revision(&authority, &identity, &revision).await;
    let empty = revision.splits.iter().find(|s| s.count == 0).unwrap();
    store.metadata_only.store(true, Ordering::Relaxed);
    let sessions = DatasetSessions::new(Arc::new(authority));
    let spec = common::spec(empty.name.as_str());
    let prepared = common::prepare(&sessions, &identity, &spec).await;
    assert_eq!(prepared.admitted, 0);
    let batch = sessions.next(&identity, spec.read.id, 11).await.unwrap();
    assert!(batch.ids.is_empty() && batch.end);
    let missing = common::spec("unknown");
    assert!(matches!(
        sessions
            .prepare_split_read(&identity, &missing, 0, 10, &ReadCancellation::new())
            .await,
        Err(AuthorityError::NotFound)
    ));
}

#[tokio::test]
async fn v2_session_accepts_empty_inline_equality_without_changing_legacy_plans() {
    use md5::{Digest, Md5};
    let (store, authority, identity, parent) = split_store::fixture().await;
    let mut manifest: ManifestRecord = bincode::deserialize(&publication::publication_manifest(2)).unwrap();
    manifest.samples[0].fields[0].value = FieldLocator::Inline {
        value: vec![],
        md5: Md5::digest([]).into(),
    };
    authority
        .publish_snapshot(&identity, Some(parent), bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    store.metadata_only.store(true, Ordering::Relaxed);
    let sessions = DatasetSessions::new(Arc::new(authority));
    let mut spec = common::spec("unused");
    spec.split = None;
    spec.read.selection = Selection::FieldMatch {
        field: "marker".into(),
        value: vec![],
    };
    let prepared = common::prepare(&sessions, &identity, &spec).await;
    assert_eq!(prepared.admitted, 1);
    assert_eq!(
        sessions.next(&identity, spec.read.id, 11).await.unwrap().ids,
        vec![b"sample-2".to_vec()]
    );
    assert!(
        spec.read.selection.validate().is_err(),
        "v1 equality must keep its original semantics"
    );
}

#[tokio::test]
async fn interrupted_preparation_releases_its_lease_without_losing_durable_progress() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let snapshot = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let authority = Arc::new(authority);
    let sessions = DatasetSessions::new(authority.clone());
    let mut spec = common::spec("unused");
    spec.split = None;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let first = sessions
        .prepare_split_read(&identity, &spec, 0, now, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(!first.complete);
    store.cancel_scan.store(true, Ordering::Relaxed);
    let cancel = ReadCancellation::new();
    assert!(matches!(
        sessions
            .prepare_split_read(&identity, &spec, first.next_page, now, &cancel)
            .await,
        Err(AuthorityError::Invalid(DatasetError::ReadCancelled))
    ));
    assert!(cancel.is_cancelled());
    let resumed = sessions
        .prepare_split_read(&identity, &spec, first.next_page, now, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(resumed.next_page > first.next_page);
    let root = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(3),
            OperationId::random(),
        )
        .await
        .unwrap();
    let attempt = CommitAttempt {
        version: 1,
        id: OperationId::random(),
        target: CommitTarget::Snapshot { snapshot: root },
        expected_head: authority.inspect_head(&identity).await.unwrap().generation,
    };
    authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .unwrap();
    assert!(matches!(
        authority.begin_reclaim(&identity, snapshot).await,
        Err(AuthorityError::SnapshotProtected)
    ));
    cancel.cancel();
    assert!(sessions
        .prepare_split_read(&identity, &spec, resumed.next_page, now, &cancel)
        .await
        .is_err());
    authority.begin_reclaim(&identity, snapshot).await.unwrap();
}
