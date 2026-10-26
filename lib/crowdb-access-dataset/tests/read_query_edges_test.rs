// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/read_query.rs"]
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
    let reads = DatasetReads::new(Arc::new(authority));
    let spec = common::spec(empty.name.as_str());
    let bound = reads.bind(&identity, spec).await.unwrap();
    let batch = reads
        .next(&identity, &bound.cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(batch.ids.is_empty() && batch.next.is_none());
    assert!(matches!(
        reads.bind(&identity, common::spec("unknown")).await,
        Err(AuthorityError::NotFound)
    ));
}

#[tokio::test]
async fn query_accepts_empty_inline_equality_without_manifest_reads() {
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
    let reads = DatasetReads::new(Arc::new(authority));
    let mut spec = common::spec("unused");
    spec.split = None;
    spec.selection = Selection::FieldMatch {
        field: "marker".into(),
        value: vec![],
    };
    let bound = reads.bind(&identity, spec).await.unwrap();
    assert_eq!(
        reads
            .next(&identity, &bound.cursor, &ReadCancellation::new())
            .await
            .unwrap()
            .ids,
        vec![b"sample-2".to_vec()]
    );
}

#[tokio::test]
async fn cancellation_is_request_local_and_deletion_rejects_old_continuations() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let snapshot = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let authority = Arc::new(authority);
    let reads = DatasetReads::new(authority.clone());
    let mut spec = common::spec("unused");
    spec.split = None;
    let binding = reads.bind(&identity, spec).await.unwrap();
    store.cancel_scan.store(true, Ordering::Relaxed);
    let cancel = ReadCancellation::new();
    assert!(matches!(
        reads.next(&identity, &binding.cursor, &cancel).await,
        Err(AuthorityError::Invalid(DatasetError::ReadCancelled))
    ));
    assert!(cancel.is_cancelled());
    reads
        .next(&identity, &binding.cursor, &ReadCancellation::new())
        .await
        .unwrap();
    let root = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(3),
            OperationId::random(),
        )
        .await
        .unwrap();
    authority
        .publish_snapshot_candidate(
            &identity,
            &CommitAttempt {
                version: 1,
                id: OperationId::random(),
                target: CommitTarget::Snapshot { snapshot: root },
                expected_head: authority.inspect_head(&identity).await.unwrap().generation,
            },
        )
        .await
        .unwrap();
    authority.begin_reclaim(&identity, snapshot).await.unwrap();
    assert!(reads
        .next(&identity, &binding.cursor, &ReadCancellation::new())
        .await
        .is_err());
}
