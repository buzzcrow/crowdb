// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/snapshot_complete.rs"]
mod complete;
#[path = "common/composition.rs"]
#[allow(dead_code)]
mod composition;
#[path = "common/legacy_drafts.rs"]
#[allow(dead_code)]
mod legacy;
#[path = "common/publication.rs"]
mod publication;

use complete::{candidate, finish_cleanup, fixture, key};
use crowdb_access_dataset::*;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[tokio::test]
async fn lost_final_reply_keeps_the_complete_result_without_confirmation_or_draft_pointer() {
    let (store, authority, identity, parent) = fixture().await;
    let operation = OperationId::random();
    legacy::editing_composition(store.storage(), &authority, &identity, operation, parent).await;
    store.lose_final_reply.store(true, Ordering::Relaxed);
    assert!(matches!(
        authority
            .prepare_snapshot_candidate(
                &identity,
                Some(parent),
                publication::publication_manifest(2),
                candidate(operation)
            )
            .await,
        Err(AuthorityError::Store(StoreError::Rejected))
    ));
    assert_eq!(store.reads_after_unknown.load(Ordering::Relaxed), 0);
    store.unknown.store(false, Ordering::Relaxed);
    let snapshot = authority
        .publication_status(&identity, candidate(operation))
        .await
        .unwrap()
        .publication
        .snapshot;
    authority
        .abandon_composition(&identity, operation, 1)
        .await
        .unwrap();
    finish_cleanup(&authority, &identity, operation).await;
    let restarted = DatasetAuthority::new(store.clone());
    store.reads_only.store(true, Ordering::Relaxed);
    let before = store.bounded_reads.load(Ordering::Relaxed);
    let summary = restarted
        .snapshot_inspection_summary(
            &identity,
            snapshot,
            composition::budget(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(summary.snapshot, snapshot);
    assert_eq!(store.bounded_reads.load(Ordering::Relaxed) - before, 3);
    let record = restarted.get_snapshot(&identity, snapshot).await.unwrap();
    assert_eq!(record.publication.parent, Some(parent));
    assert_eq!(
        restarted
            .read_batch(&identity, snapshot, &[b"sample-2".to_vec()], &["marker"])
            .await
            .unwrap()[0]
            .values["marker"],
        [2]
    );
}

#[tokio::test]
async fn cleanup_seals_the_same_root_before_a_delayed_finalizer_can_publish() {
    let (store, authority, identity, parent) = fixture().await;
    let operation = OperationId::random();
    legacy::editing_composition(store.storage(), &authority, &identity, operation, parent).await;
    store.gate.store(1, Ordering::Relaxed);
    let client = authority.clone();
    let scope = identity.clone();
    let pending = tokio::spawn(async move {
        client
            .prepare_snapshot_candidate(
                &scope,
                Some(parent),
                publication::publication_manifest(2),
                candidate(operation),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), store.started.notified())
        .await
        .unwrap();
    let snapshot = authority
        .publication_status(&identity, candidate(operation))
        .await
        .unwrap()
        .publication
        .snapshot;
    assert!(authority.get_snapshot(&identity, snapshot).await.is_err());
    authority
        .abandon_composition(&identity, operation, 1)
        .await
        .unwrap();
    finish_cleanup(&authority, &identity, operation).await;
    store.release.notify_one();
    assert!(matches!(
        pending.await.unwrap(),
        Err(AuthorityError::SnapshotProtected)
    ));
    assert_eq!(
        store
            .storage()
            .get(&key(b"DS\0CB\0", &identity, snapshot))
            .await
            .unwrap()
            .unwrap()
            .bytes,
        complete::RETIRED
    );
    let restarted = DatasetAuthority::new(store);
    assert!(restarted.get_snapshot(&identity, snapshot).await.is_err());
    assert!(restarted
        .complete_snapshot_candidate(&identity, snapshot)
        .await
        .is_err());
}

#[tokio::test]
async fn cleanup_uses_a_completed_cas_winner_without_reloading_or_retiring_content() {
    let (store, authority, identity, parent) = fixture().await;
    let operation = OperationId::random();
    legacy::editing_composition(store.storage(), &authority, &identity, operation, parent).await;
    let snapshot = authority
        .prepare_snapshot_candidate(
            &identity,
            Some(parent),
            publication::publication_manifest(2),
            candidate(operation),
        )
        .await
        .unwrap();
    let cb = key(b"DS\0CB\0", &identity, snapshot);
    let complete = store.storage().get(&cb).await.unwrap().unwrap();
    let mut staged: serde_json::Value = serde_json::from_slice(&complete.bytes).unwrap();
    staged["version"] = 3.into();
    assert!(matches!(
        store
            .storage()
            .compare_exchange(&cb, Some(&complete.bytes), &serde_json::to_vec(&staged).unwrap())
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    store.seal_winner.store(Some(Arc::new(complete.bytes)));
    authority
        .abandon_composition(&identity, operation, 1)
        .await
        .unwrap();
    finish_cleanup(&authority, &identity, operation).await;
    assert!(store.seal_conflicted.load(Ordering::Relaxed));
    assert_eq!(store.cb_reads_after_conflict.load(Ordering::Relaxed), 0);
    assert!(authority.get_snapshot(&identity, snapshot).await.is_ok());
    assert!(authority.get_manifest(&identity, snapshot).await.is_ok());
}

async fn off_head_parent(authority: &DatasetAuthority, identity: &DatasetIdentity) {
    let root = authority
        .prepare_snapshot_candidate(
            identity,
            None,
            publication::publication_manifest(3),
            OperationId::random(),
        )
        .await
        .unwrap();
    authority
        .publish_snapshot_candidate(identity, &composition::attempt(authority, identity, root).await)
        .await
        .unwrap();
}

#[tokio::test]
async fn indexed_child_protects_its_parent_while_finalization_is_pending() {
    let (store, authority, identity, parent) = fixture().await;
    off_head_parent(&authority, &identity).await;
    store.gate.store(1, Ordering::Relaxed);
    let client = authority.clone();
    let scope = identity.clone();
    let pending = tokio::spawn(async move {
        client
            .prepare_snapshot_candidate(
                &scope,
                Some(parent),
                publication::publication_manifest(2),
                OperationId::random(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), store.started.notified())
        .await
        .unwrap();
    assert!(matches!(
        authority.begin_reclaim(&identity, parent).await,
        Err(AuthorityError::SnapshotProtected)
    ));
    store.release.notify_one();
    let child = pending.await.unwrap().unwrap();
    assert_eq!(
        authority
            .get_snapshot(&identity, child)
            .await
            .unwrap()
            .publication
            .parent,
        Some(parent)
    );
}

#[tokio::test]
async fn child_cannot_complete_when_parent_retirement_precedes_its_index() {
    let (store, authority, identity, parent) = fixture().await;
    off_head_parent(&authority, &identity).await;
    let operation = OperationId::random();
    store.gate.store(2, Ordering::Relaxed);
    let client = authority.clone();
    let scope = identity.clone();
    let pending = tokio::spawn(async move {
        client
            .prepare_snapshot_candidate(
                &scope,
                Some(parent),
                publication::publication_manifest(2),
                operation,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), store.started.notified())
        .await
        .unwrap();
    authority.begin_reclaim(&identity, parent).await.unwrap();
    store.release.notify_one();
    assert!(pending.await.unwrap().is_err());
    let snapshot = authority
        .publication_status(&identity, operation)
        .await
        .unwrap()
        .publication
        .snapshot;
    assert!(authority.get_snapshot(&identity, snapshot).await.is_err());
    assert!(authority
        .publish_snapshot_candidate(
            &identity,
            &composition::attempt(&authority, &identity, snapshot).await
        )
        .await
        .is_err());
}

#[tokio::test]
async fn unpublished_continuations_cross_instances_and_still_obey_deletion() {
    let (store, authority, identity, parent) = fixture().await;
    let snapshot = authority
        .prepare_snapshot_candidate(
            &identity,
            Some(parent),
            publication::publication_manifest(2),
            OperationId::random(),
        )
        .await
        .unwrap();
    let bound = DatasetReads::new(authority.clone())
        .bind(
            &identity,
            ReadSpec {
                snapshot: snapshot.to_string(),
                selection: Selection::Prefix(b"sample-".to_vec()),
                fields: vec!["marker".into()],
                batch_size: 1,
                partition: ReadPartition {
                    worker: 0,
                    workers: 1,
                    rank: 0,
                    world_size: 1,
                },
                shuffle: None,
                split: None,
            },
        )
        .await
        .unwrap();
    off_head_parent(&authority, &identity).await;
    let restarted = Arc::new(DatasetAuthority::new(store));
    let reads = DatasetReads::new(restarted.clone());
    let page = reads
        .next(&identity, &bound.cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(page.ids, [b"sample-2".to_vec()]);
    restarted.retain_snapshot(&identity, snapshot).await.unwrap();
    assert!(matches!(
        restarted.begin_reclaim(&identity, snapshot).await,
        Err(AuthorityError::SnapshotProtected)
    ));
    restarted.release_snapshot(&identity, snapshot).await.unwrap();
    restarted.begin_reclaim(&identity, snapshot).await.unwrap();
    assert!(reads
        .next(&identity, &bound.cursor, &ReadCancellation::new())
        .await
        .is_err());
    assert!(restarted.read_manifest(&identity, snapshot).await.is_err());
    assert!(DatasetReadService::new(restarted, identity, 1)
        .unwrap()
        .read(DatasetReadRequest {
            snapshot,
            sample_ids: vec![b"sample-2".to_vec()],
            fields: vec!["marker".into()],
        })
        .await
        .is_err());
}
