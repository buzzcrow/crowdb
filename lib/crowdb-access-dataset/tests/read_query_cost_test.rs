// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/access_store.rs"]
mod access_store;
#[path = "common/publication.rs"]
mod publication;
use crowdb_access_dataset::*;
use std::sync::{atomic::Ordering, Arc};

#[tokio::test]
async fn binding_concurrent_fetch_and_cross_instance_replay_never_write() {
    let store = access_store::TestAccessStore::shared();
    let authority = Arc::new(DatasetAuthority::new(store.clone()));
    let identity = DatasetIdentity::new(NamespacePath::root(), "read-cost").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    authority
        .publish_snapshot(&identity, None, publication::publication_manifest(1))
        .await
        .unwrap();
    let writes = store.writes.load(Ordering::Relaxed);
    let reads = DatasetReads::new(authority.clone());
    let spec = ReadSpec {
        snapshot: "latest".into(),
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
    };
    let admissions = store.admission_reads.load(Ordering::Relaxed);
    let bound = reads.bind(&identity, spec.clone()).await.unwrap();
    assert_eq!(store.admission_reads.load(Ordering::Relaxed) - admissions, 1);
    let other = reads.bind(&identity, spec).await.unwrap();
    assert_eq!(bound.cursor, other.cursor);
    let cancel = ReadCancellation::new();
    let admissions = store.admission_reads.load(Ordering::Relaxed);
    let pages =
        futures::future::join_all((0..96).map(|_| reads.next(&identity, &bound.cursor, &cancel))).await;
    for page in &pages {
        assert_eq!(page.as_ref().unwrap().ids, pages[0].as_ref().unwrap().ids);
    }
    assert_eq!(store.admission_reads.load(Ordering::Relaxed) - admissions, 96);
    let restarted = DatasetReads::new(authority);
    restarted.next(&identity, &bound.cursor, &cancel).await.unwrap();
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}

#[tokio::test]
async fn legacy_scan_replays_only_the_caller_checkpoint_without_writes() {
    let store = access_store::TestAccessStore::shared();
    let authority = Arc::new(DatasetAuthority::new(store.clone()));
    let identity = DatasetIdentity::new(NamespacePath::root(), "scan-cost").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, publication::publication_manifest(1))
        .await
        .unwrap();
    let writes = store.writes.load(Ordering::Relaxed);
    let request = DatasetScanRequest {
        plan: ReadPlan {
            snapshot,
            selection: Selection::Prefix(b"sample-".to_vec()),
            ordering: crowdb_access_dataset::Ordering::SampleId,
            projection: vec!["marker".into()],
            batch_size: 1,
        },
        shuffle: None,
        cursor: None,
    };
    let admissions = store.admission_reads.load(Ordering::Relaxed);
    let first = DatasetReadService::new(authority.clone(), identity.clone(), 1)
        .unwrap()
        .scan(request.clone())
        .await
        .unwrap();
    assert_eq!(store.admission_reads.load(Ordering::Relaxed) - admissions, 1);
    let restarted = DatasetReadService::new(authority, identity, 1).unwrap();
    let admissions = store.admission_reads.load(Ordering::Relaxed);
    assert_eq!(restarted.scan(request.clone()).await.unwrap(), first);
    assert_eq!(store.admission_reads.load(Ordering::Relaxed) - admissions, 1);
    let admissions = store.admission_reads.load(Ordering::Relaxed);
    let completed = restarted
        .scan(DatasetScanRequest {
            cursor: Some(first.cursor),
            ..request
        })
        .await
        .unwrap();
    assert_eq!(store.admission_reads.load(Ordering::Relaxed) - admissions, 1);
    assert!(completed.samples.is_empty());
    assert!(completed.end);
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}

async fn split_binding_fixture() -> (
    Arc<access_store::TestAccessStore>,
    DatasetReads,
    DatasetIdentity,
    SnapshotId,
    SplitRevisionId,
) {
    use crowdb_access_dataset::split::*;
    let store = access_store::TestAccessStore::shared();
    let authority = Arc::new(DatasetAuthority::new(store.clone()));
    let identity = DatasetIdentity::new(NamespacePath::root(), "bind-split-cost").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, publication::publication_manifest(1))
        .await
        .unwrap();
    let input = SplitPreparation {
        revision: SplitRevisionId::random(),
        policy: SplitPolicy::default(),
        splits: vec![DraftSplit {
            name: SplitName::new("train").unwrap(),
            definition: SplitDefinition::Selection(Selection::Prefix(b"sample".to_vec())),
            origin: MembershipOrigin::Evaluated,
        }],
    };
    authority
        .prepare_split_revision(&identity, snapshot, &input, &ReadCancellation::new())
        .await
        .unwrap();
    authority
        .update_split_head(
            &identity,
            snapshot,
            &SplitHeadUpdate {
                revision: input.revision,
                expected_revision: None,
                expected_head: None,
            },
        )
        .await
        .unwrap();
    (
        store,
        DatasetReads::new(authority),
        identity,
        snapshot,
        input.revision,
    )
}
fn binding_spec(snapshot: SnapshotId, revision: Option<SplitRevisionId>) -> ReadSpec {
    ReadSpec {
        snapshot: snapshot.to_string(),
        selection: Selection::Prefix(b"sample".to_vec()),
        fields: vec!["marker".into()],
        batch_size: 1,
        partition: ReadPartition {
            worker: 0,
            workers: 1,
            rank: 0,
            world_size: 1,
        },
        shuffle: None,
        split: Some(ReadSplit {
            name: split::SplitName::new("train").unwrap(),
            revision,
        }),
    }
}

#[tokio::test]
async fn explicit_split_binding_reuses_snapshot_admission() {
    let (store, reads, identity, snapshot, revision) = split_binding_fixture().await;
    let before = store.reads.load(Ordering::Relaxed) + store.bounded_reads.load(Ordering::Relaxed);
    let admissions = store.admission_reads.load(Ordering::Relaxed);
    let writes = store.writes.load(Ordering::Relaxed);
    let binding = reads
        .bind(&identity, binding_spec(snapshot, Some(revision)))
        .await
        .unwrap();
    assert_eq!(binding.split.unwrap().revision, revision);
    assert_eq!(
        store.reads.load(Ordering::Relaxed) + store.bounded_reads.load(Ordering::Relaxed) - before,
        3
    );
    assert_eq!(store.admission_reads.load(Ordering::Relaxed) - admissions, 1);
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}

#[tokio::test]
async fn current_split_binding_admits_snapshot_once_before_resolving_the_resource() {
    let (store, reads, identity, snapshot, revision) = split_binding_fixture().await;
    let before = store.reads.load(Ordering::Relaxed) + store.bounded_reads.load(Ordering::Relaxed);
    let admissions = store.admission_reads.load(Ordering::Relaxed);
    let writes = store.writes.load(Ordering::Relaxed);
    let binding = reads.bind(&identity, binding_spec(snapshot, None)).await.unwrap();
    assert_eq!(binding.split.unwrap().revision, revision);
    assert_eq!(
        store.reads.load(Ordering::Relaxed) + store.bounded_reads.load(Ordering::Relaxed) - before,
        4
    );
    assert_eq!(store.admission_reads.load(Ordering::Relaxed) - admissions, 1);
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}

#[tokio::test]
async fn later_bindings_recheck_deletion_for_explicit_and_current_split_requests() {
    let (store, reads, identity, snapshot, revision) = split_binding_fixture().await;
    reads
        .bind(&identity, binding_spec(snapshot, Some(revision)))
        .await
        .unwrap();
    reads.bind(&identity, binding_spec(snapshot, None)).await.unwrap();
    let authority = DatasetAuthority::new(store.clone());
    let root = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(2),
            OperationId::random(),
        )
        .await
        .unwrap();
    let head = authority.inspect_head(&identity).await.unwrap();
    authority
        .update_snapshot_head(
            &identity,
            &SnapshotHeadUpdate {
                snapshot: root,
                expected_head: head.generation,
            },
        )
        .await
        .unwrap();
    authority.begin_reclaim(&identity, snapshot).await.unwrap();
    let writes = store.writes.load(Ordering::Relaxed);
    for revision in [Some(revision), None] {
        let admissions = store.admission_reads.load(Ordering::Relaxed);
        assert!(matches!(
            reads.bind(&identity, binding_spec(snapshot, revision)).await,
            Err(AuthorityError::SnapshotProtected)
        ));
        assert_eq!(store.admission_reads.load(Ordering::Relaxed) - admissions, 1);
    }
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}
