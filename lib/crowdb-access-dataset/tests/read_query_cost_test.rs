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
    let first = DatasetReadService::new(authority.clone(), identity.clone(), 1)
        .unwrap()
        .scan(request.clone())
        .await
        .unwrap();
    let restarted = DatasetReadService::new(authority, identity, 1).unwrap();
    assert_eq!(restarted.scan(request.clone()).await.unwrap(), first);
    let completed = restarted
        .scan(DatasetScanRequest {
            cursor: Some(first.cursor),
            ..request
        })
        .await
        .unwrap();
    assert!(completed.samples.is_empty());
    assert!(completed.end);
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}
