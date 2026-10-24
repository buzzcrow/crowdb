// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
#[path = "common/legacy_inspection.rs"]
mod legacy;
use crowdb_access_dataset::{
    inspection::{InspectionError, InspectionPreparationLimits},
    *,
};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn invalid_source_limits_are_rejected_before_store_access() {
    let (store, authority, identity, snapshot, _) = legacy::fixture(false).await;
    let reads = store.reads.load(Ordering::Relaxed);
    let writes = store.mutations.load(Ordering::Relaxed);
    for source_bytes in [0, 9 * 1024 * 1024 + 1] {
        assert!(matches!(
            authority
                .prepare_inspection(
                    &identity,
                    snapshot,
                    InspectionPreparationLimits { source_bytes },
                    &ReadCancellation::new(),
                )
                .await,
            Err(InspectionError::Limit)
        ));
    }
    assert_eq!(store.reads.load(Ordering::Relaxed), reads);
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
}

#[tokio::test]
async fn oversized_embedded_manifest_is_rejected_before_decoding_or_writes() {
    let (store, authority, identity, snapshot, old) = legacy::fixture(false).await;
    let record = SnapshotRecord {
        publication: SnapshotPublication {
            snapshot,
            parent: None,
            operation: OperationId::random(),
            manifest: vec![0; 8 * 1024 * 1024 + 1],
            state: PublicationState::Published,
        },
    };
    let bytes = bincode::serialize(&legacy::TestVersioned {
        version: 1,
        payload: record,
    })
    .unwrap();
    assert!(bytes.len() < 9 * 1024 * 1024);
    store
        .inner
        .compare_exchange(&legacy::snapshot_key(&identity, snapshot), Some(&old), &bytes)
        .await
        .unwrap();
    let writes = store.mutations.load(Ordering::Relaxed);
    assert!(matches!(
        authority
            .prepare_inspection(
                &identity,
                snapshot,
                InspectionPreparationLimits::default(),
                &ReadCancellation::new(),
            )
            .await,
        Err(InspectionError::Limit)
    ));
    assert_eq!(store.source_reads.load(Ordering::Relaxed), 1);
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
}

#[tokio::test]
async fn marked_snapshot_rejects_a_new_rebuild_without_reading_its_source() {
    let (store, authority, identity, snapshot, _) = legacy::fixture(false).await;
    store.allow_legacy_get.store(true, Ordering::Relaxed);
    authority.begin_reclaim(&identity, snapshot).await.unwrap();
    let reads = store.source_reads.load(Ordering::Relaxed);
    let writes = store.mutations.load(Ordering::Relaxed);
    assert!(matches!(
        authority
            .prepare_inspection(
                &identity,
                snapshot,
                InspectionPreparationLimits::default(),
                &ReadCancellation::new(),
            )
            .await,
        Err(InspectionError::Authority(AuthorityError::SnapshotNotFound))
    ));
    assert_eq!(store.source_reads.load(Ordering::Relaxed), reads);
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
}
