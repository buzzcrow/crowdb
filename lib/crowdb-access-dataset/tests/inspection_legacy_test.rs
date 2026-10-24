// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
#[path = "common/legacy_inspection.rs"]
mod legacy;
use crowdb_access_dataset::{
    inspection::{InspectionError, InspectionPreparationLimits, PreparationState},
    *,
};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn legacy_preparation_rebuilds_final_rows_without_progress_and_reuses_ready_root() {
    let (store, authority, identity, snapshot, bytes) = legacy::fixture(false).await;
    let before = store.mutations.load(Ordering::Relaxed);
    let reads = store.reads.load(Ordering::Relaxed);
    let result = authority
        .prepare_inspection(
            &identity,
            snapshot,
            InspectionPreparationLimits::default(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.state, PreparationState::Ready);
    assert_eq!((result.revision, result.rows_written), (0, 0));
    assert_eq!(store.reads.load(Ordering::Relaxed) - reads, 3);
    assert_eq!(store.source_reads.load(Ordering::Relaxed), 1);
    assert_eq!(store.progress_reads.load(Ordering::Relaxed), 0);
    assert_eq!(store.mutations.load(Ordering::Relaxed) - before, 13);
    let reads = store.reads.load(Ordering::Relaxed);
    assert_eq!(
        DatasetAuthority::new(store.clone())
            .prepare_inspection(
                &identity,
                snapshot,
                InspectionPreparationLimits::default(),
                &ReadCancellation::new()
            )
            .await
            .unwrap(),
        result
    );
    assert_eq!(store.mutations.load(Ordering::Relaxed) - before, 13);
    assert_eq!(store.reads.load(Ordering::Relaxed) - reads, 2);
    assert_eq!(store.source_reads.load(Ordering::Relaxed), 1);
    assert_eq!(
        store
            .inner
            .get(&legacy::snapshot_key(&identity, snapshot))
            .await
            .unwrap()
            .unwrap()
            .bytes,
        bytes
    );
    assert!(store
        .inner
        .get(&legacy::index_key(&identity, snapshot, b'P'))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn legacy_progress_is_observed_but_never_resumed_or_allowed_to_override_a_ready_root() {
    let (store, authority, identity, snapshot, _) = legacy::fixture(false).await;
    let key = legacy::index_key(&identity, snapshot, b'P');
    let bytes = legacy::old_progress(snapshot, false);
    store.inner.compare_exchange(&key, None, &bytes).await.unwrap();
    let old = authority
        .inspection_preparation_status(&identity, snapshot, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(old.state, PreparationState::Preparing);
    assert_eq!(old.revision, 7);
    let reads = store.progress_reads.load(Ordering::Relaxed);
    let result = authority
        .prepare_inspection(
            &identity,
            snapshot,
            InspectionPreparationLimits::default(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(store.progress_reads.load(Ordering::Relaxed), reads);
    assert_eq!(
        authority
            .inspection_preparation_status(&identity, snapshot, &ReadCancellation::new())
            .await
            .unwrap(),
        result
    );
    assert_eq!(store.progress_reads.load(Ordering::Relaxed), reads);
    assert_eq!(store.inner.get(&key).await.unwrap().unwrap().bytes, bytes);
}

#[tokio::test]
async fn corrupt_legacy_progress_does_not_control_a_complete_rebuild() {
    let (store, authority, identity, snapshot, _) = legacy::fixture(false).await;
    let key = legacy::index_key(&identity, snapshot, b'P');
    let mut old: serde_json::Value = serde_json::from_slice(&legacy::old_progress(snapshot, false)).unwrap();
    old["version"] = 99.into();
    let bytes = serde_json::to_vec(&old).unwrap();
    store.inner.compare_exchange(&key, None, &bytes).await.unwrap();
    assert!(matches!(
        authority
            .inspection_preparation_status(&identity, snapshot, &ReadCancellation::new())
            .await,
        Err(InspectionError::Corrupt)
    ));
    let result = authority
        .prepare_inspection(
            &identity,
            snapshot,
            InspectionPreparationLimits::default(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.state, PreparationState::Ready);
    assert_eq!(store.inner.get(&key).await.unwrap().unwrap().bytes, bytes);
}

#[tokio::test]
async fn legacy_limits_and_cancellation_do_not_write_rows_or_reader_activity() {
    let (store, authority, identity, snapshot, _) = legacy::fixture(false).await;
    let writes = store.mutations.load(Ordering::Relaxed);
    assert!(matches!(
        authority
            .prepare_inspection(
                &identity,
                snapshot,
                InspectionPreparationLimits { source_bytes: 32 },
                &ReadCancellation::new()
            )
            .await,
        Err(InspectionError::Limit)
    ));
    let cancel = ReadCancellation::new();
    cancel.cancel();
    assert!(matches!(
        authority
            .prepare_inspection(
                &identity,
                snapshot,
                InspectionPreparationLimits::default(),
                &cancel
            )
            .await,
        Err(InspectionError::Cancelled)
    ));
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
    assert!(store
        .inner
        .get(
            &[
                b"DS\0L\0".as_slice(),
                &identity.key().unwrap(),
                snapshot.as_bytes()
            ]
            .concat()
        )
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        authority
            .inspection_preparation_status(&identity, snapshot, &ReadCancellation::new())
            .await,
        Err(InspectionError::NotPrepared)
    ));
}

#[tokio::test]
async fn unsupported_legacy_source_is_an_error_without_new_process_state() {
    let (store, authority, identity, snapshot, _) = legacy::fixture(true).await;
    let writes = store.mutations.load(Ordering::Relaxed);
    assert!(matches!(
        authority
            .prepare_inspection(
                &identity,
                snapshot,
                InspectionPreparationLimits::default(),
                &ReadCancellation::new()
            )
            .await,
        Err(InspectionError::Unsupported(_))
    ));
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
    assert!(store
        .inner
        .get(&legacy::index_key(&identity, snapshot, b'P'))
        .await
        .unwrap()
        .is_none());
    assert!(store
        .inner
        .get(&legacy::index_key(&identity, snapshot, b'H'))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn legacy_ready_progress_without_final_root_remains_corrupt() {
    let (store, authority, identity, snapshot, _) = legacy::fixture(false).await;
    store
        .inner
        .compare_exchange(
            &legacy::index_key(&identity, snapshot, b'P'),
            None,
            &legacy::old_progress(snapshot, true),
        )
        .await
        .unwrap();
    assert!(matches!(
        authority
            .inspection_preparation_status(&identity, snapshot, &ReadCancellation::new())
            .await,
        Err(InspectionError::Corrupt)
    ));
}
