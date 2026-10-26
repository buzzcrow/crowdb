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
async fn partial_failure_remains_unavailable_and_retry_recomputes_from_source() {
    let (store, authority, identity, snapshot, source) = legacy::fixture(false).await;
    store.fail_value.store(true, Ordering::Relaxed);
    assert!(authority
        .prepare_inspection(
            &identity,
            snapshot,
            InspectionPreparationLimits::default(),
            &ReadCancellation::new()
        )
        .await
        .is_err());
    assert!(store
        .inner
        .get(&legacy::index_key(&identity, snapshot, b'H'))
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        authority
            .inspection_preparation_status(&identity, snapshot, &ReadCancellation::new())
            .await,
        Err(InspectionError::NotPrepared)
    ));
    let restarted = DatasetAuthority::new(store.clone());
    let result = restarted
        .prepare_inspection(
            &identity,
            snapshot,
            InspectionPreparationLimits::default(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.state, PreparationState::Ready);
    assert_eq!(store.source_reads.load(Ordering::Relaxed), 2);
    assert_eq!(store.progress_reads.load(Ordering::Relaxed), 1);
    assert_eq!(
        store
            .inner
            .get(&legacy::snapshot_key(&identity, snapshot))
            .await
            .unwrap()
            .unwrap()
            .bytes,
        source
    );
    assert_eq!(store.pending.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn lost_final_reply_returns_without_confirmation_and_retry_writes_nothing() {
    let (store, authority, identity, snapshot, _) = legacy::fixture(false).await;
    store.lose_final_reply.store(true, Ordering::Relaxed);
    let reads = store.reads.load(Ordering::Relaxed);
    let writes = store.mutations.load(Ordering::Relaxed);
    assert!(authority
        .prepare_inspection(
            &identity,
            snapshot,
            InspectionPreparationLimits::default(),
            &ReadCancellation::new()
        )
        .await
        .is_err());
    assert_eq!(store.reads.load(Ordering::Relaxed) - reads, 3);
    assert_eq!(store.mutations.load(Ordering::Relaxed) - writes, 13);
    store.reject_after_unknown.store(false, Ordering::Relaxed);
    let reads = store.reads.load(Ordering::Relaxed);
    let result = DatasetAuthority::new(store.clone())
        .prepare_inspection(
            &identity,
            snapshot,
            InspectionPreparationLimits::default(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.state, PreparationState::Ready);
    assert_eq!(store.reads.load(Ordering::Relaxed) - reads, 2);
    assert_eq!(store.mutations.load(Ordering::Relaxed) - writes, 13);
}

#[tokio::test]
async fn concurrent_rebuilds_share_only_complete_rows_without_progress_cas() {
    let (store, authority, identity, snapshot, _) = legacy::fixture(false).await;
    let writes = store.mutations.load(Ordering::Relaxed);
    let cancel = ReadCancellation::new();
    let (first, second) = tokio::join!(
        authority.prepare_inspection(
            &identity,
            snapshot,
            InspectionPreparationLimits::default(),
            &cancel,
        ),
        authority.prepare_inspection(
            &identity,
            snapshot,
            InspectionPreparationLimits::default(),
            &cancel,
        )
    );
    let first = first.unwrap();
    assert_eq!(first.state, PreparationState::Ready);
    assert_eq!(second.unwrap(), first);
    assert_eq!(store.source_reads.load(Ordering::Relaxed), 2);
    assert_eq!(store.progress_reads.load(Ordering::Relaxed), 0);
    assert_eq!(store.mutations.load(Ordering::Relaxed) - writes, 26);
    assert_eq!(store.pending.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn cancellation_drops_pending_writes_without_installing_readiness_or_progress() {
    let (store, authority, identity, snapshot, _) = legacy::fixture(false).await;
    store.pause_kind.store(b'S', Ordering::Relaxed);
    let cancel = ReadCancellation::new();
    let task = tokio::spawn({
        let authority = authority.clone();
        let identity = identity.clone();
        let cancel = cancel.clone();
        async move {
            authority
                .prepare_inspection(
                    &identity,
                    snapshot,
                    InspectionPreparationLimits::default(),
                    &cancel,
                )
                .await
        }
    });
    store.observed.notified().await;
    cancel.cancel();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        Err(InspectionError::Cancelled)
    ));
    assert_eq!(store.pending.load(Ordering::Relaxed), 0);
    assert!(store
        .inner
        .get(&legacy::index_key(&identity, snapshot, b'H'))
        .await
        .unwrap()
        .is_none());
    assert!(store
        .inner
        .get(&legacy::index_key(&identity, snapshot, b'P'))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test(start_paused = true)]
async fn rebuild_has_one_total_deadline_and_releases_delayed_mutations() {
    let (store, authority, identity, snapshot, _) = legacy::fixture(false).await;
    store.pause_kind.store(b'S', Ordering::Relaxed);
    let task = tokio::spawn({
        let identity = identity.clone();
        async move {
            authority
                .prepare_inspection(
                    &identity,
                    snapshot,
                    InspectionPreparationLimits::default(),
                    &ReadCancellation::new(),
                )
                .await
        }
    });
    store.observed.notified().await;
    tokio::time::advance(std::time::Duration::from_secs(31)).await;
    assert!(matches!(task.await.unwrap(), Err(InspectionError::Limit)));
    assert_eq!(store.pending.load(Ordering::Relaxed), 0);
    assert!(store
        .inner
        .get(&legacy::index_key(&identity, snapshot, b'H'))
        .await
        .unwrap()
        .is_none());
}
