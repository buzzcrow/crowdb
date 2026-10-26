// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset.rs"]
#[allow(dead_code)] // Uses the immutable Manifest and operation context, not read fixtures.
mod dataset;
#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/storage.rs"]
mod storage;

use crowdb_access_dataset::{composition::*, split::*, *};
use crowdb_sdk_native::{dataset::DatasetClient, Error};
use dataset::context;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

fn counts(stats: &metadata::TestMutationGate) -> (usize, usize) {
    (
        stats.reads.load(Ordering::Relaxed) + stats.bounded_reads.load(Ordering::Relaxed),
        stats.writes.load(Ordering::Relaxed),
    )
}
fn delta(stats: &metadata::TestMutationGate, before: (usize, usize)) -> (usize, usize) {
    let after = counts(stats);
    (after.0 - before.0, after.1 - before.1)
}

async fn fixture() -> (
    DatasetClient,
    Arc<metadata::TestMutationGate>,
    Arc<AtomicBool>,
    SnapshotId,
    SplitRevisionId,
) {
    let stats = Arc::new(metadata::TestMutationGate::default());
    stats.reject_workflow_writes.store(true, Ordering::Relaxed);
    let loss = Arc::new(AtomicBool::new(false));
    let metadata = metadata::client_with_loss_and_gate(loss.clone(), stats.clone()).await;
    let runtime = storage::runtime_with_metadata(metadata);
    let client = DatasetClient::new(
        &runtime,
        DatasetIdentity::new(NamespacePath::root(), "delete").unwrap(),
    )
    .unwrap();
    client.create(context(1)).await.unwrap();
    let manifest: ManifestRecord = bincode::deserialize(&dataset::manifest()).unwrap();
    let snapshot = SnapshotId::random();
    client
        .prepare_snapshot(
            context(2),
            snapshot,
            &CompositionRequest {
                parent: None,
                source: CompositionSource::All,
                schema: Some(manifest.schema),
                samples: manifest.samples.into_iter().map(SampleChange::Upsert).collect(),
                splits: vec![],
                policy: SplitPolicy::default(),
            },
        )
        .await
        .unwrap();
    let revision = SplitRevisionId::random();
    client
        .prepare_split_revision(
            context(3),
            snapshot,
            &SplitPreparation {
                revision,
                policy: SplitPolicy::default(),
                splits: vec![],
            },
        )
        .await
        .unwrap();
    (client, stats, loss, snapshot, revision)
}

#[tokio::test]
async fn native_delete_adds_no_metadata_rpc_or_workflow_state_and_repeats_the_fixed_mark() {
    let (client, stats, _, snapshot, revision) = fixture().await;
    let before = counts(&stats);
    let deleted = client
        .delete_split_revision(context(4), snapshot, revision)
        .await
        .unwrap();
    assert_eq!(delta(&stats, before), (4, 3));
    assert_eq!((deleted.snapshot, deleted.revision), (snapshot, revision));
    assert!(deleted.reclaim_after.is_some());
    assert!(!deleted.complete);
    let before = counts(&stats);
    assert_eq!(
        client
            .delete_split_revision(context(5), snapshot, revision)
            .await
            .unwrap(),
        deleted
    );
    assert_eq!(delta(&stats, before), (1, 0));
    client.snapshot(context(6), snapshot).await.unwrap();
}

#[tokio::test]
async fn native_lost_delete_reply_returns_uncertainty_without_confirmation_and_fresh_retry_finishes() {
    let (client, stats, loss, snapshot, revision) = fixture().await;
    let before = counts(&stats);
    loss.store(true, Ordering::Release);
    assert!(matches!(
        client
            .delete_split_revision(context(20), snapshot, revision)
            .await,
        Err(Error::UncertainMutation { request_id: 20, .. })
    ));
    assert_eq!(delta(&stats, before), (4, 1));
    loss.store(false, Ordering::Release);
    let before = counts(&stats);
    let deleted = client
        .delete_split_revision(context(21), snapshot, revision)
        .await
        .unwrap();
    assert!(!deleted.complete);
    assert_eq!(delta(&stats, before), (4, 2));
    let before = counts(&stats);
    assert_eq!(
        client
            .delete_split_revision(context(22), snapshot, revision)
            .await
            .unwrap(),
        deleted
    );
    assert_eq!(delta(&stats, before), (1, 0));
}

#[tokio::test]
async fn native_current_revision_conflicts_without_rebasing_or_retiring_the_snapshot() {
    let (client, stats, _, snapshot, revision) = fixture().await;
    client
        .update_split_head(
            context(30),
            snapshot,
            &SplitHeadUpdate {
                revision,
                expected_revision: None,
                expected_head: None,
            },
        )
        .await
        .unwrap();
    let before = counts(&stats);
    assert!(matches!(
        client
            .delete_split_revision(context(31), snapshot, revision)
            .await,
        Err(Error::Dataset {
            kind: crowdb_sdk_native::dataset::FailureKind::Conflict,
            ..
        })
    ));
    assert_eq!(delta(&stats, before), (4, 0));
    client.snapshot(context(32), snapshot).await.unwrap();
}
