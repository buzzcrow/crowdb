// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset.rs"]
#[allow(dead_code)] // Reuses the manifest fixture without its read-only client setup.
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

fn input() -> CompositionRequest {
    let manifest: ManifestRecord = bincode::deserialize(&dataset::manifest()).unwrap();
    CompositionRequest {
        parent: None,
        source: CompositionSource::All,
        schema: Some(manifest.schema),
        samples: manifest.samples.into_iter().map(SampleChange::Upsert).collect(),
        splits: vec![],
        policy: SplitPolicy::default(),
    }
}
fn counts(stats: &metadata::TestMutationGate) -> (usize, usize) {
    (
        stats.reads.load(Ordering::Relaxed) + stats.bounded_reads.load(Ordering::Relaxed),
        stats.writes.load(Ordering::Relaxed),
    )
}
fn delta(stats: &metadata::TestMutationGate, old: (usize, usize)) -> (usize, usize) {
    let now = counts(stats);
    (now.0 - old.0, now.1 - old.1)
}
async fn fixture() -> (DatasetClient, Arc<metadata::TestMutationGate>, Arc<AtomicBool>) {
    let stats = Arc::new(metadata::TestMutationGate::default());
    stats.reject_workflow_writes.store(true, Ordering::Relaxed);
    let loss = Arc::new(AtomicBool::new(false));
    let metadata = metadata::client_with_loss_and_gate(loss.clone(), stats.clone()).await;
    let runtime = storage::runtime_with_metadata(metadata);
    let client = DatasetClient::new(
        &runtime,
        DatasetIdentity::new(NamespacePath::root(), "resources").unwrap(),
    )
    .unwrap();
    client.create(context(1)).await.unwrap();
    (client, stats, loss)
}
async fn prepare(client: &DatasetClient, stats: &metadata::TestMutationGate) -> SnapshotId {
    let snapshot = SnapshotId::random();
    let head_reads = stats.head_reads.load(Ordering::Relaxed);
    let result = client
        .prepare_snapshot(context(2), snapshot, &input())
        .await
        .unwrap();
    assert_eq!(result.snapshot, snapshot);
    assert_eq!(result.sample_count, 3);
    let before = counts(stats);
    assert_eq!(
        client
            .prepare_snapshot(context(3), snapshot, &input())
            .await
            .unwrap(),
        result
    );
    assert_eq!(delta(stats, before).1, 0);
    assert_eq!(stats.head_reads.load(Ordering::Relaxed), head_reads);
    client.snapshot(context(4), snapshot).await.unwrap();
    snapshot
}

#[tokio::test]
async fn complete_resources_and_conflict_observations_need_no_workflow_or_confirmation_get() {
    let (client, stats, _) = fixture().await;
    let first = prepare(&client, &stats).await;
    let second = prepare(&client, &stats).await;
    let before = counts(&stats);
    let head = client
        .update_snapshot_head(
            context(5),
            &SnapshotHeadUpdate {
                snapshot: first,
                expected_head: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(delta(&stats, before), (3, 1));
    for _ in 0..4 {
        let before = counts(&stats);
        let error = client
            .update_snapshot_head(
                context(6),
                &SnapshotHeadUpdate {
                    snapshot: second,
                    expected_head: None,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(
            error,
            Error::SnapshotHeadConflict {
                observed: Box::new(head.clone())
            }
        );
        assert_eq!(delta(&stats, before), (3, 0));
    }
    let before = counts(&stats);
    let next = client
        .update_snapshot_head(
            context(7),
            &SnapshotHeadUpdate {
                snapshot: second,
                expected_head: head.generation,
            },
        )
        .await
        .unwrap();
    assert_eq!(next.latest, Some(second));
    assert_eq!(delta(&stats, before), (3, 1));

    assert_split_revisions(&client, &stats, second).await;
}

async fn assert_split_revisions(
    client: &DatasetClient,
    stats: &metadata::TestMutationGate,
    snapshot: SnapshotId,
) {
    let prepare = |revision| SplitPreparation {
        revision,
        policy: SplitPolicy::default(),
        splits: vec![DraftSplit {
            name: SplitName::new("train").unwrap(),
            definition: SplitDefinition::Selection(Selection::Explicit(vec![vec![1], vec![2], vec![3]])),
            origin: MembershipOrigin::Evaluated,
        }],
    };
    let a = SplitRevisionId::random();
    let b = SplitRevisionId::random();
    let prepared = client
        .prepare_split_revision(context(8), snapshot, &prepare(a))
        .await
        .unwrap();
    let before = counts(stats);
    assert_eq!(
        client
            .prepare_split_revision(context(9), snapshot, &prepare(a))
            .await
            .unwrap(),
        prepared
    );
    assert_eq!(delta(stats, before).1, 0);
    client
        .prepare_split_revision(context(10), snapshot, &prepare(b))
        .await
        .unwrap();
    let head = client
        .update_split_head(
            context(11),
            snapshot,
            &SplitHeadUpdate {
                revision: a,
                expected_revision: None,
                expected_head: None,
            },
        )
        .await
        .unwrap();
    let before = counts(stats);
    let error = client
        .update_split_head(
            context(12),
            snapshot,
            &SplitHeadUpdate {
                revision: b,
                expected_revision: None,
                expected_head: None,
            },
        )
        .await
        .unwrap_err();
    let Error::SplitHeadConflict {
        snapshot: observed_snapshot,
        observed,
    } = error
    else {
        panic!("missing observation: {error:?}")
    };
    assert_eq!(observed_snapshot, snapshot);
    assert_eq!(*observed, head);
    assert_eq!(delta(stats, before), (4, 0));
    let before = counts(stats);
    let next = client
        .update_split_head(
            context(13),
            snapshot,
            &SplitHeadUpdate {
                revision: b,
                expected_revision: observed.revision,
                expected_head: observed.generation,
            },
        )
        .await
        .unwrap();
    assert_eq!(next.revision, Some(b));
    assert_eq!(delta(stats, before), (4, 1));
}

#[tokio::test]
async fn an_applied_head_with_a_lost_reply_stays_uncertain_and_replays_the_original_condition() {
    let (client, stats, loss) = fixture().await;
    let snapshot = prepare(&client, &stats).await;
    let update = SnapshotHeadUpdate {
        snapshot,
        expected_head: None,
    };
    let before = counts(&stats);
    loss.store(true, Ordering::Relaxed);
    assert!(matches!(
        client.update_snapshot_head(context(20), &update).await,
        Err(Error::UncertainMutation { request_id: 20, .. })
    ));
    assert_eq!(delta(&stats, before), (3, 1));
    loss.store(false, Ordering::Relaxed);
    let before = counts(&stats);
    let head = client.update_snapshot_head(context(21), &update).await.unwrap();
    assert_eq!(head.latest, Some(snapshot));
    assert_eq!(delta(&stats, before), (3, 0));
}
