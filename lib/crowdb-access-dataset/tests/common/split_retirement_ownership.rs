// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{budget, retirement_fixture::TestFixture, split_store};
use crowdb_access_dataset::{composition::*, split::*, *};
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test]
async fn immutable_initial_revision_remains_protected_after_current_moves() {
    let fixture = TestFixture::new().await;
    let snapshot = SnapshotId::random();
    let result = fixture
        .authority
        .prepare_composition_resource(
            &fixture.identity,
            snapshot,
            &CompositionRequest {
                parent: Some(fixture.snapshot),
                source: CompositionSource::All,
                schema: None,
                samples: vec![],
                splits: split_store::partition_splits(),
                policy: SplitPolicy::default(),
            },
            None,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    let initial = result.initial_split.unwrap();
    let next = split_store::prepared(&fixture.authority, &fixture.identity, snapshot, Some(initial)).await;
    fixture
        .authority
        .update_split_head(
            &fixture.identity,
            snapshot,
            &SplitHeadUpdate {
                revision: next.revision,
                expected_revision: Some(initial),
                expected_head: None,
            },
        )
        .await
        .unwrap();
    let before = fixture.store.counts();
    assert!(matches!(
        fixture
            .authority
            .delete_split_revision(&fixture.identity, snapshot, initial, &ReadCancellation::new())
            .await,
        Err(SplitLifecycleError::Frozen)
    ));
    let after = fixture.store.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (4, 0));
    assert!(fixture
        .authority
        .get_split_revision(
            &fixture.identity,
            snapshot,
            initial,
            budget(),
            &ReadCancellation::new()
        )
        .await
        .is_ok());
}

#[tokio::test]
async fn a_completed_inherited_revision_owns_its_members_after_source_deletion() {
    let fixture = TestFixture::new().await;
    let child = fixture
        .authority
        .prepare_snapshot_candidate(
            &fixture.identity,
            Some(fixture.snapshot),
            fixture
                .authority
                .get_snapshot(&fixture.identity, fixture.snapshot)
                .await
                .unwrap()
                .publication
                .manifest,
            OperationId::random(),
        )
        .await
        .unwrap();
    let source = fixture
        .record
        .splits
        .iter()
        .find(|split| split.count > 0)
        .unwrap();
    let mut input = SplitPreparation {
        revision: SplitRevisionId::random(),
        policy: SplitPolicy::default(),
        splits: vec![DraftSplit {
            name: source.name.clone(),
            definition: source.definition.clone(),
            origin: MembershipOrigin::Inherited(SplitSource {
                snapshot: fixture.snapshot,
                revision: fixture.record.revision,
                name: source.name.clone(),
            }),
        }],
    };
    let inherited = fixture
        .authority
        .prepare_split_revision(&fixture.identity, child, &input, &ReadCancellation::new())
        .await
        .unwrap();
    fixture
        .authority
        .delete_split_revision(
            &fixture.identity,
            fixture.snapshot,
            fixture.record.revision,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    fixture.clock.store(2200, Ordering::Release);
    fixture.finish().await;
    let members = fixture
        .authority
        .split_members_page(
            &fixture.identity,
            child,
            inherited.revision,
            &source.name,
            None,
            budget(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(u64::try_from(members.entries.len()).unwrap(), source.count);
    input.revision = SplitRevisionId::random();
    assert!(fixture
        .authority
        .prepare_split_revision(&fixture.identity, child, &input, &ReadCancellation::new())
        .await
        .is_err());
}

#[tokio::test]
async fn owner_grace_reclaims_an_unknown_revision_written_after_owner_cleanup() {
    let fixture = TestFixture::new().await;
    let input = SplitPreparation {
        revision: SplitRevisionId::random(),
        policy: SplitPolicy::default(),
        splits: split_store::partition_splits(),
    };
    let gate = fixture.store.pause(b"DS\0SR\0", None, false);
    let authority = fixture.authority.clone();
    let identity = fixture.identity.clone();
    let snapshot = fixture.snapshot;
    let revision = input.revision;
    let preparation = tokio::spawn(async move {
        authority
            .prepare_split_revision(&identity, snapshot, &input, &ReadCancellation::new())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), gate.entered())
        .await
        .unwrap();
    fixture
        .authority
        .begin_reclaim(&fixture.identity, snapshot)
        .await
        .unwrap();
    fixture.clock.store(2200, Ordering::Release);
    let mut progress = MetadataCleanupProgress::new(snapshot);
    for _ in 0..100 {
        if fixture
            .authority
            .reclaim_snapshot_metadata_step_for_tests(
                &fixture.identity,
                snapshot,
                &mut progress,
                &ReadCancellation::new(),
            )
            .await
            .unwrap()
            .complete
        {
            break;
        }
    }
    assert!(progress.complete);
    gate.release();
    preparation.await.unwrap().unwrap();
    let root = key::revision(&fixture.identity, snapshot, revision).unwrap();
    let live = fixture.store.inner.get(&root).await.unwrap().unwrap();
    assert!(SplitRevisionRecord::decode(&live.bytes).is_ok());
    assert!(fixture
        .authority
        .delete_split_revision(&fixture.identity, snapshot, revision, &ReadCancellation::new())
        .await
        .is_err());
    let restarted =
        DatasetAuthority::new(fixture.store.clone()).with_reclaim_clock_for_tests(fixture.clock.clone());
    let mut sweep = SplitRevisionReclaimSweep::default();
    for _ in 0..80 {
        restarted
            .reclaim_split_revisions_page(&mut sweep, &ReadCancellation::new())
            .await
            .unwrap();
    }
    assert_eq!(
        fixture.store.inner.get(&root).await.unwrap().unwrap().bytes,
        b"split-retired-v1"
    );
}

#[tokio::test]
async fn owner_deadline_takes_priority_over_a_later_independent_split_deadline() {
    let fixture = TestFixture::new().await;
    fixture.clock.store(1100, Ordering::Release);
    let marked = fixture
        .authority
        .delete_split_revision(
            &fixture.identity,
            fixture.snapshot,
            fixture.record.revision,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(marked.reclaim_after, Some(2300));
    // Independent Access instances may observe different wall clocks. The owner
    // fence and its durable deadline remain authoritative for all owned metadata.
    let owner = DatasetAuthority::new(fixture.store.clone())
        .with_reclaim_clock_for_tests(std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1000)));
    owner
        .begin_reclaim(&fixture.identity, fixture.snapshot)
        .await
        .unwrap();
    fixture.clock.store(2200, Ordering::Release);
    assert_eq!(
        fixture
            .authority
            .snapshot_reclaim_after(&fixture.identity, fixture.snapshot)
            .await
            .unwrap(),
        2200
    );
    fixture.finish().await;
    fixture.assert_terminal().await;
}
