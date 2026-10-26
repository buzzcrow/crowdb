// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/legacy_split.rs"]
mod legacy;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;

use crowdb_access_dataset::{composition::*, split::*, *};
use std::sync::atomic::Ordering;

async fn cleanup(authority: &DatasetAuthority, identity: &DatasetIdentity, operation: OperationId) {
    let mut cursor = SplitReclaimCursor::new(identity.clone(), operation);
    for _ in 0..100 {
        if authority
            .cleanup_split_scratch_step_for_tests(&mut cursor, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            return;
        }
    }
    panic!("Split cleanup did not finish");
}

async fn interrupted_publication(
    store: &dyn DatasetStore,
    identity: &DatasetIdentity,
    record: &SplitRevisionRecord,
) {
    let key = legacy::state_key(b"DS\0SD\0", identity, record.operation);
    let old = store.get(&key).await.unwrap().unwrap();
    let mut state: serde_json::Value = serde_json::from_slice(&old.bytes).unwrap();
    state["record"]["progress"] = serde_json::json!(DraftProgress::Committing {
        target: CommitTarget::SplitRevision {
            snapshot: record.snapshot,
            revision: record.revision
        },
        attempt: OperationId::random()
    });
    assert!(matches!(
        store
            .compare_exchange(&key, Some(&old.bytes), &serde_json::to_vec(&state).unwrap())
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
}

#[tokio::test]
async fn losing_pointer_cas_keeps_result_readable_without_split_proofs_or_writes() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let winner = split_store::prepared(&authority, &identity, snapshot, None).await;
    let candidate = legacy::prepared(store.as_ref(), &authority, &identity, snapshot, None).await;
    let attempt = split_store::attempt(&authority, &identity, &candidate).await;
    split_store::publish_revision(&authority, &identity, &winner).await;
    assert!(matches!(
        authority
            .update_split_head(
                &identity,
                snapshot,
                &SplitHeadUpdate {
                    revision: candidate.revision,
                    expected_revision: None,
                    expected_head: attempt.expected_head
                }
            )
            .await,
        Err(SplitLifecycleError::HeadConflict { .. })
    ));
    let head = authority.inspect_split_head(&identity, snapshot).await.unwrap();
    assert_eq!(head.revision, Some(winner.revision));
    interrupted_publication(store.as_ref(), &identity, &candidate).await;
    authority
        .abandon_split_draft(&identity, candidate.operation, 1)
        .await
        .unwrap();
    cleanup(&authority, &identity, candidate.operation).await;
    store.reject_split_proof_reads.store(true, Ordering::Relaxed);
    store.inspect_only.store(true, Ordering::Relaxed);
    let before = store.revision_reads.load(Ordering::Relaxed);
    let total = store.bounded_reads.load(Ordering::Relaxed);
    let restarted = DatasetAuthority::new(store.clone());
    assert_eq!(
        restarted
            .get_split_revision(
                &identity,
                snapshot,
                candidate.revision,
                budget(),
                &ReadCancellation::new()
            )
            .await
            .unwrap(),
        candidate
    );
    assert_eq!(store.revision_reads.load(Ordering::Relaxed) - before, 1);
    assert_eq!(store.bounded_reads.load(Ordering::Relaxed) - total, 3);
    let split = candidate.splits.iter().find(|s| s.count > 0).unwrap();
    let mut after = None;
    let mut members = Vec::new();
    loop {
        let page = restarted
            .split_members_page(
                &identity,
                snapshot,
                candidate.revision,
                &split.name,
                after,
                MetadataScanBudget { items: 1, ..budget() },
                &ReadCancellation::new(),
            )
            .await
            .unwrap();
        members.extend(page.entries);
        after = page.after;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(members.len() as u64, split.count);
    store.inspect_only.store(false, Ordering::Relaxed);
    store.reject_split_proof_reads.store(false, Ordering::Relaxed);
    assert_eq!(
        restarted.inspect_split_head(&identity, snapshot).await.unwrap(),
        head
    );
}

#[tokio::test]
async fn complete_root_with_lost_reply_survives_abandonment_and_cleanup_cas_race() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let operation = OperationId::random();
    let input = SplitPreparation {
        revision: legacy::revision(operation, 1),
        policy: SplitPolicy::default(),
        splits: split_store::partition_splits(),
    };
    store.lose_revision_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .prepare_split_revision(&identity, snapshot, &input, &ReadCancellation::new())
        .await
        .is_err());
    let roots = store
        .scan_page(
            &MetadataScan {
                prefix: b"DS\0SR\0".to_vec(),
                after: None,
                budget: budget(),
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(roots.entries.len(), 1);
    let record = SplitRevisionRecord::decode(&roots.entries[0].1.bytes).unwrap();
    let record = legacy::install(store.as_ref(), &identity, operation, 1, None, &record).await;
    authority
        .abandon_split_draft(&identity, operation, 1)
        .await
        .unwrap();
    // Cleanup reads absence, then loses its create-if-absent to the completed root.
    store.hide_revision_once.store(true, Ordering::Relaxed);
    let reads = store.revision_reads.load(Ordering::Relaxed);
    authority
        .cleanup_split_scratch_step_for_tests(
            &mut SplitReclaimCursor::new(identity.clone(), operation),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(store.revision_reads.load(Ordering::Relaxed) - reads, 1);
    cleanup(&DatasetAuthority::new(store.clone()), &identity, operation).await;
    let restarted = DatasetAuthority::new(store.clone());
    store.inspect_only.store(true, Ordering::Relaxed);
    store.reject_split_proof_reads.store(true, Ordering::Relaxed);
    assert_eq!(
        restarted
            .get_split_revision(
                &identity,
                snapshot,
                record.revision,
                budget(),
                &ReadCancellation::new()
            )
            .await
            .unwrap(),
        record
    );
    for split in &record.splits {
        let page = restarted
            .split_members_page(
                &identity,
                snapshot,
                record.revision,
                &split.name,
                None,
                budget(),
                &ReadCancellation::new(),
            )
            .await
            .unwrap();
        assert_eq!(page.entries.len() as u64, split.count);
    }
}

#[tokio::test]
async fn cleanup_fences_missing_final_root_before_retiring_pages() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let record = legacy::prepared(store.as_ref(), &authority, &identity, snapshot, None).await;
    interrupted_publication(store.as_ref(), &identity, &record).await;
    let root = key::revision(&identity, snapshot, record.revision).unwrap();
    // Recreate a producer paused before its final immutable write.
    store.delete(&root).await.unwrap();
    store
        .delete(&key::revision_summary(&identity, snapshot, record.revision).unwrap())
        .await
        .unwrap();
    authority
        .abandon_split_draft(&identity, record.operation, 1)
        .await
        .unwrap();
    cleanup(&authority, &identity, record.operation).await;
    assert!(matches!(
        store
            .compare_exchange(&root, None, &record.encode().unwrap())
            .await
            .unwrap(),
        CasOutcome::Conflict(Some(_))
    ));
    assert!(matches!(
        authority
            .get_split_revision(
                &identity,
                snapshot,
                record.revision,
                budget(),
                &ReadCancellation::new()
            )
            .await,
        Err(SplitLifecycleError::NotFound)
    ));
    for split in &record.splits {
        for page in &split.pages {
            let key =
                key::membership_page(&identity, snapshot, record.revision, &split.name, page.page).unwrap();
            assert!(MembershipPage::decode(&store.get(&key).await.unwrap().unwrap().bytes).is_err());
        }
    }
}

#[tokio::test]
async fn unpublished_revision_can_supply_frozen_child_membership() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let source = split_store::prepared(&authority, &identity, parent, None).await;
    let child = SnapshotId::random();
    let result = authority
        .prepare_composition_resource(
            &identity,
            child,
            &CompositionRequest {
                parent: Some(parent),
                source: CompositionSource::All,
                schema: None,
                samples: vec![],
                splits: source
                    .splits
                    .iter()
                    .map(|split| DraftSplit {
                        name: split.name.clone(),
                        definition: split.definition.clone(),
                        origin: MembershipOrigin::Inherited(SplitSource {
                            snapshot: parent,
                            revision: source.revision,
                            name: split.name.clone(),
                        }),
                    })
                    .collect(),
                policy: source.policy,
            },
            None,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    let head = authority.inspect_head(&identity).await.unwrap();
    authority
        .update_snapshot_head(
            &identity,
            &SnapshotHeadUpdate {
                snapshot: child,
                expected_head: head.generation,
            },
        )
        .await
        .unwrap();
    let revision = result.initial_split.unwrap();
    let child_splits = authority
        .get_split_revision(&identity, child, revision, budget(), &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(
        child_splits.splits.iter().map(|s| s.count).sum::<u64>(),
        source.splits.iter().map(|s| s.count).sum::<u64>()
    );
    assert!(child_splits.splits.iter().all(|s| matches!(&s.origin, MembershipOrigin::Inherited(origin) if origin.snapshot == parent && origin.revision == source.revision)));
    let root = SnapshotId::random();
    let manifest: ManifestRecord = bincode::deserialize(&publication::publication_manifest(7)).unwrap();
    authority
        .prepare_composition_resource(
            &identity,
            root,
            &CompositionRequest {
                parent: None,
                source: CompositionSource::All,
                schema: Some(manifest.schema),
                samples: manifest.samples.into_iter().map(SampleChange::Upsert).collect(),
                splits: vec![],
                policy: SplitPolicy::default(),
            },
            None,
            &ReadCancellation::new(),
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
    assert!(matches!(
        authority.begin_reclaim(&identity, parent).await,
        Err(AuthorityError::SnapshotProtected)
    ));
    authority.begin_reclaim(&identity, child).await.unwrap();
    assert!(authority
        .get_split_revision(&identity, child, revision, budget(), &ReadCancellation::new())
        .await
        .is_err());
}

fn budget() -> MetadataScanBudget {
    MetadataScanBudget {
        items: 20,
        bytes: 1024 * 1024,
        timeout: std::time::Duration::from_secs(2),
    }
}
