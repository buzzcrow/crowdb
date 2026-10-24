// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{split::*, *};

async fn finish(authority: &DatasetAuthority, identity: &DatasetIdentity, operation: OperationId) {
    for _ in 0..100 {
        if authority
            .cleanup_split_scratch_step(identity, operation, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            return;
        }
    }
    panic!("Split cleanup did not finish");
}

#[tokio::test]
async fn abandoning_complete_split_preserves_result_and_membership() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let record = split_store::prepared(&authority, &identity, snapshot, None).await;
    let record_key = key::revision(&identity, snapshot, record.revision).unwrap();
    let member = record.splits.iter().find(|s| s.count > 0).unwrap();
    let page_key = key::membership_page(&identity, snapshot, record.revision, &member.name, 0).unwrap();
    let original = store.get(&page_key).await.unwrap().unwrap();
    authority
        .abandon_split_draft(&identity, record.operation, 1)
        .await
        .unwrap();
    finish(&authority, &identity, record.operation).await;
    assert_eq!(store.get(&page_key).await.unwrap().unwrap().bytes, original.bytes);
    assert!(matches!(
        store
            .compare_exchange(&page_key, None, &original.bytes)
            .await
            .unwrap(),
        CasOutcome::Conflict(_)
    ));
    assert!(matches!(
        store
            .compare_exchange(&record_key, None, &record.encode().unwrap())
            .await
            .unwrap(),
        CasOutcome::Conflict(_)
    ));
    assert_eq!(
        authority
            .split_revision_history(&identity, snapshot, None, budget(), &ReadCancellation::new())
            .await
            .unwrap()
            .entries
            .len(),
        1
    );
    assert_eq!(
        authority
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
    finish(&DatasetAuthority::new(store), &identity, record.operation).await;
}

#[tokio::test]
async fn obsolete_split_cleanup_keeps_current_and_historical_committed_memberships() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let prior = split_store::prepared(&authority, &identity, snapshot, None).await;
    split_store::publish_revision(&authority, &identity, &prior).await;
    finish(&authority, &identity, prior.operation).await;
    let operation = OperationId::random();
    authority
        .begin_split_draft(
            &identity,
            operation,
            snapshot,
            Some(prior.revision),
            SplitPolicy::default(),
            split_store::partition_splits(),
        )
        .await
        .unwrap();
    authority
        .evaluate_split_draft(&identity, operation, 1, 0, &ReadCancellation::new())
        .await
        .unwrap();
    authority
        .edit_split_draft(
            &identity,
            operation,
            &SplitDraftEdit {
                id: OperationId::random(),
                expected_revision: 1,
                policy: SplitPolicy::default(),
                splits: vec![],
            },
        )
        .await
        .unwrap();
    finish(&authority, &identity, operation).await;
    authority
        .evaluate_split_draft(&identity, operation, 2, 0, &ReadCancellation::new())
        .await
        .unwrap();
    let next = authority
        .prepare_split_draft(&identity, operation, 2)
        .await
        .unwrap();
    split_store::publish_revision(&authority, &identity, &next).await;
    let restarted = DatasetAuthority::new(store);
    finish(&restarted, &identity, operation).await;
    assert_eq!(
        restarted
            .get_split_revision(
                &identity,
                snapshot,
                prior.revision,
                budget(),
                &ReadCancellation::new()
            )
            .await
            .unwrap(),
        prior
    );
    assert_eq!(
        restarted
            .get_split_revision(
                &identity,
                snapshot,
                next.revision,
                budget(),
                &ReadCancellation::new()
            )
            .await
            .unwrap(),
        next
    );
}

#[tokio::test]
async fn cleanup_chunks_large_overlap_windows() {
    let (_, authority, identity, snapshot) = split_store::fixture().await;
    let operation = OperationId::random();
    let splits = (0..45)
        .map(|index| DraftSplit {
            name: SplitName::new(format!("split-{index}")).unwrap(),
            definition: SplitDefinition::Selection(Selection::Prefix(b"sample".to_vec())),
            origin: MembershipOrigin::Evaluated,
        })
        .collect();
    authority
        .begin_split_draft(
            &identity,
            operation,
            snapshot,
            None,
            SplitPolicy {
                overlap: OverlapPolicy::AllowOverlap,
                coverage: CoveragePolicy::AllowUnassigned,
            },
            splits,
        )
        .await
        .unwrap();
    authority
        .evaluate_split_draft(&identity, operation, 1, 0, &ReadCancellation::new())
        .await
        .unwrap();
    authority
        .abandon_split_draft(&identity, operation, 1)
        .await
        .unwrap();
    for _ in 0..2 {
        let progress = authority
            .cleanup_split_scratch_step(&identity, operation, &ReadCancellation::new())
            .await
            .unwrap();
        assert!(!progress.complete);
        assert_eq!(progress.next_page, 0);
    }
    finish(&authority, &identity, operation).await;
}
fn budget() -> MetadataScanBudget {
    MetadataScanBudget {
        items: 20,
        bytes: 1024 * 1024,
        timeout: std::time::Duration::from_secs(2),
    }
}
