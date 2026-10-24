// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/legacy_split.rs"]
mod legacy;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::DraftProgress, split::*, *};

async fn finish(authority: &DatasetAuthority, identity: &DatasetIdentity, operation: OperationId) {
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

#[tokio::test]
async fn abandoning_complete_split_preserves_result_and_membership() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let record = legacy::prepared(store.as_ref(), &authority, &identity, snapshot, None).await;
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
    let prior = legacy::prepared(store.as_ref(), &authority, &identity, snapshot, None).await;
    split_store::publish_revision(&authority, &identity, &prior).await;
    finish(&authority, &identity, prior.operation).await;
    let operation = OperationId::random();
    let old = authority
        .prepare_split_revision(
            &identity,
            snapshot,
            &SplitPreparation {
                revision: legacy::revision(operation, 1),
                policy: SplitPolicy::default(),
                splits: split_store::partition_splits(),
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    legacy::install(
        store.as_ref(),
        &identity,
        operation,
        1,
        Some(prior.revision),
        &old,
    )
    .await;
    legacy::remove_root(store.as_ref(), &identity, &old).await;
    legacy::state(
        store.as_ref(),
        &identity,
        &SplitDraftRecord {
            version: 1,
            operation,
            snapshot,
            expected_revision: Some(prior.revision),
            revision: 2,
            policy: SplitPolicy::default(),
            splits: vec![],
            progress: DraftProgress::Editing,
        },
    )
    .await;
    finish(&authority, &identity, operation).await;
    for split in &old.splits {
        for page in &split.pages {
            let key =
                key::membership_page(&identity, snapshot, old.revision, &split.name, page.page).unwrap();
            assert_eq!(store.get(&key).await.unwrap().unwrap().bytes, b"split-retired-v1");
        }
    }
    let next = authority
        .prepare_split_revision(
            &identity,
            snapshot,
            &SplitPreparation {
                revision: legacy::revision(operation, 2),
                policy: SplitPolicy::default(),
                splits: vec![],
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    let next = legacy::install(
        store.as_ref(),
        &identity,
        operation,
        2,
        Some(prior.revision),
        &next,
    )
    .await;
    split_store::publish_revision(&authority, &identity, &next).await;
    let restarted = DatasetAuthority::new(store);
    finish(&restarted, &identity, operation).await;
    for expected in [&prior, &next] {
        assert_eq!(
            &restarted
                .get_split_revision(
                    &identity,
                    snapshot,
                    expected.revision,
                    budget(),
                    &ReadCancellation::new()
                )
                .await
                .unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn cleanup_chunks_large_overlap_windows() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let operation = OperationId::random();
    let splits = (0..45)
        .map(|index| DraftSplit {
            name: SplitName::new(format!("split-{index}")).unwrap(),
            definition: SplitDefinition::Selection(Selection::Prefix(b"sample".to_vec())),
            origin: MembershipOrigin::Evaluated,
        })
        .collect();
    let record = authority
        .prepare_split_revision(
            &identity,
            snapshot,
            &SplitPreparation {
                revision: legacy::revision(operation, 1),
                policy: SplitPolicy {
                    overlap: OverlapPolicy::AllowOverlap,
                    coverage: CoveragePolicy::AllowUnassigned,
                },
                splits,
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    let record = legacy::install(store.as_ref(), &identity, operation, 1, None, &record).await;
    legacy::remove_root(store.as_ref(), &identity, &record).await;
    authority
        .abandon_split_draft(&identity, operation, 1)
        .await
        .unwrap();
    let mut cursor = SplitReclaimCursor::new(identity.clone(), operation);
    for _ in 0..2 {
        let progress = authority
            .cleanup_split_scratch_step_for_tests(&mut cursor, &ReadCancellation::new())
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
