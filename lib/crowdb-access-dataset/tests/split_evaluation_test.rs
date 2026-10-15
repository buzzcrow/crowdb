// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::DraftProgress, split::*, *};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn split_evaluation_resumes_pages_and_prepares_complete_immutable_membership() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let mut manifest: ManifestRecord = bincode::deserialize(&publication::publication_manifest(1)).unwrap();
    manifest.samples = (0..45u8)
        .map(|id| SampleRecord {
            sample_id: vec![id],
            fields: manifest.samples[0].fields.clone(),
        })
        .collect();
    let snapshot = authority
        .publish_snapshot(&identity, Some(parent), bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    let operation = OperationId::random();
    let splits = split_store::partition_splits();
    let policy = SplitPolicy {
        overlap: OverlapPolicy::Disjoint,
        coverage: CoveragePolicy::RequireComplete,
    };
    authority
        .begin_split_draft(&identity, operation, snapshot, None, policy, splits)
        .await
        .unwrap();
    assert!(matches!(
        authority.prepare_split_draft(&identity, operation, 1).await,
        Err(SplitLifecycleError::NotEvaluated)
    ));
    let cancelled = ReadCancellation::new();
    cancelled.cancel();
    assert!(authority
        .evaluate_split_draft(&identity, operation, 1, 0, &cancelled)
        .await
        .is_err());
    assert_eq!(
        authority
            .split_draft_status(&identity, operation)
            .await
            .unwrap()
            .progress,
        DraftProgress::Editing
    );
    store.lose_edit_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .evaluate_split_draft(&identity, operation, 1, 0, &ReadCancellation::new())
        .await
        .is_err());
    let restarted = DatasetAuthority::new(store);
    let recovered = restarted
        .evaluate_split_draft(&identity, operation, 1, 0, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(recovered.validation.scanned, 20);
    assert!(!recovered.complete);
    let mut preview = recovered;
    while !preview.complete {
        preview = restarted
            .evaluate_split_draft(
                &identity,
                operation,
                1,
                preview.next_page,
                &ReadCancellation::new(),
            )
            .await
            .unwrap();
    }
    assert_eq!(preview.validation.scanned, 45);
    assert_eq!(preview.validation.overlapping, 0);
    assert_eq!(preview.validation.unassigned, 0);
    assert_eq!(preview.member_counts.iter().sum::<u64>(), 45);
    let prepared = restarted
        .prepare_split_draft(&identity, operation, 1)
        .await
        .unwrap();
    assert_eq!(prepared.splits.iter().map(|s| s.count).sum::<u64>(), 45);
    assert_eq!(
        restarted
            .prepare_split_draft(&identity, operation, 1)
            .await
            .unwrap(),
        prepared
    );
    assert!(matches!(
        restarted
            .edit_split_draft(
                &identity,
                operation,
                &SplitDraftEdit {
                    id: OperationId::random(),
                    expected_revision: 1,
                    policy,
                    splits: vec![]
                }
            )
            .await,
        Err(SplitLifecycleError::Frozen)
    ));
}

#[tokio::test]
async fn policy_violations_block_prepare_and_edits_invalidate_preview() {
    let (_, authority, identity, snapshot) = split_store::fixture().await;
    let operation = OperationId::random();
    let splits = ["train", "test"]
        .into_iter()
        .map(|name| DraftSplit {
            name: SplitName::new(name).unwrap(),
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
            SplitPolicy::default(),
            splits,
        )
        .await
        .unwrap();
    let preview = authority
        .evaluate_split_draft(&identity, operation, 1, 0, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(preview.complete);
    assert_eq!(preview.validation.overlapping, 1);
    assert!(matches!(
        authority.prepare_split_draft(&identity, operation, 1).await,
        Err(SplitLifecycleError::PolicyViolation)
    ));
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
    assert!(matches!(
        authority.prepare_split_draft(&identity, operation, 2).await,
        Err(SplitLifecycleError::NotEvaluated)
    ));
    assert!(matches!(
        authority
            .evaluate_split_draft(&identity, operation, 1, 1, &ReadCancellation::new())
            .await,
        Err(SplitLifecycleError::Conflict)
    ));
    let empty = authority
        .evaluate_split_draft(&identity, operation, 2, 0, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(empty.complete);
    assert_eq!(empty.validation.unassigned, 1);
    let prepared = authority
        .prepare_split_draft(&identity, operation, 2)
        .await
        .unwrap();
    assert!(prepared.splits.is_empty());
}
