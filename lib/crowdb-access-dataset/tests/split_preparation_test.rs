// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/split_inheritance.rs"]
mod inheritance;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{split::*, *};
use std::sync::atomic::Ordering;

fn request() -> SplitPreparation {
    SplitPreparation {
        revision: SplitRevisionId::random(),
        policy: SplitPolicy::default(),
        splits: split_store::partition_splits(),
    }
}
fn pages(record: &SplitRevisionRecord) -> usize {
    record.splits.iter().map(|split| split.pages.len()).sum()
}
fn reads(store: &split_store::TestDraftStore) -> (usize, usize) {
    (
        store.bounded_reads.load(Ordering::Relaxed) + store.unbounded_reads.load(Ordering::Relaxed),
        store.metadata_scans.load(Ordering::Relaxed),
    )
}

#[tokio::test]
async fn preparation_writes_only_final_resources_without_draft_or_head_state() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let snapshot = split_store::publish_binary_samples(&authority, &identity, parent).await;
    store.reject_draft_writes.store(true, Ordering::Relaxed);
    let before = store.mutations.load(Ordering::Relaxed);
    let previous_reads = reads(&store);
    let input = request();
    let result = authority
        .prepare_split_revision(&identity, snapshot, &input, &ReadCancellation::new())
        .await
        .unwrap();
    let writes = store.mutations.load(Ordering::Relaxed) - before;
    let new_reads = (
        reads(&store).0 - previous_reads.0,
        reads(&store).1 - previous_reads.1,
    );
    assert_eq!(writes, pages(&result) + 2);
    assert_eq!(pages(&result), 6);
    assert_eq!((new_reads.0, new_reads.1, writes), (14, 6, 8));
    assert_eq!(result.splits.iter().map(|split| split.count).sum::<u64>(), 45);
    let before = store.mutations.load(Ordering::Relaxed);
    assert_eq!(
        DatasetAuthority::new(store.clone())
            .prepare_split_revision(&identity, snapshot, &input, &ReadCancellation::new())
            .await
            .unwrap(),
        result
    );
    assert_eq!(store.mutations.load(Ordering::Relaxed), before);
    assert_eq!(
        authority
            .inspect_split_head(&identity, snapshot)
            .await
            .unwrap()
            .revision,
        None
    );
    assert_eq!(
        authority
            .get_split_revision(
                &identity,
                snapshot,
                input.revision,
                budget(),
                &ReadCancellation::new()
            )
            .await
            .unwrap(),
        result
    );
    store.reject_draft_writes.store(false, Ordering::Relaxed);
    authority
        .update_split_head(
            &identity,
            snapshot,
            &SplitHeadUpdate {
                revision: result.revision,
                expected_revision: None,
                expected_head: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        authority
            .inspect_split_head(&identity, snapshot)
            .await
            .unwrap()
            .revision,
        Some(result.revision)
    );
}

#[tokio::test]
async fn an_incomplete_result_recomputes_from_caller_input_after_a_member_reply_is_lost() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let snapshot = split_store::publish_binary_samples(&authority, &identity, parent).await;
    store.reject_draft_writes.store(true, Ordering::Relaxed);
    store.lose_member_reply.store(true, Ordering::Relaxed);
    let input = request();
    assert!(authority
        .prepare_split_revision(&identity, snapshot, &input, &ReadCancellation::new())
        .await
        .is_err());
    assert!(store
        .get(&key::revision(&identity, snapshot, input.revision).unwrap())
        .await
        .unwrap()
        .is_none());
    let result = DatasetAuthority::new(store.clone())
        .prepare_split_revision(&identity, snapshot, &input, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(result.splits.iter().map(|split| split.count).sum::<u64>(), 45);
    assert_eq!(result.revision, input.revision);
}

#[tokio::test]
async fn a_lost_final_reply_has_no_confirmation_query_and_reuses_the_complete_result() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    store.reject_draft_writes.store(true, Ordering::Relaxed);
    store.lose_revision_reply.store(true, Ordering::Relaxed);
    let reads = store.revision_reads.load(Ordering::Relaxed);
    let input = request();
    assert!(authority
        .prepare_split_revision(&identity, snapshot, &input, &ReadCancellation::new())
        .await
        .is_err());
    assert_eq!(store.revision_reads.load(Ordering::Relaxed) - reads, 1);
    let before = store.mutations.load(Ordering::Relaxed);
    let restarted = DatasetAuthority::new(store.clone());
    let result = restarted
        .prepare_split_revision(&identity, snapshot, &input, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(store.mutations.load(Ordering::Relaxed), before);
    assert_eq!(result.revision, input.revision);
    let mut changed = input;
    changed.splits.clear();
    assert!(matches!(
        restarted
            .prepare_split_revision(&identity, snapshot, &changed, &ReadCancellation::new())
            .await,
        Err(SplitLifecycleError::Conflict)
    ));
    assert_eq!(store.mutations.load(Ordering::Relaxed), before);
}

#[tokio::test]
async fn cancellation_and_invalid_definitions_do_not_write_or_expose_a_revision() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    store.reject_draft_writes.store(true, Ordering::Relaxed);
    let before = store.mutations.load(Ordering::Relaxed);
    let cancel = ReadCancellation::new();
    cancel.cancel();
    let mut input = request();
    assert!(authority
        .prepare_split_revision(&identity, snapshot, &input, &cancel)
        .await
        .is_err());
    input.splits.push(input.splits[0].clone());
    assert!(authority
        .prepare_split_revision(&identity, snapshot, &input, &ReadCancellation::new())
        .await
        .is_err());
    assert_eq!(store.mutations.load(Ordering::Relaxed), before);
}

#[tokio::test]
async fn violated_policy_leaves_discoverable_result_pages_but_no_complete_root() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    store.reject_draft_writes.store(true, Ordering::Relaxed);
    let mut input = request();
    for split in &mut input.splits {
        split.definition = SplitDefinition::Selection(Selection::Explicit(vec![b"sample-1".to_vec()]));
    }
    assert!(matches!(
        authority
            .prepare_split_revision(&identity, snapshot, &input, &ReadCancellation::new())
            .await,
        Err(SplitLifecycleError::PolicyViolation)
    ));
    assert!(store
        .get(&key::revision_summary(&identity, snapshot, input.revision).unwrap())
        .await
        .unwrap()
        .is_some());
    assert!(store
        .get(&key::revision(&identity, snapshot, input.revision).unwrap())
        .await
        .unwrap()
        .is_none());
    assert!(authority
        .get_split_revision(
            &identity,
            snapshot,
            input.revision,
            budget(),
            &ReadCancellation::new()
        )
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

#[tokio::test]
async fn inherited_members_use_the_requested_revision_after_the_parent_head_changes() {
    let inheritance::TestInheritedFixture {
        authority,
        identity,
        parent,
        child,
        source,
        inherited,
    } = inheritance::fixture().await;
    let head = authority.inspect_split_head(&identity, parent).await.unwrap();
    let empty = authority
        .prepare_split_revision(
            &identity,
            parent,
            &SplitPreparation {
                revision: SplitRevisionId::random(),
                policy: SplitPolicy::default(),
                splits: Vec::new(),
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert!(empty.splits.is_empty());
    authority
        .update_split_head(
            &identity,
            parent,
            &SplitHeadUpdate {
                revision: empty.revision,
                expected_revision: Some(source.revision),
                expected_head: head.generation,
            },
        )
        .await
        .unwrap();
    let frozen = authority
        .prepare_split_revision(
            &identity,
            child,
            &SplitPreparation {
                revision: SplitRevisionId::random(),
                policy: SplitPolicy::default(),
                splits: vec![inherited.clone()],
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(frozen.splits[0].count, 1);
    assert_eq!(frozen.splits[0].origin, inherited.origin);
    let evaluated = authority
        .prepare_split_revision(
            &identity,
            child,
            &SplitPreparation {
                revision: SplitRevisionId::random(),
                policy: SplitPolicy::default(),
                splits: vec![DraftSplit {
                    origin: MembershipOrigin::Evaluated,
                    ..inherited
                }],
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(evaluated.splits[0].count, 2);
}
