// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{split::*, *};
use split_store::{fixture, prepared};
use std::sync::atomic::Ordering;

fn update(revision: SplitRevisionId, base: &SplitReference) -> SplitHeadUpdate {
    SplitHeadUpdate {
        revision,
        expected_revision: base.revision,
        expected_head: base.generation,
    }
}

#[tokio::test]
async fn independent_revision_cas_and_historical_conflicts_never_move_data_head() {
    let (_, authority, identity, snapshot) = fixture().await;
    let data_head = authority.inspect_head(&identity).await.unwrap();
    let first = prepared(&authority, &identity, snapshot, None).await;
    let competing = prepared(&authority, &identity, snapshot, None).await;
    let original = authority.inspect_split_head(&identity, snapshot).await.unwrap();
    let first_update = update(first.revision, &original);
    let stale = update(competing.revision, &original);
    let first_head = authority
        .update_split_head(&identity, snapshot, &first_update)
        .await
        .unwrap();
    assert!(matches!(
        authority.update_split_head(&identity, snapshot, &stale).await,
        Err(SplitLifecycleError::HeadConflict { observed, .. }) if *observed == first_head
    ));
    assert_eq!(
        authority
            .get_split_revision(
                &identity,
                snapshot,
                competing.revision,
                budget(),
                &ReadCancellation::new()
            )
            .await
            .unwrap(),
        competing
    );
    let next = prepared(&authority, &identity, snapshot, Some(first.revision)).await;
    let current = authority
        .update_split_head(&identity, snapshot, &update(next.revision, &first_head))
        .await
        .unwrap();
    assert_eq!(current.revision, Some(next.revision));
    assert!(matches!(
        authority.update_split_head(&identity, snapshot, &first_update).await,
        Err(SplitLifecycleError::HeadConflict { observed, .. }) if *observed == current
    ));
    assert_eq!(
        authority.inspect_split_head(&identity, snapshot).await.unwrap(),
        current
    );
    assert_eq!(authority.inspect_head(&identity).await.unwrap(), data_head);
}

#[tokio::test]
async fn lost_split_cas_reply_retries_current_effect_without_a_request_ledger() {
    let (store, authority, identity, snapshot) = fixture().await;
    let record = prepared(&authority, &identity, snapshot, None).await;
    let original = authority.inspect_split_head(&identity, snapshot).await.unwrap();
    let input = update(record.revision, &original);
    store.lose_split_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .update_split_head(&identity, snapshot, &input)
        .await
        .is_err());
    store.inspect_only.store(true, Ordering::Relaxed);
    store.reject_split_proof_reads.store(true, Ordering::Relaxed);
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
    store.inspect_only.store(false, Ordering::Relaxed);
    store.reject_split_proof_reads.store(false, Ordering::Relaxed);
    let head = authority.inspect_split_head(&identity, snapshot).await.unwrap();
    let restarted = DatasetAuthority::new(store.clone());
    assert_eq!(
        restarted
            .update_split_head(&identity, snapshot, &input)
            .await
            .unwrap(),
        head
    );
    assert_eq!(
        restarted
            .update_split_head(&identity, snapshot, &input)
            .await
            .unwrap(),
        head
    );
    assert_eq!(
        restarted.inspect_split_head(&identity, snapshot).await.unwrap(),
        head
    );
    for prefix in [b"DS\0CA\0".as_slice(), b"DS\0CR\0", b"DS\0SD\0"] {
        assert!(store
            .scan_page(
                &MetadataScan {
                    prefix: prefix.to_vec(),
                    after: None,
                    budget: budget()
                },
                &ReadCancellation::new()
            )
            .await
            .unwrap()
            .entries
            .is_empty());
    }
}

#[tokio::test]
async fn replacing_current_split_does_not_write_or_help_receipts() {
    let (store, authority, identity, snapshot) = fixture().await;
    let first = prepared(&authority, &identity, snapshot, None).await;
    let original = authority.inspect_split_head(&identity, snapshot).await.unwrap();
    let input = update(first.revision, &original);
    store.reject_receipts.store(true, Ordering::Relaxed);
    let old = authority
        .update_split_head(&identity, snapshot, &input)
        .await
        .unwrap();
    let next = prepared(&authority, &identity, snapshot, Some(first.revision)).await;
    let current = authority
        .update_split_head(&identity, snapshot, &update(next.revision, &old))
        .await
        .unwrap();
    assert_ne!(current, old);
    assert!(matches!(
        authority.update_split_head(&identity, snapshot, &input).await,
        Err(SplitLifecycleError::HeadConflict { observed, .. }) if *observed == current
    ));
    assert_eq!(
        authority.inspect_split_head(&identity, snapshot).await.unwrap(),
        current
    );
}

fn budget() -> MetadataScanBudget {
    MetadataScanBudget {
        items: 20,
        bytes: 1024 * 1024,
        timeout: std::time::Duration::from_secs(2),
    }
}
