// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/split_retirement_compatibility.rs"]
mod compatibility;
#[path = "common/split_retirement_concurrency.rs"]
mod concurrency;
#[path = "common/split_retirement_flow.rs"]
mod flow;
#[path = "common/split_retirement_ownership.rs"]
mod ownership;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_retirement_reclaim.rs"]
mod reclaim;
#[path = "common/split_retirement_fixture.rs"]
mod retirement_fixture;
#[path = "common/split_retirement_store.rs"]
mod retirement_store;
#[path = "common/split_store.rs"]
mod split_store;

use crowdb_access_dataset::{split::SplitHeadUpdate, DatasetAuthority, ReadCancellation};
use retirement_store::TestRetirementStore;
use std::sync::Arc;

#[tokio::test]
async fn historical_revision_delete_marks_a_fixed_grace_without_reclaiming_members() {
    let (inner, authority, identity, snapshot) = split_store::fixture().await;
    let record = split_store::prepared(&authority, &identity, snapshot, None).await;
    let clock = Arc::new(std::sync::atomic::AtomicU64::new(1000));
    let authority = DatasetAuthority::new(inner).with_reclaim_clock_for_tests(clock.clone());
    let deleted = authority
        .delete_split_revision(&identity, snapshot, record.revision, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(deleted.reclaim_after, Some(2200));
    assert!(!deleted.complete);
    clock.store(1100, std::sync::atomic::Ordering::Release);
    assert_eq!(
        authority
            .delete_split_revision(&identity, snapshot, record.revision, &ReadCancellation::new())
            .await
            .unwrap(),
        deleted
    );
    assert!(authority
        .get_split_revision(
            &identity,
            snapshot,
            record.revision,
            budget(),
            &ReadCancellation::new()
        )
        .await
        .is_err());
    assert!(authority.get_snapshot(&identity, snapshot).await.is_ok());
}

fn budget() -> crowdb_access_dataset::MetadataScanBudget {
    crowdb_access_dataset::MetadataScanBudget {
        items: 20,
        bytes: 1024 * 1024,
        timeout: std::time::Duration::from_secs(2),
    }
}

#[tokio::test]
async fn publisher_observes_the_head_generation_before_admitting_the_revision() {
    let (inner, authority, identity, snapshot) = split_store::fixture().await;
    let record = split_store::prepared(&authority, &identity, snapshot, None).await;
    let base = authority.inspect_split_head(&identity, snapshot).await.unwrap();
    let store = Arc::new(TestRetirementStore::new(inner));
    let authority = DatasetAuthority::new(store.clone());
    authority
        .update_split_head(
            &identity,
            snapshot,
            &SplitHeadUpdate {
                revision: record.revision,
                expected_revision: base.revision,
                expected_head: base.generation,
            },
        )
        .await
        .unwrap();
    assert_eq!(store.split_read_order(), vec!["SH", "SR"]);
}
