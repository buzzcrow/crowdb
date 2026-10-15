// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{split::*, *};
use split_store::{attempt, fixture, prepared};
use std::{sync::atomic::Ordering, time::Duration};
fn budget(items: usize) -> MetadataScanBudget {
    MetadataScanBudget {
        items,
        bytes: 1024 * 1024,
        timeout: Duration::from_secs(2),
    }
}

#[tokio::test]
async fn members_are_scoped_ordered_and_bounded() {
    let (store, authority, identity, parent) = fixture().await;
    let snapshot = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let first = prepared(&authority, &identity, snapshot, None).await;
    let cancel = ReadCancellation::new();
    authority
        .publish_split_draft(
            &identity,
            first.operation,
            &attempt(&authority, &identity, &first).await,
        )
        .await
        .unwrap();
    let second = prepared(&authority, &identity, snapshot, Some(first.revision)).await;
    authority
        .publish_split_draft(
            &identity,
            second.operation,
            &attempt(&authority, &identity, &second).await,
        )
        .await
        .unwrap();
    store.inspect_only.store(true, Ordering::Relaxed);
    let name = SplitName::new("train").unwrap();
    let initial = authority
        .split_members_page(
            &identity,
            snapshot,
            first.revision,
            &name,
            None,
            budget(3),
            &cancel,
        )
        .await
        .unwrap();
    assert_eq!(initial.entries.len(), 3);
    assert!(authority
        .split_members_page(
            &identity,
            snapshot,
            second.revision,
            &name,
            initial.after.clone(),
            budget(3),
            &cancel
        )
        .await
        .is_err());
    assert!(authority
        .split_members_page(
            &identity,
            snapshot,
            first.revision,
            &SplitName::new("test").unwrap(),
            initial.after.clone(),
            budget(3),
            &cancel
        )
        .await
        .is_err());
    let mut ids = initial.entries;
    let mut after = initial.after;
    while after.is_some() {
        let page = authority
            .split_members_page(
                &identity,
                snapshot,
                first.revision,
                &name,
                after,
                budget(3),
                &cancel,
            )
            .await
            .unwrap();
        ids.extend(page.entries);
        after = page.after;
    }
    assert_eq!(
        ids.len() as u64,
        first.splits.iter().find(|s| s.name == name).unwrap().count
    );
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    let partition = SamplePartition::new(
        42,
        ["train", "test"]
            .into_iter()
            .map(|name| PartitionWeight {
                name: SplitName::new(name).unwrap(),
                weight: 1,
            })
            .collect(),
    )
    .unwrap();
    assert!(ids.iter().all(|id| partition.assign(id).unwrap() == &name));
}

#[tokio::test]
async fn history_excludes_prepared_revisions_and_retains_committed_revisions() {
    let (store, authority, identity, snapshot) = fixture().await;
    let first = prepared(&authority, &identity, snapshot, None).await;
    let cancel = ReadCancellation::new();
    assert!(authority
        .split_revision_history(&identity, snapshot, None, budget(20), &cancel)
        .await
        .unwrap()
        .entries
        .is_empty());
    assert!(matches!(
        authority
            .get_split_revision(&identity, snapshot, first.revision, budget(20), &cancel)
            .await,
        Err(SplitLifecycleError::NotFound)
    ));
    authority
        .publish_split_draft(
            &identity,
            first.operation,
            &attempt(&authority, &identity, &first).await,
        )
        .await
        .unwrap();
    let second = prepared(&authority, &identity, snapshot, Some(first.revision)).await;
    authority
        .publish_split_draft(
            &identity,
            second.operation,
            &attempt(&authority, &identity, &second).await,
        )
        .await
        .unwrap();
    let _uncommitted = prepared(&authority, &identity, snapshot, Some(second.revision)).await;
    store.inspect_only.store(true, Ordering::Relaxed);
    let history = authority
        .split_revision_history(&identity, snapshot, None, budget(20), &cancel)
        .await
        .unwrap();
    assert_eq!(history.entries.len(), 2);
    assert_eq!(
        authority
            .get_split_revision(&identity, snapshot, first.revision, budget(20), &cancel)
            .await
            .unwrap(),
        first
    );
}
