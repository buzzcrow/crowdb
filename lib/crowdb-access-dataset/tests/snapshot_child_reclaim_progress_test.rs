// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/snapshot_expiry.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
use common::TestExpiryFixture;
use crowdb_access_dataset::*;

fn child_key(identity: &DatasetIdentity, parent: SnapshotId, child: SnapshotId) -> Vec<u8> {
    [
        b"DS\0IC\0".as_slice(),
        &identity.key().unwrap(),
        &[1],
        parent.as_bytes(),
        child.as_bytes(),
    ]
    .concat()
}

fn retired_key(identity: &DatasetIdentity, child: SnapshotId) -> Vec<u8> {
    [b"DS\0RT\0".as_slice(), &identity.key().unwrap(), child.as_bytes()].concat()
}

#[tokio::test]
async fn retired_siblings_larger_than_one_window_do_not_permanently_pin_the_parent() {
    let f = TestExpiryFixture::new().await;
    let parent = f.candidate(None, 2).await;
    let mut children = Vec::new();
    let mut qualifying_bytes = 0;
    while qualifying_bytes <= 1024 * 1024 {
        let mut operation = [7; 16];
        operation[8..].copy_from_slice(&(children.len() as u64).to_be_bytes());
        let child = f
            .authority
            .prepare_snapshot_candidate(
                &f.identity,
                Some(parent),
                publication::publication_manifest(8),
                OperationId::from_bytes(&operation).unwrap(),
            )
            .await
            .unwrap();
        f.authority.begin_reclaim(&f.identity, child).await.unwrap();
        let index_key = child_key(&f.identity, parent, child);
        let index = f.store.get(&index_key).await.unwrap().unwrap();
        let summary: inspection::SnapshotSummary = serde_json::from_slice(&index.bytes).unwrap();
        assert_eq!(summary.parent, Some(parent));
        assert_eq!(summary.snapshot, child);
        let retirement_key = retired_key(&f.identity, child);
        let retired = f.store.get(&retirement_key).await.unwrap().unwrap();
        assert_eq!(retired.bytes, [1]);
        qualifying_bytes += index_key.len() + index.bytes.len() + retirement_key.len() + retired.bytes.len();
        children.push(child);
    }
    eprintln!(
        "legitimate retired siblings: {}, qualification bytes: {qualifying_bytes}",
        children.len()
    );
    let mut results = Vec::new();
    let mut marked = false;
    for _ in 0..4 {
        let result = f.authority.begin_reclaim(&f.identity, parent).await;
        results.push(format!("{result:?}"));
        if result.is_ok() {
            marked = true;
            break;
        }
        assert!(matches!(
            result,
            Err(AuthorityError::Invalid(DatasetError::ReadLimitExceeded)
                | AuthorityError::Store(StoreError::Limit))
        ));
    }
    assert!(
        marked,
        "bounded retries never advanced past retired children: {results:?}"
    );
    assert_eq!(
        f.authority
            .snapshot_reclaim_after(&f.identity, parent)
            .await
            .unwrap(),
        2200
    );
    for child in children {
        assert!(f
            .store
            .get(&child_key(&f.identity, parent, child))
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            f.store
                .get(&retired_key(&f.identity, child))
                .await
                .unwrap()
                .unwrap()
                .bytes,
            [1]
        );
        assert!(f
            .authority
            .require_committed_snapshot(&f.identity, child)
            .await
            .is_err());
    }
    assert!(f.authority.get_snapshot(&f.identity, f.root).await.is_ok());
}
