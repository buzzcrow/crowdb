// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/access_store.rs"]
#[allow(dead_code)]
mod access_store;
#[path = "common/deletion.rs"]
#[allow(dead_code)]
mod deletion;
#[path = "common/publication.rs"]
mod publication;

use crowdb_access_dataset::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;

#[derive(Deserialize, Serialize)]
struct TestStoredDeletion {
    version: u16,
    reclaiming: bool,
    owner: Option<OperationId>,
    retiring: bool,
    reclaim_after: u64,
    target: Option<(DatasetIdentity, SnapshotId)>,
    complete: bool,
}

#[tokio::test]
async fn an_existing_absolute_deadline_is_not_renewed_by_the_new_default() {
    let test = deletion::TestDeletionFixture::new().await;
    test.authority
        .begin_reclaim(&test.identity, test.snapshot)
        .await
        .unwrap();
    let mut key = b"DS\0L\0".to_vec();
    key.extend(test.identity.key().unwrap());
    key.extend(test.snapshot.as_bytes());
    let old = test.store.get(&key).await.unwrap().unwrap();
    let mut mark: TestStoredDeletion = bincode::deserialize(&old.bytes).unwrap();
    assert_eq!(mark.version, 4);
    assert_eq!(mark.reclaim_after, 2200);
    mark.reclaim_after = 1300;
    assert!(matches!(
        test.store
            .compare_exchange(&key, Some(&old.bytes), &bincode::serialize(&mark).unwrap())
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    test.clock.store(1299, Ordering::Release);
    let restarted =
        DatasetAuthority::new(test.store.clone()).with_reclaim_clock_for_tests(test.clock.clone());
    restarted
        .begin_reclaim(&test.identity, test.snapshot)
        .await
        .unwrap();
    assert_eq!(
        restarted
            .snapshot_reclaim_after(&test.identity, test.snapshot)
            .await
            .unwrap(),
        1300
    );
    let before = test.store.writes.load(Ordering::Relaxed);
    let mut cursor = MetadataCleanupProgress::new(test.snapshot);
    assert!(matches!(
        restarted
            .reclaim_snapshot_metadata_step_for_tests(
                &test.identity,
                test.snapshot,
                &mut cursor,
                &ReadCancellation::new()
            )
            .await,
        Err(AuthorityError::ReclaimPending { reclaim_after: 1300 })
    ));
    assert_eq!(test.store.writes.load(Ordering::Relaxed), before);
}
