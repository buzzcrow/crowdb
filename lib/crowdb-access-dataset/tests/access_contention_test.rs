// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/access_store.rs"]
mod access_store;
#[path = "common/publication.rs"]
mod publication;
use crowdb_access_dataset::*;
use std::sync::{atomic::Ordering, Arc};

#[tokio::test]
async fn competing_renewal_does_not_starve_acquisition_or_release_another_reader() {
    let identity = DatasetIdentity::new(NamespacePath::root(), "lease-contention").unwrap();
    let inner = access_store::store();
    let setup = DatasetAuthority::new(inner.clone());
    setup.create_dataset(identity.clone()).await.unwrap();
    let snapshot = setup
        .publish_snapshot(&identity, None, publication::publication_manifest(1))
        .await
        .unwrap();
    let store = Arc::new(access_store::TestAccessStore {
        inner,
        race: true.into(),
        renewals: 0.into(),
        identity: identity.clone(),
        snapshot,
        other: OperationId::random(),
    });
    let authority = DatasetAuthority::new(store.clone());
    let reader = OperationId::random();
    authority
        .touch_read_handle(&identity, snapshot, reader, u64::MAX / 2, 60)
        .await
        .unwrap();
    authority
        .release_read_handle(&identity, snapshot, reader)
        .await
        .unwrap();
    assert_eq!(store.renewals.load(Ordering::Relaxed), 2);
    store.race.store(false, Ordering::Relaxed);
    // Remove head protection so only the competing reader prevents reclamation.
    let next = setup
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(2),
            OperationId::random(),
        )
        .await
        .unwrap();
    setup
        .publish_snapshot_candidate(
            &identity,
            &CommitAttempt {
                version: 1,
                id: OperationId::random(),
                target: CommitTarget::Snapshot { snapshot: next },
                expected_head: setup.inspect_head(&identity).await.unwrap().generation,
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        authority.begin_reclaim(&identity, snapshot).await,
        Err(AuthorityError::SnapshotProtected)
    ));
    authority
        .release_read_handle(&identity, snapshot, store.other)
        .await
        .unwrap();
    authority.begin_reclaim(&identity, snapshot).await.unwrap();
    assert!(matches!(
        authority
            .touch_read_handle(&identity, snapshot, reader, u64::MAX / 2, 60)
            .await,
        Err(AuthorityError::SnapshotProtected)
    ));
}
