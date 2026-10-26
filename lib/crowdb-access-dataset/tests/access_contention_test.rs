// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/access_store.rs"]
mod access_store;
#[path = "common/publication.rs"]
mod publication;
use crowdb_access_dataset::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn concurrent_readers_admit_without_any_persistent_mutation() {
    let store = access_store::TestAccessStore::shared();
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "concurrent-leases").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, publication::publication_manifest(1))
        .await
        .unwrap();
    let writes = store.writes.load(Ordering::Relaxed);
    let results = futures::future::join_all((0..96).map(|_| async {
        for _ in 0..64 {
            authority.check_snapshot_readable(&identity, snapshot).await?;
        }
        Ok::<_, AuthorityError>(())
    }))
    .await;
    for result in results {
        result.unwrap();
    }
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}
