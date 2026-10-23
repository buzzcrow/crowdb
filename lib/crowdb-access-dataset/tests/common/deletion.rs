// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{access_store, publication};
use crowdb_access_dataset::*;
use std::sync::{atomic::AtomicU64, Arc};

pub struct TestDeletionFixture {
    pub store: Arc<access_store::TestAccessStore>,
    pub clock: Arc<AtomicU64>,
    pub authority: DatasetAuthority,
    pub identity: DatasetIdentity,
    pub snapshot: SnapshotId,
    pub next: SnapshotId,
    pub held: ManifestRecord,
}
impl TestDeletionFixture {
    pub async fn new() -> Self {
        let store = access_store::TestAccessStore::shared();
        let clock = Arc::new(AtomicU64::new(1000));
        let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock.clone());
        let identity = DatasetIdentity::new(NamespacePath::root(), "grace").unwrap();
        authority.create_dataset(identity.clone()).await.unwrap();
        let snapshot = authority
            .publish_snapshot(&identity, None, publication::publication_manifest(1))
            .await
            .unwrap();
        // A persisted old reader, even with a distant expiry, cannot hold deletion.
        let mut old_key = b"DS\0L\0".to_vec();
        old_key.extend(identity.key().unwrap());
        old_key.extend(snapshot.as_bytes());
        let old_access = bincode::serialize(&(
            3u16,
            false,
            0u64,
            0u64,
            vec![(OperationId::random(), u64::MAX)],
            None::<OperationId>,
            false,
        ))
        .unwrap();
        store.compare_exchange(&old_key, None, &old_access).await.unwrap();
        authority
            .check_snapshot_readable(&identity, snapshot)
            .await
            .unwrap();
        let held = authority.get_manifest(&identity, snapshot).await.unwrap();
        let next = authority
            .prepare_snapshot_candidate(
                &identity,
                None,
                publication::publication_manifest(2),
                OperationId::random(),
            )
            .await
            .unwrap();
        authority
            .publish_snapshot_candidate(
                &identity,
                &CommitAttempt {
                    version: 1,
                    id: OperationId::random(),
                    target: CommitTarget::Snapshot { snapshot: next },
                    expected_head: authority.inspect_head(&identity).await.unwrap().generation,
                },
            )
            .await
            .unwrap();
        Self {
            store,
            clock,
            authority,
            identity,
            snapshot,
            next,
            held,
        }
    }
}
