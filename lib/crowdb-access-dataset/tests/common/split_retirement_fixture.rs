// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{publication, retirement_store::TestRetirementStore, split_store};
use crowdb_access_dataset::{
    split::{key, SplitRevisionReclaimCursor, SplitRevisionRecord},
    DatasetAuthority, DatasetIdentity, OperationId, ReadCancellation, SnapshotId,
};
use std::sync::{atomic::AtomicU64, Arc};

pub struct TestFixture {
    pub store: Arc<TestRetirementStore>,
    pub authority: Arc<DatasetAuthority>,
    pub identity: DatasetIdentity,
    pub snapshot: SnapshotId,
    pub record: SplitRevisionRecord,
    pub clock: Arc<AtomicU64>,
}
impl TestFixture {
    pub async fn new() -> Self {
        let (inner, authority, identity, _) = split_store::fixture().await;
        let snapshot = authority
            .prepare_snapshot_candidate(
                &identity,
                None,
                publication::publication_manifest(2),
                OperationId::random(),
            )
            .await
            .unwrap();
        let record = split_store::prepared(&authority, &identity, snapshot, None).await;
        let store = Arc::new(TestRetirementStore::new(inner));
        let clock = Arc::new(AtomicU64::new(1000));
        let authority =
            Arc::new(DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock.clone()));
        Self {
            store,
            authority,
            identity,
            snapshot,
            record,
            clock,
        }
    }
    pub fn cursor(&self) -> SplitRevisionReclaimCursor {
        SplitRevisionReclaimCursor::new(self.identity.clone(), self.snapshot, self.record.revision)
    }
    pub fn root(&self) -> Vec<u8> {
        key::revision(&self.identity, self.snapshot, self.record.revision).unwrap()
    }
    pub fn pages(&self) -> Vec<Vec<u8>> {
        self.record
            .splits
            .iter()
            .flat_map(|split| {
                split.pages.iter().map(|page| {
                    key::membership_page(
                        &self.identity,
                        self.snapshot,
                        self.record.revision,
                        &split.name,
                        page.page,
                    )
                    .unwrap()
                })
            })
            .collect()
    }
    pub async fn finish(&self) {
        let mut cursor = self.cursor();
        for _ in 0..100 {
            if self
                .authority
                .advance_split_revision_reclaim(&mut cursor, &ReadCancellation::new())
                .await
                .unwrap()
                .complete
            {
                return;
            }
        }
        panic!("Split metadata reclamation did not converge");
    }
    pub async fn assert_terminal(&self) {
        for key in self.pages().into_iter().chain([self.root()]) {
            assert_eq!(
                self.store.inner.get(&key).await.unwrap().unwrap().bytes,
                b"split-retired-v1"
            );
        }
    }
}
