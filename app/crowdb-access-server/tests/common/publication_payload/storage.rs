// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Default)]
pub struct TestPublicationStore {
    inner: super::upload::TestStore,
    pub upload_reads: AtomicU64,
    pub claim_attempts: AtomicU64,
    pub complete_attempts: AtomicU64,
    pub fail_metadata: AtomicBool,
    pub lose_claim_reply: AtomicBool,
    pub abort_at_claim: AtomicBool,
    pub unknown: AtomicBool,
    pub operations_after_unknown: AtomicU64,
}
impl TestPublicationStore {
    pub fn mutations(&self) -> u64 {
        self.inner.mutations.load(Ordering::Relaxed)
    }

    fn observe(&self) {
        if self.unknown.load(Ordering::Relaxed) {
            self.operations_after_unknown.fetch_add(1, Ordering::Relaxed);
        }
    }
}
#[async_trait::async_trait]
impl DatasetStore for TestPublicationStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.observe();
        if key.starts_with(b"DS\0U\0") {
            self.upload_reads.fetch_add(1, Ordering::Relaxed);
            if self.fail_metadata.swap(false, Ordering::Relaxed) {
                self.unknown.store(true, Ordering::Relaxed);
                return Err(StoreError::Rejected);
            }
        }
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.observe();
        self.inner.get_bounded(key, budget, cancel).await
    }
    async fn scan_page(
        &self,
        request: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        self.observe();
        self.inner.scan_page(request, cancel).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        self.observe();
        let json: Option<serde_json::Value> = serde_json::from_slice(value).ok();
        let claiming =
            key.starts_with(b"DS\0U\0") && json.as_ref().is_some_and(|value| value["state"] == "claimed");
        if claiming {
            self.claim_attempts.fetch_add(1, Ordering::Relaxed);
            if self.abort_at_claim.swap(false, Ordering::Relaxed) {
                let stored = self.inner.get(key).await?.unwrap();
                let mut aborted: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
                aborted["state"] = "aborted".into();
                assert!(matches!(
                    self.inner
                        .compare_exchange(key, Some(&stored.bytes), &serde_json::to_vec(&aborted).unwrap())
                        .await?,
                    CasOutcome::Applied(_)
                ));
            }
        }
        if key.starts_with(b"DS\0CB\0") && json.as_ref().is_some_and(|value| value["version"] == 2) {
            self.complete_attempts.fetch_add(1, Ordering::Relaxed);
        }
        let outcome = self.inner.compare_exchange(key, expected, value).await?;
        if claiming
            && matches!(outcome, CasOutcome::Applied(_))
            && self.lose_claim_reply.swap(false, Ordering::Relaxed)
        {
            self.unknown.store(true, Ordering::Relaxed);
            return Err(StoreError::Rejected);
        }
        Ok(outcome)
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.observe();
        self.inner.delete(key).await
    }
}
