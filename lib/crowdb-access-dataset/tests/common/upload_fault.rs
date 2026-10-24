// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_access_dataset::{CasOutcome, DatasetStore, StoreError, StoredValue};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use super::upload::TestStore;

pub enum TestMutation {
    Conflict(Vec<u8>),
    Apply,
    LoseReply,
}

pub struct TestUploadFaultStore {
    pub inner: Arc<TestStore>,
    key: Vec<u8>,
    mutations: Vec<TestMutation>,
    pub reads: AtomicUsize,
    pub writes: AtomicUsize,
}

impl TestUploadFaultStore {
    pub fn new(inner: Arc<TestStore>, key: Vec<u8>, mutations: Vec<TestMutation>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            key,
            mutations,
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl DatasetStore for TestUploadFaultStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        if key == self.key {
            self.reads.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        if key != self.key {
            return self.inner.compare_exchange(key, expected, value).await;
        }
        let index = self.writes.fetch_add(1, Ordering::Relaxed);
        let current = self.inner.get(key).await?;
        assert_eq!(
            current.as_ref().map(|stored| stored.bytes.as_slice()),
            expected,
            "retry must use the last returned observation"
        );
        match self.mutations.get(index) {
            Some(TestMutation::Conflict(observed)) => {
                assert!(matches!(
                    self.inner.compare_exchange(key, expected, observed).await?,
                    CasOutcome::Applied(_)
                ));
                Ok(CasOutcome::Conflict(self.inner.get(key).await?))
            }
            Some(TestMutation::LoseReply) => {
                self.inner.compare_exchange(key, expected, value).await?;
                Err(StoreError::Rejected)
            }
            Some(TestMutation::Apply) | None => self.inner.compare_exchange(key, expected, value).await,
        }
    }
}
