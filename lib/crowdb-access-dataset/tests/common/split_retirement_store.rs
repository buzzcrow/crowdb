// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{
    CasOutcome, DatasetStore, MetadataPage, MetadataScan, MetadataScanBudget, ReadCancellation, StoreError,
    StoredValue,
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

pub struct TestPause {
    prefix: Vec<u8>,
    value_prefix: Option<Vec<u8>>,
    after_apply: bool,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

impl TestPause {
    pub async fn entered(&self) {
        self.entered.acquire().await.unwrap().forget();
    }
    pub fn release(&self) {
        self.release.add_permits(1);
    }
    async fn wait(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }
}

pub struct TestRetirementStore {
    pub inner: Arc<dyn DatasetStore>,
    reads: Mutex<Vec<Vec<u8>>>,
    writes: AtomicUsize,
    fail_after: AtomicUsize,
    apply_unknown: AtomicBool,
    stopped: AtomicBool,
    gates: Mutex<Vec<Arc<TestPause>>>,
}

impl TestRetirementStore {
    pub fn new(inner: Arc<dyn DatasetStore>) -> Self {
        Self {
            inner,
            reads: Mutex::new(Vec::new()),
            writes: AtomicUsize::new(0),
            fail_after: AtomicUsize::new(usize::MAX),
            apply_unknown: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            gates: Mutex::new(Vec::new()),
        }
    }

    pub fn counts(&self) -> (usize, usize) {
        (
            self.reads.lock().unwrap().len(),
            self.writes.load(Ordering::Relaxed),
        )
    }
    pub fn unknown_write(&self, offset: usize, applied: bool) {
        self.fail_after.store(offset, Ordering::Relaxed);
        self.apply_unknown.store(applied, Ordering::Relaxed);
    }
    pub fn resume(&self) {
        self.stopped.store(false, Ordering::Relaxed);
    }
    pub fn pause(&self, prefix: &[u8], value_prefix: Option<&[u8]>, after_apply: bool) -> Arc<TestPause> {
        let gate = Arc::new(TestPause {
            prefix: prefix.to_vec(),
            value_prefix: value_prefix.map(<[u8]>::to_vec),
            after_apply,
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        self.gates.lock().unwrap().push(gate.clone());
        gate
    }
    fn check(&self) {
        assert!(
            !self.stopped.load(Ordering::Relaxed),
            "RPC after unknown retirement mutation"
        );
    }

    pub fn split_read_order(&self) -> Vec<&'static str> {
        self.reads
            .lock()
            .unwrap()
            .iter()
            .filter_map(|key| {
                if key.starts_with(b"DS\0SH\0") {
                    Some("SH")
                } else if key.starts_with(b"DS\0SR\0") {
                    Some("SR")
                } else {
                    None
                }
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl DatasetStore for TestRetirementStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.check();
        self.reads.lock().unwrap().push(key.to_vec());
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.check();
        self.reads.lock().unwrap().push(key.to_vec());
        self.inner.get_bounded(key, budget, cancel).await
    }
    async fn scan_page(
        &self,
        request: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        self.check();
        self.inner.scan_page(request, cancel).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        self.check();
        self.writes.fetch_add(1, Ordering::Relaxed);
        let gate = {
            let mut gates = self.gates.lock().unwrap();
            let position = gates.iter().position(|gate| {
                key.starts_with(&gate.prefix)
                    && gate
                        .value_prefix
                        .as_ref()
                        .map_or(true, |prefix| value.starts_with(prefix))
            });
            position.map(|index| gates.remove(index))
        };
        if let Some(gate) = &gate {
            if !gate.after_apply {
                gate.wait().await;
            }
        }
        let fault = self
            .fail_after
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                if remaining == usize::MAX {
                    None
                } else if remaining == 0 {
                    Some(usize::MAX)
                } else {
                    Some(remaining - 1)
                }
            })
            .is_ok_and(|remaining| remaining == 0);
        if fault && !self.apply_unknown.load(Ordering::Relaxed) {
            self.stopped.store(true, Ordering::Relaxed);
            return Err(StoreError::Rejected);
        }
        let result = self.inner.compare_exchange(key, expected, value).await?;
        if let Some(gate) = &gate {
            if gate.after_apply {
                gate.wait().await;
            }
        }
        if fault {
            self.stopped.store(true, Ordering::Relaxed);
            return Err(StoreError::Rejected);
        }
        Ok(result)
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.check();
        self.inner.delete(key).await
    }
}
