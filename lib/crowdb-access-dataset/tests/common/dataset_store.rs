// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_access_dataset::{CasOutcome, DatasetStore, StoredValue};
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Default)]
pub(super) struct TestDatasetStore {
    values: Mutex<BTreeMap<Vec<u8>, StoredValue>>,
    fail_snapshot_write: Mutex<bool>,
}

impl TestDatasetStore {
    pub(super) fn fail_next_snapshot_write(&self) {
        *self.fail_snapshot_write.lock().unwrap() = true;
    }
}

#[async_trait]
impl DatasetStore for TestDatasetStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, crowdb_access_dataset::StoreError> {
        Ok(self.values.lock().unwrap().get(key).cloned())
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, crowdb_access_dataset::StoreError> {
        if key.starts_with(b"DS\0S\0") && *self.fail_snapshot_write.lock().unwrap() {
            *self.fail_snapshot_write.lock().unwrap() = false;
            return Err(crowdb_access_dataset::StoreError::Rejected);
        }
        let mut values = self.values.lock().unwrap();
        let current = values.get(key).cloned();
        if current.as_ref().map(|entry| entry.bytes.as_slice()) != expected {
            return Ok(CasOutcome::Conflict(current));
        }
        let revision = current.as_ref().map_or(1, |entry| entry.revision + 1);
        values.insert(
            key.to_vec(),
            StoredValue {
                bytes: value.to_vec(),
                revision,
            },
        );
        Ok(CasOutcome::Applied(revision))
    }

    async fn delete(&self, key: &[u8]) -> Result<(), crowdb_access_dataset::StoreError> {
        self.values.lock().unwrap().remove(key);
        Ok(())
    }
}
