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

    async fn get_bounded(
        &self,
        key: &[u8],
        budget: crowdb_access_dataset::MetadataScanBudget,
        cancel: &crowdb_access_dataset::ReadCancellation,
    ) -> Result<Option<StoredValue>, crowdb_access_dataset::StoreError> {
        budget.validate()?;
        if key.is_empty() || key.len() >= 64 * 1024 {
            return Err(crowdb_access_dataset::StoreError::Rejected);
        }
        if cancel.is_cancelled() {
            return Err(crowdb_access_dataset::DatasetError::ReadCancelled.into());
        }
        let values = self.values.lock().unwrap();
        let value = values.get(key);
        if value.is_some_and(|v| key.len().saturating_add(v.bytes.len()) > budget.bytes) {
            return Err(crowdb_access_dataset::StoreError::Rejected);
        }
        Ok(value.cloned())
    }
    async fn scan_page(
        &self,
        request: &crowdb_access_dataset::MetadataScan,
        cancel: &crowdb_access_dataset::ReadCancellation,
    ) -> Result<crowdb_access_dataset::MetadataPage, crowdb_access_dataset::StoreError> {
        request.validate()?;
        if cancel.is_cancelled() {
            return Err(crowdb_access_dataset::DatasetError::ReadCancelled.into());
        }
        let started = std::time::Instant::now();
        let values = self.values.lock().unwrap();
        let mut entries = Vec::new();
        let mut bytes: usize = 0;
        let start = request.after.as_ref().unwrap_or(&request.prefix);
        for (key, value) in values.range(start.clone()..) {
            if cancel.is_cancelled() {
                return Err(crowdb_access_dataset::DatasetError::ReadCancelled.into());
            }
            if started.elapsed() >= request.budget.timeout {
                return Err(crowdb_access_dataset::StoreError::Rejected);
            }
            if !key.starts_with(&request.prefix) {
                break;
            }
            if request.after.as_ref().is_some_and(|after| key <= after) {
                continue;
            }
            let size = key.len().saturating_add(value.bytes.len());
            if entries.len() == request.budget.items || bytes.saturating_add(size) > request.budget.bytes {
                let after = entries
                    .last()
                    .map(|(key, _): &(Vec<u8>, StoredValue)| key.clone());
                if after.is_none() {
                    return Err(crowdb_access_dataset::StoreError::Rejected);
                }
                return Ok(crowdb_access_dataset::MetadataPage { entries, after });
            }
            bytes += size;
            entries.push((key.clone(), value.clone()));
        }
        Ok(crowdb_access_dataset::MetadataPage { entries, after: None })
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
