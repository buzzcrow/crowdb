// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use crowdb_access_dataset::{CasOutcome, DatasetStore, PayloadStorage, StoreError, StoredValue, UploadError};
use crowdb_chunk_client::SmallWriteIntent;
use crowdb_protocol::{chunkdb::rpc::Location, common::ChunkId};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

#[derive(Default)]
pub struct TestStore {
    values: ArcSwap<BTreeMap<Vec<u8>, StoredValue>>,
}
#[async_trait]
impl DatasetStore for TestStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        Ok(self.values.load().get(key).cloned())
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
        let values = self.values.load();
        let value = values.get(key);
        if value.is_some_and(|v| key.len().saturating_add(v.bytes.len()) > budget.bytes) {
            return Err(crowdb_access_dataset::StoreError::Limit);
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
        let values = self.values.load();
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
                    return Err(crowdb_access_dataset::StoreError::Limit);
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
    ) -> Result<CasOutcome, StoreError> {
        loop {
            let old = self.values.load_full();
            let current = old.get(key).cloned();
            if current.as_ref().map(|v| v.bytes.as_slice()) != expected {
                return Ok(CasOutcome::Conflict(current));
            }
            let revision = current.as_ref().map_or(1, |v| v.revision + 1);
            let mut next = (*old).clone();
            next.insert(
                key.to_vec(),
                StoredValue {
                    bytes: value.to_vec(),
                    revision,
                },
            );
            if Arc::ptr_eq(&self.values.compare_and_swap(&old, Arc::new(next)), &old) {
                return Ok(CasOutcome::Applied(revision));
            }
        }
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        loop {
            let old = self.values.load_full();
            let mut next = (*old).clone();
            next.remove(key);
            if Arc::ptr_eq(&self.values.compare_and_swap(&old, Arc::new(next)), &old) {
                return Ok(());
            }
        }
    }
}
#[derive(Default)]
pub struct TestPayload {
    data: ArcSwap<BTreeMap<u64, Bytes>>,
    pub writes: AtomicU64,
    pub lose_reply: AtomicBool,
    pub defer_reclaim: AtomicBool,
    pub unconfirmed: AtomicBool,
    pub abandoned: AtomicBool,
}
#[async_trait]
impl PayloadStorage for TestPayload {
    async fn write(
        &self,
        _key: &[u8],
        data: Bytes,
        intent: Arc<dyn SmallWriteIntent>,
    ) -> Result<Vec<Location>, UploadError> {
        let index = self.writes.fetch_add(1, Ordering::AcqRel) + 1;
        let location = Location {
            chunk_id: Some(ChunkId { high: 1, low: index }),
            offset: 0,
            length: data.len() as u64,
            logical_offset: 0,
            logical_length: data.len() as u64,
        };
        intent
            .before_write(&location)
            .await
            .map_err(|e| UploadError::Storage(e.to_string()))?;
        loop {
            let old = self.data.load_full();
            let mut next = (*old).clone();
            next.insert(index, data.clone());
            if Arc::ptr_eq(&self.data.compare_and_swap(&old, Arc::new(next)), &old) {
                break;
            }
        }
        if self.lose_reply.swap(false, Ordering::AcqRel) {
            return Err(UploadError::Storage("lost reply".into()));
        }
        Ok(vec![location])
    }
    async fn read(&self, locations: &[Location]) -> Result<Bytes, UploadError> {
        let index = locations
            .first()
            .and_then(|l| l.chunk_id)
            .ok_or(UploadError::Integrity)?
            .low;
        self.data
            .load()
            .get(&index)
            .cloned()
            .ok_or(UploadError::Storage("missing bytes".into()))
    }
    async fn durability(
        &self,
        _locations: &[Location],
    ) -> Result<crowdb_access_dataset::PayloadDurability, UploadError> {
        Ok(if self.abandoned.load(Ordering::Acquire) {
            crowdb_access_dataset::PayloadDurability::Abandoned
        } else if self.unconfirmed.load(Ordering::Acquire) {
            crowdb_access_dataset::PayloadDurability::Pending
        } else {
            crowdb_access_dataset::PayloadDurability::Durable
        })
    }
    async fn discard(&self, locations: &[Location]) -> Result<bool, UploadError> {
        if self.defer_reclaim.load(Ordering::Acquire) {
            return Ok(false);
        }
        loop {
            let old = self.data.load_full();
            let mut next = (*old).clone();
            for location in locations {
                next.remove(&location.chunk_id.ok_or(UploadError::Integrity)?.low);
            }
            if Arc::ptr_eq(&self.data.compare_and_swap(&old, Arc::new(next)), &old) {
                return Ok(true);
            }
        }
    }
}
