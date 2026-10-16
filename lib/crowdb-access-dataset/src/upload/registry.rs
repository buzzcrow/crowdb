// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Paged cleanup registry with registration generations and a CAS fence for stale entries.

use super::{decode, decode_session, encode, DatasetUploads, UploadError, UploadState};
use crate::{CasOutcome, OperationId};
use serde::{Deserialize, Serialize};
const TAIL: &[u8] = b"DS\0UTAIL\0";
const SCAN: &[u8] = b"DS\0USCAN\0";
const PAGE_SIZE: usize = 128;
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
struct Entry {
    location: Vec<u8>,
    generation: Option<OperationId>,
}
#[derive(Serialize, Deserialize)]
struct Page {
    version: u16,
    entries: Vec<Entry>,
}
fn page(bytes: &[u8]) -> Result<Page, UploadError> {
    if let Ok(record) = decode::<Page>(bytes) {
        if record.version != 2 {
            return Err(UploadError::Integrity);
        }
        return Ok(record);
    }
    let old: Vec<Vec<u8>> = decode(bytes)?;
    Ok(Page {
        version: 2,
        entries: old
            .into_iter()
            .map(|location| Entry {
                location,
                generation: None,
            })
            .collect(),
    })
}
fn page_key(index: u64) -> Vec<u8> {
    let mut key = b"DS\0UPAGE\0".to_vec();
    key.extend(index.to_be_bytes());
    key
}
impl DatasetUploads {
    pub(super) async fn register(&self, location: &[u8]) -> Result<(), UploadError> {
        for _ in 0..16 {
            let stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
            let mut record = decode_session(&stored.bytes)?;
            if record.cleanup_complete {
                return Ok(());
            }
            if let Some(index) = record.registry_page {
                return self
                    .add_entry(
                        index,
                        &Entry {
                            location: location.to_vec(),
                            generation: record.registry_generation,
                        },
                    )
                    .await;
            }
            let entry = Entry {
                location: location.to_vec(),
                generation: Some(OperationId::random()),
            };
            let index = self.append_registry(&entry).await?;
            record.registry_page = Some(index);
            record.registry_generation = entry.generation;
            if matches!(
                self.store
                    .compare_exchange(location, Some(&stored.bytes), &encode(&record)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return self.add_entry(index, &entry).await;
            }
            self.remove_stale(index, &entry).await?;
        }
        Err(UploadError::Pending)
    }
    async fn append_registry(&self, entry: &Entry) -> Result<u64, UploadError> {
        for _ in 0..32 {
            let tail = self.store.get(TAIL).await?;
            let index = tail
                .as_ref()
                .map(|v| decode::<u64>(&v.bytes))
                .transpose()?
                .unwrap_or(0);
            match self.add_entry(index, entry).await {
                Ok(()) => return Ok(index),
                Err(UploadError::Capacity) => {
                    let next = index.checked_add(1).ok_or(UploadError::Capacity)?;
                    self.store
                        .compare_exchange(TAIL, tail.as_ref().map(|v| v.bytes.as_slice()), &encode(&next)?)
                        .await?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(UploadError::Pending)
    }
    async fn add_entry(&self, index: u64, entry: &Entry) -> Result<(), UploadError> {
        let key = page_key(index);
        for _ in 0..32 {
            let stored = self.store.get(&key).await?;
            let mut record = stored
                .as_ref()
                .map(|v| page(&v.bytes))
                .transpose()?
                .unwrap_or(Page {
                    version: 2,
                    entries: Vec::new(),
                });
            if record.entries.contains(entry) {
                return Ok(());
            }
            if record.entries.len() >= PAGE_SIZE {
                return Err(UploadError::Capacity);
            }
            record.entries.push(entry.clone());
            if matches!(
                self.store
                    .compare_exchange(
                        &key,
                        stored.as_ref().map(|v| v.bytes.as_slice()),
                        &encode(&record)?
                    )
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(());
            }
        }
        Err(UploadError::Pending)
    }
    /// Removes tracking after durable binding or completed abort. A later
    /// registration has a distinct generation and cannot be removed here.
    /// # Errors
    /// Returns storage failures or a session that still needs expiry tracking.
    pub async fn untrack(&self, location: &[u8]) -> Result<(), UploadError> {
        for _ in 0..16 {
            let stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
            let mut record = decode_session(&stored.bytes)?;
            let Some(index) = record.registry_page else {
                return Ok(());
            };
            if record.state != UploadState::Claimed && !record.cleanup_complete {
                return Err(UploadError::Conflict);
            }
            let entry = Entry {
                location: location.to_vec(),
                generation: record.registry_generation,
            };
            record.registry_page = None;
            if matches!(
                self.store
                    .compare_exchange(location, Some(&stored.bytes), &encode(&record)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return self.remove_entry(index, &entry).await;
            }
        }
        Err(UploadError::Pending)
    }
    async fn remove_entry(&self, index: u64, entry: &Entry) -> Result<(), UploadError> {
        let key = page_key(index);
        for _ in 0..32 {
            let Some(stored) = self.store.get(&key).await? else {
                return Ok(());
            };
            let mut record = page(&stored.bytes)?;
            record.entries.retain(|v| v != entry);
            if matches!(
                self.store
                    .compare_exchange(&key, Some(&stored.bytes), &encode(&record)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(());
            }
        }
        Err(UploadError::Pending)
    }
    async fn remove_stale(&self, index: u64, entry: &Entry) -> Result<(), UploadError> {
        // Fence registrars using the observed session bytes before removing an
        // unbound entry. Otherwise a delayed registration could publish a link
        // to an entry the scanner has just removed.
        for _ in 0..32 {
            let stored = self
                .store
                .get(&entry.location)
                .await?
                .ok_or(UploadError::NotFound)?;
            let mut record = decode_session(&stored.bytes)?;
            if record.registry_page == Some(index) && record.registry_generation == entry.generation {
                return Ok(());
            }
            record.registry_fence = Some(OperationId::random());
            if matches!(
                self.store
                    .compare_exchange(&entry.location, Some(&stored.bytes), &encode(&record)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return self.remove_entry(index, entry).await;
            }
        }
        Err(UploadError::Pending)
    }
    /// Scans one bounded registry page; restart preserves the scan cursor.
    /// # Errors
    /// Returns storage/cleanup failures. Deferred reclamation remains tracked.
    pub async fn expire(&self, now: u64) -> Result<(), UploadError> {
        let tail = self
            .store
            .get(TAIL)
            .await?
            .map(|v| decode::<u64>(&v.bytes))
            .transpose()?
            .unwrap_or(0);
        let scan = self.store.get(SCAN).await?;
        let index = scan
            .as_ref()
            .map(|v| decode::<u64>(&v.bytes))
            .transpose()?
            .unwrap_or(0)
            .min(tail);
        if let Some(stored) = self.store.get(&page_key(index)).await? {
            for entry in page(&stored.bytes)?.entries {
                let session = self.session(&entry.location).await?;
                if session.registry_page != Some(index) || session.registry_generation != entry.generation {
                    self.remove_stale(index, &entry).await?;
                    continue;
                }
                if session.state == UploadState::Aborted
                    || (session.owners.is_empty() && now >= session.expires_at)
                {
                    match self.abort(&entry.location).await {
                        Ok(_) | Err(UploadError::Conflict) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        let next = if index >= tail { 0 } else { index + 1 };
        self.store
            .compare_exchange(SCAN, scan.as_ref().map(|v| v.bytes.as_slice()), &encode(&next)?)
            .await?;
        Ok(())
    }
}
