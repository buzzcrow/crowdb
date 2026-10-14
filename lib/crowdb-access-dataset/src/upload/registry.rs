// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Paged cleanup registry. Only active/unbound uploads need expiry scanning.

use super::{decode, decode_session, encode, DatasetUploads, UploadError, UploadState};
use crate::CasOutcome;
const TAIL: &[u8] = b"DS\0UTAIL\0";
const SCAN: &[u8] = b"DS\0USCAN\0";
const PAGE_SIZE: usize = 128;

impl DatasetUploads {
    pub(super) async fn register(&self, location: &[u8]) -> Result<(), UploadError> {
        for _ in 0..16 {
            let stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
            let mut record = decode_session(&stored.bytes)?;
            if record.registry_page.is_some() || record.cleanup_complete {
                return Ok(());
            }
            let page = self.append_registry(location).await?;
            record.registry_page = Some(page);
            if matches!(
                self.store
                    .compare_exchange(location, Some(&stored.bytes), &encode(&record)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(());
            }
        }
        Err(UploadError::Pending)
    }
    async fn append_registry(&self, location: &[u8]) -> Result<u64, UploadError> {
        for _ in 0..32 {
            let tail = self.store.get(TAIL).await?;
            let page = tail
                .as_ref()
                .map(|v| decode::<u64>(&v.bytes))
                .transpose()?
                .unwrap_or(0);
            let key = page_key(page);
            let stored = self.store.get(&key).await?;
            let mut entries = stored
                .as_ref()
                .map(|v| decode::<Vec<Vec<u8>>>(&v.bytes))
                .transpose()?
                .unwrap_or_default();
            if entries.iter().any(|v| v == location) {
                return Ok(page);
            }
            if entries.len() >= PAGE_SIZE {
                let next = page.checked_add(1).ok_or(UploadError::Capacity)?;
                self.store
                    .compare_exchange(TAIL, tail.as_ref().map(|v| v.bytes.as_slice()), &encode(&next)?)
                    .await?;
                continue;
            }
            entries.push(location.to_vec());
            if matches!(
                self.store
                    .compare_exchange(
                        &key,
                        stored.as_ref().map(|v| v.bytes.as_slice()),
                        &encode(&entries)?
                    )
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(page);
            }
        }
        Err(UploadError::Pending)
    }

    /// Removes expiry tracking after durable publication binding or completed abort.
    /// # Errors
    /// Returns storage failure or conflicting registry updates.
    pub async fn untrack(&self, location: &[u8]) -> Result<(), UploadError> {
        for _ in 0..16 {
            let stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
            let mut record = decode_session(&stored.bytes)?;
            let Some(page) = record.registry_page else {
                return Ok(());
            };
            if record.state != UploadState::Claimed && !record.cleanup_complete {
                return Err(UploadError::Conflict);
            }
            record.registry_page = None;
            if matches!(
                self.store
                    .compare_exchange(location, Some(&stored.bytes), &encode(&record)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return self.remove_entry(page, location).await;
            }
        }
        Err(UploadError::Pending)
    }
    async fn remove_entry(&self, page: u64, location: &[u8]) -> Result<(), UploadError> {
        let key = page_key(page);
        for _ in 0..16 {
            let Some(stored) = self.store.get(&key).await? else {
                return Ok(());
            };
            let mut entries: Vec<Vec<u8>> = decode(&stored.bytes)?;
            entries.retain(|v| v != location);
            if matches!(
                self.store
                    .compare_exchange(&key, Some(&stored.bytes), &encode(&entries)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(());
            }
        }
        Err(UploadError::Pending)
    }

    /// Scans a bounded registry page and resumes scanning after service restart.
    /// # Errors
    /// Returns storage or cleanup failures; deferred bytes remain tracked.
    pub async fn expire(&self, now: u64) -> Result<(), UploadError> {
        let tail = self
            .store
            .get(TAIL)
            .await?
            .map(|v| decode::<u64>(&v.bytes))
            .transpose()?
            .unwrap_or(0);
        let scan = self.store.get(SCAN).await?;
        let page = scan
            .as_ref()
            .map(|v| decode::<u64>(&v.bytes))
            .transpose()?
            .unwrap_or(0)
            .min(tail);
        if let Some(stored) = self.store.get(&page_key(page)).await? {
            let entries: Vec<Vec<u8>> = decode(&stored.bytes)?;
            for location in entries {
                let session = self.session(&location).await?;
                if session.registry_page != Some(page) {
                    self.remove_entry(page, &location).await?;
                    continue;
                }
                if session.state == UploadState::Aborted
                    || (session.owners.is_empty() && now >= session.expires_at)
                {
                    match self.abort(&location).await {
                        Ok(_) | Err(UploadError::Conflict) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        let next = if page >= tail { 0 } else { page + 1 };
        self.store
            .compare_exchange(SCAN, scan.as_ref().map(|v| v.bytes.as_slice()), &encode(&next)?)
            .await?;
        Ok(())
    }
}
fn page_key(page: u64) -> Vec<u8> {
    let mut key = b"DS\0UPAGE\0".to_vec();
    key.extend_from_slice(&page.to_be_bytes());
    key
}
