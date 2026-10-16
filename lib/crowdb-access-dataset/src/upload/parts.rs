// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    decode_part, decode_session, digest, encode, part_count, part_key, DatasetUploads, PartEntry, PartState,
    UploadError, UploadPart, UploadRecord, UploadState, WRITE_LEASE_SECONDS,
};
use crate::{CasOutcome, OperationId};
use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{IoError, SmallWriteIntent};
use crowdb_protocol::chunkdb::rpc::Location;
use md5::{Digest, Md5};
use std::sync::Arc;

impl DatasetUploads {
    /// # Errors
    /// Rejects divergent bytes, expiry, invalid sizes and storage failures.
    pub async fn put_part(
        &self,
        location: &[u8],
        index: u32,
        data: Bytes,
        now: u64,
    ) -> Result<UploadPart, UploadError> {
        let session = self.session(location).await?;
        validate(&session, index, &data, now)?;
        if session.state == UploadState::Open {
            self.register(location).await?;
        }
        let key = part_key(location, index);
        let pending = PartEntry {
            version: 1,
            owner: OperationId::random(),
            deadline: now.saturating_add(WRITE_LEASE_SECONDS),
            state: PartState::Reserved,
            part: UploadPart {
                index,
                length: data.len() as u64,
                md5: digest(&data),
                locations: Vec::new(),
            },
        };
        let expected = match self.store.get(&key).await? {
            None => None,
            Some(value) => {
                let existing = decode_part(&value.bytes)?;
                if existing.part.md5 != pending.part.md5 || existing.part.length != pending.part.length {
                    return Err(UploadError::Conflict);
                }
                match existing.state {
                    PartState::Accepted => {
                        if session.state == UploadState::Open {
                            self.index_part(location, &existing.part).await?;
                        }
                        return Ok(existing.part);
                    }
                    PartState::Intent => match self.payloads.durability(&existing.part.locations).await? {
                        crate::PayloadDurability::Durable => {
                            return self.accept_existing(location, &key, &existing).await
                        }
                        crate::PayloadDurability::Pending => return Err(UploadError::Pending),
                        crate::PayloadDurability::Abandoned => {
                            if !self.payloads.discard(&existing.part.locations).await? {
                                return Err(UploadError::Pending);
                            }
                            Some(value.bytes)
                        }
                    },
                    PartState::Reserved if now >= existing.deadline => Some(value.bytes),
                    PartState::Reserved => return Err(UploadError::Pending),
                    PartState::Reclaimed => return Err(UploadError::Conflict),
                }
            }
        };
        if session.state != UploadState::Open {
            return Err(UploadError::Conflict);
        }
        if !matches!(
            self.store
                .compare_exchange(&key, expected.as_deref(), &encode(&pending)?)
                .await?,
            CasOutcome::Applied(_)
        ) {
            return Err(UploadError::Pending);
        }
        let intent = Arc::new(PartIntent {
            uploads: self.clone(),
            session: location.to_vec(),
            key: key.clone(),
            pending: pending.clone(),
        });
        let locations = self.payloads.write(&key, data, intent).await?;
        let current = self.store.get(&key).await?.ok_or(UploadError::Integrity)?;
        let entry = decode_part(&current.bytes)?;
        if entry.owner != pending.owner
            || entry.state != PartState::Intent
            || entry.part.locations != locations
        {
            return Err(UploadError::Conflict);
        }
        self.accept_part(location, &key, &entry).await
    }

    async fn accept_existing(
        &self,
        location: &[u8],
        key: &[u8],
        entry: &PartEntry,
    ) -> Result<UploadPart, UploadError> {
        if self.session(location).await?.state != UploadState::Open {
            return Err(UploadError::Conflict);
        }
        self.verified_part(&entry.part)
            .await
            .map_err(|error| match error {
                UploadError::Storage(_) => UploadError::Pending,
                other => other,
            })?;
        self.accept_part(location, key, entry).await
    }

    async fn accept_part(
        &self,
        location: &[u8],
        key: &[u8],
        entry: &PartEntry,
    ) -> Result<UploadPart, UploadError> {
        if self.session(location).await?.state != UploadState::Open {
            return Err(UploadError::Conflict);
        }
        let mut accepted = entry.clone();
        accepted.state = PartState::Accepted;
        match self
            .store
            .compare_exchange(key, Some(&encode(entry)?), &encode(&accepted)?)
            .await?
        {
            CasOutcome::Applied(_) => {}
            CasOutcome::Conflict(Some(value)) if decode_part(&value.bytes)? == accepted => {}
            CasOutcome::Conflict(_) => return Err(UploadError::Conflict),
        }
        self.index_part(location, &entry.part).await?;
        Ok(entry.part.clone())
    }

    /// # Errors
    /// Rejects missing, expired or corrupt parts; repeats are idempotent.
    pub async fn complete(&self, location: &[u8], now: u64) -> Result<UploadRecord, UploadError> {
        let stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
        let mut session = decode_session(&stored.bytes)?;
        if matches!(session.state, UploadState::Complete | UploadState::Claimed) {
            return self.get(location).await;
        }
        if session.state != UploadState::Open || now >= session.expires_at {
            return Err(UploadError::Conflict);
        }
        let parts = self.accepted_parts(location, &session).await?;
        if parts.len() != part_count(&session) as usize {
            return Err(UploadError::Conflict);
        }
        let mut hash = Md5::new();
        let mut length = 0;
        for part in &parts {
            let bytes = self.verified_part(part).await?;
            hash.update(&bytes);
            length += part.length;
        }
        if length != session.length || <[u8; 16]>::from(hash.finalize()) != session.md5 {
            return Err(UploadError::Integrity);
        }
        session.state = UploadState::Complete;
        match self
            .store
            .compare_exchange(location, Some(&stored.bytes), &encode(&session)?)
            .await?
        {
            CasOutcome::Applied(_) => self.get(location).await,
            CasOutcome::Conflict(Some(value))
                if matches!(
                    decode_session(&value.bytes)?.state,
                    UploadState::Complete | UploadState::Claimed
                ) =>
            {
                self.get(location).await
            }
            CasOutcome::Conflict(_) => Err(UploadError::Conflict),
        }
    }
}

struct PartIntent {
    uploads: DatasetUploads,
    session: Vec<u8>,
    key: Vec<u8>,
    pending: PartEntry,
}
#[async_trait]
impl SmallWriteIntent for PartIntent {
    async fn before_write(&self, location: &Location) -> crowdb_chunk_client::Result<()> {
        self.persist(location)
            .await
            .map_err(|_| IoError::MetadataConflict("Dataset part intent rejected".into()))
    }
}
impl PartIntent {
    async fn persist(&self, location: &Location) -> Result<(), UploadError> {
        if location.logical_offset != 0
            || location.logical_length != self.pending.part.length
            || self.uploads.session(&self.session).await?.state != UploadState::Open
        {
            return Err(UploadError::Conflict);
        }
        let mut intent = self.pending.clone();
        intent.state = PartState::Intent;
        intent.part.locations = vec![location.clone()];
        match self
            .uploads
            .store
            .compare_exchange(&self.key, Some(&encode(&self.pending)?), &encode(&intent)?)
            .await?
        {
            CasOutcome::Applied(_) => {}
            CasOutcome::Conflict(Some(value)) if decode_part(&value.bytes)? == intent => {}
            CasOutcome::Conflict(_) => return Err(UploadError::Conflict),
        }
        if self.uploads.session(&self.session).await?.state != UploadState::Open {
            return Err(UploadError::Conflict);
        }
        Ok(())
    }
}
fn validate(session: &UploadRecord, index: u32, data: &[u8], now: u64) -> Result<(), UploadError> {
    if session.state == UploadState::Aborted
        || (session.state != UploadState::Claimed && now >= session.expires_at)
    {
        return Err(UploadError::Conflict);
    }
    let offset = u64::from(index) * u64::from(session.part_size);
    if offset >= session.length
        || data.len() as u64 != (session.length - offset).min(u64::from(session.part_size))
    {
        return Err(UploadError::Invalid);
    }
    Ok(())
}
