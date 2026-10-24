// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Terminal multipart session transitions.

use super::{
    MetadataKey, MultipartExchange, MultipartPhase, MultipartRepository, MultipartRepositoryError,
    MultipartSessionRecord,
};
use crate::metadata::BucketId;
use crowdb_access_multipart::next_revision;

const MAX_EXPIRY_PAGE: usize = 1_000;
const MAX_EXPIRY_SCAN_BYTES: usize = 4 * 1024 * 1024;

mod sweep;
pub use sweep::{MultipartExpiryError, MultipartExpiryStep, MultipartExpirySweep};

#[derive(Debug)]
pub struct MultipartExpiryPage {
    pub next: Option<Vec<u8>>,
    pub expired: usize,
}

impl MultipartRepository {
    /// Marks expired open uploads terminal while retaining all part evidence.
    ///
    /// Returns a key cursor for the next bounded listing-index page. A worker
    /// can resume from that key without an additional per-upload cleanup queue.
    ///
    /// # Errors
    /// Rejects an invalid cursor, corrupt index data or unavailable metadata.
    pub async fn expire_page(
        &self,
        bucket: BucketId,
        after: Option<&[u8]>,
        now_ms: u64,
        max_items: usize,
    ) -> Result<MultipartExpiryPage, MultipartRepositoryError> {
        if max_items == 0 || max_items > MAX_EXPIRY_PAGE {
            return Err(MultipartRepositoryError::Conflict);
        }
        let prefix = MetadataKey::multipart_session_prefix(&self.tenant, bucket);
        let end = MetadataKey::multipart_session_end(&self.tenant, bucket);
        let start = match after {
            Some(after)
                if after.starts_with(&prefix) && after.len() > prefix.len() && after < end.as_slice() =>
            {
                let mut next = after.to_vec();
                next.push(0);
                next
            }
            Some(_) => return Err(MultipartRepositoryError::Conflict),
            None => prefix,
        };
        let page = self
            .store
            .scan_page(start, end, max_items, MAX_EXPIRY_SCAN_BYTES, None)
            .await?;
        let next = if page.continuation.is_some() {
            Some(
                page.items
                    .last()
                    .ok_or(MultipartRepositoryError::Conflict)?
                    .key
                    .clone(),
            )
        } else {
            None
        };
        let mut expired = 0;
        for index in page.items {
            expired += usize::from(self.expire_index(bucket, index, now_ms).await?);
        }
        Ok(MultipartExpiryPage { next, expired })
    }

    async fn expire_index(
        &self,
        bucket: BucketId,
        index: crowdb_protocol::chunk_kv::RpcValue,
        now_ms: u64,
    ) -> Result<bool, MultipartRepositoryError> {
        let Some(session_value) = self.store.get(index.value).await? else {
            return Ok(false);
        };
        let session = MultipartSessionRecord::decode_unbound(&session_value.value)?;
        if session.bucket_id != bucket
            || MetadataKey::multipart_upload_index(
                &self.tenant,
                bucket,
                &session.object_key,
                &session.upload_id,
            )? != index.key
            || MetadataKey::multipart_session(&self.tenant, bucket, &session.upload_id) != session_value.key
        {
            return Err(MultipartRepositoryError::Conflict);
        }
        if session.phase != MultipartPhase::Open || session.expires_ms > now_ms {
            return Ok(false);
        }
        match self.abort_observed(session).await {
            Ok(_) => Ok(true),
            Err(MultipartRepositoryError::Conflict) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Logically aborts an open upload, retaining part evidence for cleanup.
    ///
    /// # Errors
    /// Rejects an upload whose completion or publication already won.
    pub async fn abort(
        &self,
        session: &MultipartSessionRecord,
    ) -> Result<MultipartSessionRecord, MultipartRepositoryError> {
        let current = self
            .load(session)
            .await?
            .ok_or(MultipartRepositoryError::Conflict)?;
        self.abort_observed(current).await
    }

    async fn abort_observed(
        &self,
        mut current: MultipartSessionRecord,
    ) -> Result<MultipartSessionRecord, MultipartRepositoryError> {
        if current.pending.is_some() {
            current = self.settle_current_part(&current).await?.current;
            if current.pending.is_some() {
                return Err(MultipartRepositoryError::Busy);
            }
        }
        if current.phase == MultipartPhase::Aborted {
            return Ok(current);
        }
        if current.phase != MultipartPhase::Open {
            return Err(MultipartRepositoryError::Conflict);
        }
        let mut next = current.clone();
        next.revision = next_revision(current.revision).ok_or(MultipartRepositoryError::Conflict)?;
        next.phase = MultipartPhase::Aborted;
        match self.exchange_observed(&current, &next).await? {
            MultipartExchange::Applied => Ok(next),
            MultipartExchange::Conflict(Some(observed)) if observed.phase == MultipartPhase::Aborted => {
                Ok(*observed)
            }
            MultipartExchange::Conflict(_) => Err(MultipartRepositoryError::Conflict),
        }
    }
}
