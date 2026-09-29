// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Terminal multipart session transitions.

use super::{
    MetadataKey, MultipartPhase, MultipartRepository, MultipartRepositoryError, MultipartSessionRecord,
};
use crate::metadata::BucketId;

const MAX_EXPIRY_PAGE: usize = 1_000;
const MAX_EXPIRY_SCAN_BYTES: usize = 4 * 1024 * 1024;

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
            let Some(session_value) = self.store.get(index.value).await? else {
                continue;
            };
            let session = MultipartSessionRecord::decode_unbound(&session_value.value)?;
            if session.bucket_id != bucket
                || MetadataKey::multipart_upload_index(
                    &self.tenant,
                    bucket,
                    &session.object_key,
                    &session.upload_id,
                )? != index.key
                || MetadataKey::multipart_session(&self.tenant, bucket, &session.upload_id)
                    != session_value.key
            {
                return Err(MultipartRepositoryError::Conflict);
            }
            if session.phase == MultipartPhase::Open && session.expires_ms <= now_ms {
                match self.abort(&session).await {
                    Ok(_) => expired += 1,
                    Err(MultipartRepositoryError::Conflict) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(MultipartExpiryPage { next, expired })
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
        if current.phase == MultipartPhase::Aborted {
            return Ok(current);
        }
        if current.phase != MultipartPhase::Open {
            return Err(MultipartRepositoryError::Conflict);
        }
        let mut next = current.clone();
        next.revision = current
            .revision
            .checked_add(1)
            .ok_or(MultipartRepositoryError::Conflict)?;
        next.phase = MultipartPhase::Aborted;
        if self.exchange(&current, &next).await? {
            Ok(next)
        } else {
            self.load(session)
                .await?
                .filter(|record| record.phase == MultipartPhase::Aborted)
                .ok_or(MultipartRepositoryError::Conflict)
        }
    }
}
