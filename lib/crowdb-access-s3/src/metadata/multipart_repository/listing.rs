// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded listing of current part-number generations.

use super::{
    MetadataKey, MultipartPartRecord, MultipartPhase, MultipartRepository, MultipartRepositoryError,
    MultipartSessionRecord,
};
use crate::metadata::BucketId;

const MAX_LIST_PARTS: usize = 1_000;
const MAX_SCAN_BYTES: usize = 4 * 1024 * 1024;
const MAX_UPLOAD_SCAN_PAGES: usize = 8;
const MAX_UPLOAD_SCAN_ITEMS: usize = 4_096;

pub struct MultipartPartPage {
    pub parts: Vec<MultipartPartRecord>,
    pub next_part_number_marker: Option<u16>,
}

pub struct MultipartUploadPage {
    pub uploads: Vec<MultipartSessionRecord>,
    pub next: Option<(Vec<u8>, [u8; 16])>,
}

impl MultipartRepository {
    /// Lists active uploads in object-key and upload-ID order with bounded scans.
    ///
    /// Callers must create upload IDs in initiation-time order for the same key.
    /// A dense interval of terminal records returns a scan-budget error rather
    /// than an incomplete success page.
    ///
    /// # Errors
    /// Rejects invalid markers, corrupt records and exhausted scan budgets.
    pub async fn list_uploads(
        &self,
        bucket: BucketId,
        prefix: &[u8],
        key_marker: Option<&[u8]>,
        upload_marker: Option<&[u8; 16]>,
        max_uploads: usize,
        now_ms: u64,
    ) -> Result<MultipartUploadPage, MultipartRepositoryError> {
        if max_uploads == 0
            || max_uploads > MAX_LIST_PARTS
            || (upload_marker.is_some() && key_marker.is_none())
        {
            return Err(MultipartRepositoryError::Conflict);
        }
        let mut start = MetadataKey::multipart_session_key_prefix(&self.tenant, bucket, prefix)?;
        let end = MetadataKey::multipart_session_key_prefix_end(&self.tenant, bucket, prefix)?;
        if let Some(key) = key_marker {
            let mut after = MetadataKey::multipart_upload_index(
                &self.tenant,
                bucket,
                key,
                upload_marker.unwrap_or(&[u8::MAX; 16]),
            )?;
            after.push(0);
            start = start.max(after);
        }
        if start >= end {
            return Ok(MultipartUploadPage {
                uploads: Vec::new(),
                next: None,
            });
        }
        let mut uploads = Vec::with_capacity(max_uploads + 1);
        let mut continuation = None;
        let mut scanned = 0;
        for _ in 0..MAX_UPLOAD_SCAN_PAGES {
            let page = self
                .store
                .scan_page(
                    start.clone(),
                    end.clone(),
                    (MAX_UPLOAD_SCAN_ITEMS - scanned).min(MAX_LIST_PARTS),
                    MAX_SCAN_BYTES,
                    continuation,
                )
                .await?;
            scanned += page.items.len();
            for item in page.items {
                let Some(record) = self.store.get(item.value).await? else {
                    continue;
                };
                let session = MultipartSessionRecord::decode_unbound(&record.value)?;
                if session.bucket_id != bucket
                    || MetadataKey::multipart_upload_index(
                        &self.tenant,
                        bucket,
                        &session.object_key,
                        &session.upload_id,
                    )? != item.key
                    || MetadataKey::multipart_session(&self.tenant, bucket, &session.upload_id) != record.key
                {
                    return Err(MultipartRepositoryError::Conflict);
                }
                if session.phase == MultipartPhase::Open
                    && session.created_ms <= now_ms
                    && now_ms < session.expires_ms
                {
                    uploads.push(session);
                    if uploads.len() > max_uploads {
                        uploads.pop();
                        let last = uploads.last().ok_or(MultipartRepositoryError::Conflict)?;
                        let next = Some((last.object_key.clone(), last.upload_id));
                        return Ok(MultipartUploadPage { uploads, next });
                    }
                }
            }
            match page.continuation {
                None => return Ok(MultipartUploadPage { uploads, next: None }),
                Some(next) if scanned < MAX_UPLOAD_SCAN_ITEMS => continuation = Some(next),
                Some(_) => return Err(MultipartRepositoryError::ScanBudgetExhausted),
            }
        }
        Err(MultipartRepositoryError::ScanBudgetExhausted)
    }

    /// Lists current, visible part generations in ascending part-number order.
    ///
    /// # Errors
    /// Rejects a closed upload, invalid marker/limit or malformed scan data.
    pub async fn list_parts(
        &self,
        session: &MultipartSessionRecord,
        after_number: u16,
        max_parts: usize,
    ) -> Result<MultipartPartPage, MultipartRepositoryError> {
        if max_parts == 0 || max_parts > MAX_LIST_PARTS || after_number > 10_000 {
            return Err(MultipartRepositoryError::Conflict);
        }
        let current = self
            .load(session)
            .await?
            .ok_or(MultipartRepositoryError::Conflict)?;
        if current.phase != MultipartPhase::Open {
            return Err(MultipartRepositoryError::Conflict);
        }
        if after_number == 10_000 {
            return Ok(MultipartPartPage {
                parts: Vec::new(),
                next_part_number_marker: None,
            });
        }
        let prefix = MetadataKey::multipart_part_prefix(&self.tenant, current.bucket_id, &current.upload_id);
        let start = if after_number == 0 {
            prefix.clone()
        } else {
            MetadataKey::multipart_part(
                &self.tenant,
                current.bucket_id,
                &current.upload_id,
                after_number + 1,
            )?
        };
        let end = MetadataKey::multipart_part_end(&self.tenant, current.bucket_id, &current.upload_id);
        let page = self
            .store
            .scan_page(start, end, max_parts + 1, MAX_SCAN_BYTES, None)
            .await?;
        let mut parts = Vec::with_capacity(page.items.len().min(max_parts));
        let has_more = page.continuation.is_some() || page.items.len() > max_parts;
        for item in page.items.into_iter().take(max_parts) {
            let suffix = item
                .key
                .strip_prefix(prefix.as_slice())
                .ok_or(MultipartRepositoryError::InvalidPart)?;
            let number = match suffix {
                [high, low] => u16::from_be_bytes([*high, *low]),
                _ => return Err(MultipartRepositoryError::InvalidPart),
            };
            if number <= after_number
                || parts
                    .last()
                    .is_some_and(|prior: &MultipartPartRecord| prior.number >= number)
            {
                return Err(MultipartRepositoryError::InvalidPart);
            }
            parts.push(MultipartPartRecord::decode(
                &item.value,
                current.bucket_id,
                &current.upload_id,
                number,
            )?);
        }
        let next_part_number_marker = if has_more {
            Some(parts.last().ok_or(MultipartRepositoryError::InvalidPart)?.number)
        } else {
            None
        };
        Ok(MultipartPartPage {
            parts,
            next_part_number_marker,
        })
    }
}
