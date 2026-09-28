// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded listing of current part-number generations.

use super::{
    MetadataKey, MultipartPartRecord, MultipartPhase, MultipartRepository, MultipartRepositoryError,
    MultipartSessionRecord,
};

const MAX_LIST_PARTS: usize = 1_000;
const MAX_SCAN_BYTES: usize = 4 * 1024 * 1024;

pub struct MultipartPartPage {
    pub parts: Vec<MultipartPartRecord>,
    pub next_part_number_marker: Option<u16>,
}

impl MultipartRepository {
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
