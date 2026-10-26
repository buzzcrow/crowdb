// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    decode_part, part_count, part_key, DatasetUploads, PartState, UploadError, UploadPart, UploadRecord,
};
use crate::{MetadataScan, MetadataScanBudget, ReadCancellation};
use futures::{stream, StreamExt, TryStreamExt};
use std::time::Duration;

const PREFIX_PARTS: u32 = 256;
const PAGE_PARTS: usize = 128;

impl DatasetUploads {
    pub(super) async fn accepted_parts(
        &self,
        location: &[u8],
        session: &UploadRecord,
    ) -> Result<Vec<UploadPart>, UploadError> {
        stream::iter(0..part_count(session).div_ceil(PREFIX_PARTS))
            .map(|page| self.accepted_part_prefix(location, session, page))
            .buffered(16)
            .try_fold(Vec::new(), |mut output, parts| async move {
                output.extend(parts);
                Ok(output)
            })
            .await
    }

    async fn accepted_part_prefix(
        &self,
        location: &[u8],
        session: &UploadRecord,
        page: u32,
    ) -> Result<Vec<UploadPart>, UploadError> {
        let mut prefix = part_key(location, page * PREFIX_PARTS);
        prefix.pop();
        let mut after = None;
        let mut output = Vec::new();
        loop {
            let result = self
                .store
                .scan_page(
                    &MetadataScan {
                        prefix: prefix.clone(),
                        after: after.clone(),
                        budget: MetadataScanBudget {
                            items: PAGE_PARTS,
                            bytes: 1024 * 1024,
                            timeout: Duration::from_secs(2),
                        },
                    },
                    &ReadCancellation::new(),
                )
                .await?;
            for (key, stored) in &result.entries {
                let entry = decode_part(&stored.bytes)?;
                if entry.part.index >= part_count(session)
                    || key != &part_key(location, entry.part.index)
                    || !key.starts_with(&prefix)
                    || after.as_ref().is_some_and(|last| key <= last)
                {
                    return Err(UploadError::Integrity);
                }
                after = Some(key.clone());
                if entry.state == PartState::Accepted {
                    let start = u64::from(entry.part.index) * u64::from(session.part_size);
                    if entry.part.length != (session.length - start).min(u64::from(session.part_size)) {
                        return Err(UploadError::Integrity);
                    }
                    output.push(entry.part);
                }
            }
            let Some(cursor) = result.after else {
                return Ok(output);
            };
            if result.entries.is_empty()
                || !cursor.starts_with(&prefix)
                || after.as_ref().map_or(true, |last| cursor < *last)
            {
                return Err(UploadError::Integrity);
            }
            after = Some(cursor);
        }
    }
}
