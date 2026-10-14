// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{decode, encode, part_count, DatasetUploads, UploadError, UploadPart, UploadRecord};
use crate::CasOutcome;
use futures::{stream, StreamExt, TryStreamExt};
const PAGE_PARTS: u32 = 128;
impl DatasetUploads {
    pub(super) async fn accepted_parts(
        &self,
        location: &[u8],
        session: &UploadRecord,
    ) -> Result<Vec<UploadPart>, UploadError> {
        stream::iter(0..part_count(session).div_ceil(PAGE_PARTS))
            .map(|page| async move {
                let Some(stored) = self.store.get(&index_key(location, page)).await? else {
                    return Ok(Vec::new());
                };
                let parts: Vec<UploadPart> = decode(&stored.bytes)?;
                if parts.len() > PAGE_PARTS as usize
                    || parts.iter().any(|part| part.index / PAGE_PARTS != page)
                {
                    return Err(UploadError::Integrity);
                }
                Ok(parts)
            })
            .buffered(16)
            .try_fold(Vec::new(), |mut output, parts| async move {
                output.extend(parts);
                Ok(output)
            })
            .await
    }
    pub(super) async fn index_part(&self, location: &[u8], part: &UploadPart) -> Result<(), UploadError> {
        let key = index_key(location, part.index / PAGE_PARTS);
        for _ in 0..16 {
            let stored = self.store.get(&key).await?;
            let mut parts = stored
                .as_ref()
                .map(|v| decode::<Vec<UploadPart>>(&v.bytes))
                .transpose()?
                .unwrap_or_default();
            if let Some(existing) = parts.iter().find(|v| v.index == part.index) {
                return if existing == part {
                    Ok(())
                } else {
                    Err(UploadError::Conflict)
                };
            }
            if parts.len() >= PAGE_PARTS as usize {
                return Err(UploadError::Integrity);
            }
            parts.push(part.clone());
            parts.sort_by_key(|v| v.index);
            if matches!(
                self.store
                    .compare_exchange(
                        &key,
                        stored.as_ref().map(|v| v.bytes.as_slice()),
                        &encode(&parts)?
                    )
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(());
            }
        }
        Err(UploadError::Pending)
    }
}
fn index_key(location: &[u8], page: u32) -> Vec<u8> {
    let mut key = location.to_vec();
    key.extend_from_slice(b"\0I");
    key.extend_from_slice(&page.to_be_bytes());
    key
}
