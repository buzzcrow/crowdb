// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{decode_session, DatasetUploads, UploadError, UploadState};
use crate::{MetadataScan, MetadataScanBudget, ReadCancellation};
use std::time::Duration;

const PREFIX: &[u8] = b"DS\0U\0";

impl DatasetUploads {
    /// Scans one bounded page of upload records. The caller-owned cursor is an
    /// optimization only; restarting the scan cannot lose a cleanup obligation.
    /// # Errors
    /// Reports corrupt records or cleanup/storage failures. An item failure
    /// advances the cursor so unrelated uploads can progress before it is retried.
    pub async fn expire(&self, now: u64, after: &mut Option<Vec<u8>>) -> Result<(), UploadError> {
        let page = self
            .store
            .scan_page(
                &MetadataScan {
                    prefix: PREFIX.to_vec(),
                    after: after.clone(),
                    budget: MetadataScanBudget {
                        items: 20,
                        bytes: 1024 * 1024,
                        timeout: Duration::from_secs(2),
                    },
                },
                &ReadCancellation::new(),
            )
            .await?;
        for (key, stored) in &page.entries {
            *after = Some(key.clone());
            if !is_session_key(key)? {
                continue;
            }
            let session = decode_session(&stored.bytes)?;
            if session.cleanup_complete || !session.owners.is_empty() {
                continue;
            }
            if session.state == UploadState::Aborted || now >= session.expires_at {
                match self.abort(key).await {
                    Ok(_) | Err(UploadError::Conflict) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        *after = page.after;
        Ok(())
    }
}

// Qualified identity = length-prefixed namespace segments, 0xff, name;
// the upload ID follows it. Part and index records append six bytes.
fn is_session_key(key: &[u8]) -> Result<bool, UploadError> {
    let mut rest = key.strip_prefix(PREFIX).ok_or(UploadError::Integrity)?;
    while rest.first() != Some(&0xff) {
        rest = skip_name(rest)?;
    }
    rest = skip_name(&rest[1..])?;
    let id = rest.get(..16).ok_or(UploadError::Integrity)?;
    crate::OperationId::from_bytes(id).map_err(|_| UploadError::Integrity)?;
    match &rest[16..] {
        [] => Ok(true),
        [0, b'P' | b'I', _, _, _, _] => Ok(false),
        _ => Err(UploadError::Integrity),
    }
}

fn skip_name(bytes: &[u8]) -> Result<&[u8], UploadError> {
    let length: [u8; 2] = bytes
        .get(..2)
        .ok_or(UploadError::Integrity)?
        .try_into()
        .map_err(|_| UploadError::Integrity)?;
    let length = usize::from(u16::from_be_bytes(length));
    if length == 0 || length > 255 {
        return Err(UploadError::Integrity);
    }
    bytes.get(2 + length..).ok_or(UploadError::Integrity)
}
