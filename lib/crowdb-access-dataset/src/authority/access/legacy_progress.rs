// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::decode_access;
use crate::{
    key, AuthorityError, DatasetAuthority, MetadataScan, MetadataScanBudget, ReadCancellation,
    SnapshotReclaimError,
};
use std::time::Duration;
use tokio::time::Instant;

const SCOPES: [&[u8]; 4] = [b"DS\0GUP\0", b"DS\0GMC\0", b"DS\0GSD\0", b"DS\0CGF\0"];

/// Obsolete checkpoints are hints; permanent completion and write fences remain.
#[derive(Default, Debug)]
pub(super) struct LegacyProgressSweep {
    scope: usize,
    after: Option<Vec<u8>>,
    next: Option<Instant>,
}
impl LegacyProgressSweep {
    pub(super) fn has_more(&self) -> bool {
        self.scope != 0 || self.after.is_some()
    }
}

impl DatasetAuthority {
    pub(super) async fn sweep_legacy_progress(
        &self,
        cursor: &mut LegacyProgressSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), SnapshotReclaimError> {
        if cursor.next.is_some_and(|next| Instant::now() < next) {
            return Ok(());
        }
        let prefix = SCOPES[cursor.scope];
        let page = self
            .store
            .scan_page(
                &MetadataScan {
                    prefix: prefix.to_vec(),
                    after: cursor.after.clone(),
                    budget: MetadataScanBudget {
                        items: 20,
                        bytes: 1024 * 1024,
                        timeout: Duration::from_secs(2),
                    },
                },
                cancel,
            )
            .await?;
        for (key, _) in &page.entries {
            // A failed entry is retried on the next full pass, without starving others.
            cursor.after = Some(key.clone());
            self.delete_completed_legacy_progress(key, prefix, cancel).await?;
        }
        cursor.after = page.after;
        if cursor.after.is_none() {
            cursor.scope += 1;
            if cursor.scope == SCOPES.len() {
                cursor.scope = 0;
                cursor.next = Some(Instant::now() + Duration::from_secs(15));
            }
        }
        Ok(())
    }

    async fn delete_completed_legacy_progress(
        &self,
        checkpoint: &[u8],
        prefix: &[u8],
        cancel: &ReadCancellation,
    ) -> Result<(), SnapshotReclaimError> {
        let suffix = checkpoint.strip_prefix(prefix).ok_or(AuthorityError::Corrupt)?;
        let mut admission = key::LEASE_PREFIX.to_vec();
        admission.extend_from_slice(suffix);
        let Some(value) = self
            .store
            .get_bounded(
                &admission,
                MetadataScanBudget {
                    items: 1,
                    bytes: 256 * 1024,
                    timeout: Duration::from_secs(2),
                },
                cancel,
            )
            .await?
        else {
            return Ok(());
        };
        let access = decode_access(&value.bytes)?;
        if access.cleanup_complete {
            let Some((identity, snapshot)) = access.target else {
                return Err(AuthorityError::Corrupt.into());
            };
            if key::read_lease(&identity, snapshot).map_err(AuthorityError::from)? != admission {
                return Err(AuthorityError::Corrupt.into());
            }
            if cancel.is_cancelled() {
                return Err(AuthorityError::from(crate::DatasetError::ReadCancelled).into());
            }
            self.store.delete(checkpoint).await?;
        }
        Ok(())
    }
}
