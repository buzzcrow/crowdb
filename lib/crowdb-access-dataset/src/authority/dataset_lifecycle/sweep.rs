// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::DatasetEnvelope;
use crate::{key, AuthorityError, DatasetAuthority, MetadataScan, MetadataScanBudget, ReadCancellation};
use std::time::Duration;

/// Discardable discovery cursor. Namespace records own all persistent lifecycle state.
#[derive(Debug, Default)]
pub struct DatasetDeletionSweep {
    after: Option<Vec<u8>>,
}
impl DatasetDeletionSweep {
    #[must_use]
    pub fn has_more(&self) -> bool {
        self.after.is_some()
    }
}

impl DatasetAuthority {
    /// Settles orphan creation intents and completes due empty-Dataset retirement.
    /// A minimal identity receipt and child fences remain for safe retries/name reuse.
    /// # Errors
    /// Returns corrupt metadata, cancellation and bounded storage failures.
    pub async fn reclaim_datasets_page(
        &self,
        cursor: &mut DatasetDeletionSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), AuthorityError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.reclaim_datasets_window(cursor, cancel),
        )
        .await
        .map_err(|_| crate::DatasetError::ReadLimitExceeded)?
    }

    async fn reclaim_datasets_window(
        &self,
        cursor: &mut DatasetDeletionSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), AuthorityError> {
        let page = self
            .store
            .scan_page(
                &MetadataScan {
                    prefix: key::DATASET_PREFIX.to_vec(),
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
        for (key, value) in page.entries {
            if cancel.is_cancelled() {
                return Err(crate::DatasetError::ReadCancelled.into());
            }
            // The root retains all work. Move discovery first so a corrupt or
            // stalled root cannot starve later roots; the next sweep retries it.
            cursor.after = Some(key.clone());
            let mut envelope = DatasetEnvelope::decode(&value.bytes)?;
            if crate::key::dataset(&envelope.record.identity)? != key {
                return Err(AuthorityError::Corrupt);
            }
            if let Some(deletion) = &mut envelope.deletion {
                if !deletion.complete && self.reclaim_time() >= deletion.reclaim_after {
                    deletion.complete = true;
                    self.store
                        .compare_exchange(&key, Some(&value.bytes), &envelope.encode()?)
                        .await?;
                }
            } else if !envelope.pending.is_empty() {
                let count = envelope.pending.len();
                self.settle_dataset_reservations(&mut envelope).await?;
                if envelope.pending.len() != count {
                    self.store
                        .compare_exchange(&key, Some(&value.bytes), &envelope.encode()?)
                        .await?;
                }
            }
        }
        cursor.after = page.after;
        Ok(())
    }
}
