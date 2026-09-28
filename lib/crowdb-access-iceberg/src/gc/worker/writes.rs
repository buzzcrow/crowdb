use crate::{catalog::CasOutcome, file::FileWriteIntent, key::CatalogScope, operation::mutation_identity};

use super::{
    CatalogError, GcScan, GcStalledReason, GcTask, GcTaskKind, GcWorkError, GcWorker, IcebergKey,
    ReclaimOutcome, StorageRecord, ValidationError,
};

impl GcWorker {
    pub(super) async fn sweep_writes(&self, task: &GcTask, now_ms: u64) -> Result<GcTask, GcWorkError> {
        self.verify_inactive(task).await?;
        if task.kind != GcTaskKind::RetiredCatalog {
            if self.repository.table_fence_released(task).await? {
                return self.finish_sweep(task, task.progress()?, now_ms).await;
            }
            self.repository.verify_table_fence(task).await?;
        }
        if now_ms < task.not_before_ms {
            return Err(CatalogError::Busy.into());
        }
        let scan = GcScan {
            catalog: task.context.catalog,
            scope: Some(CatalogScope::FileWriteIntent),
            prefix: task
                .head
                .as_ref()
                .map_or_else(Vec::new, |head| head.table.as_bytes().to_vec()),
            after: task.scan_after.clone(),
            items: 1,
            bytes: (crate::record::MAX_RECORD_BYTES + crate::key::MAX_KEY_BYTES)
                .min(self.limits.step_bytes as usize),
        };
        let page = self
            .repository
            .store
            .scan_gc(scan.clone())
            .await
            .map_err(CatalogError::from)?;
        scan.validate_page(&page).map_err(CatalogError::from)?;
        let mut next = task.progress()?;
        let Some(item) = page.items.first() else {
            return self.finish_sweep(task, next, now_ms).await;
        };
        let key = IcebergKey::decode(&item.key)?;
        let StorageRecord::FileWriteIntent(intent) = StorageRecord::decode(&key, &item.value)? else {
            return Err(ValidationError::Record.into());
        };
        if self.write_owner_retained(task, &intent).await? {
            next.scan_after.clone_from(&item.key);
        } else if intent.not_before_ms == 0 {
            let mut retained = (*intent).clone();
            retained.not_before_ms = now_ms
                .max(intent.created_ms)
                .checked_add(self.limits.minimum_retention_ms)
                .ok_or(ValidationError::Deadline)?;
            self.repository
                .change(
                    &key,
                    Some(&StorageRecord::FileWriteIntent(intent)),
                    &StorageRecord::FileWriteIntent(Box::new(retained)),
                )
                .await?;
        } else if now_ms < intent.not_before_ms {
            next.stalled = GcStalledReason::Retention;
            if task.kind == GcTaskKind::LiveTable {
                next.scan_after.clone_from(&item.key);
            } else {
                next.retry_at_ms = intent.not_before_ms;
            }
        } else if !intent.deleting {
            let mut deleting = (*intent).clone();
            deleting.deleting = true;
            self.repository
                .change(
                    &key,
                    Some(&StorageRecord::FileWriteIntent(intent)),
                    &StorageRecord::FileWriteIntent(Box::new(deleting)),
                )
                .await?;
        } else {
            self.fence_write_owner(&intent).await?;
            if self.blocks.reclaim(&intent.root).await? == ReclaimOutcome::Deferred {
                next.deferred_ranges = true;
                next.stalled = GcStalledReason::UnsupportedRange;
            } else {
                self.delete_exact(&item.key, &item.value).await?;
            }
            next.scan_after.clone_from(&item.key);
        }
        self.repository.update(task, &next).await?;
        Ok(next)
    }

    async fn write_owner_retained(
        &self,
        task: &GcTask,
        intent: &FileWriteIntent,
    ) -> Result<bool, GcWorkError> {
        if task.kind == GcTaskKind::LiveTable
            && self.repository.proof_contains(task, intent.owner.file).await?
        {
            return Ok(true);
        }
        for scope in [CatalogScope::GcClaim, CatalogScope::GcAssemblyClaim] {
            let mut suffix = intent.owner.table.table.as_bytes().to_vec();
            suffix.extend_from_slice(intent.owner.file.as_bytes());
            let key = IcebergKey::Catalog {
                catalog: intent.owner.table.catalog,
                scope,
                suffix,
            };
            if let Some(value) = self
                .repository
                .store
                .get(&key.encode()?)
                .await
                .map_err(CatalogError::from)?
            {
                let StorageRecord::GcCandidate(claim) = StorageRecord::decode(&key, &value.bytes)? else {
                    return Err(ValidationError::Record.into());
                };
                let key = claim.key();
                let value = self
                    .repository
                    .store
                    .get(&key.encode()?)
                    .await
                    .map_err(CatalogError::from)?
                    .ok_or(ValidationError::Record)?;
                let StorageRecord::GcCandidate(candidate) = StorageRecord::decode(&key, &value.bytes)? else {
                    return Err(ValidationError::Record.into());
                };
                self.repository.verify_claim(&candidate).await?;
                if candidate.phase != super::CandidatePhase::Complete {
                    return if task.kind == GcTaskKind::LiveTable {
                        Ok(true)
                    } else {
                        Err(CatalogError::Busy.into())
                    };
                }
            }
        }
        let key = crate::file::file_key(intent.owner.table.catalog, intent.owner.file);
        if let Some(value) = self
            .repository
            .store
            .get(&key.encode()?)
            .await
            .map_err(CatalogError::from)?
        {
            let StorageRecord::File(file) = StorageRecord::decode(&key, &value.bytes)? else {
                return Err(ValidationError::Record.into());
            };
            if file.location.table() != intent.owner.table {
                return Err(ValidationError::IdentityMismatch.into());
            }
            if task.kind != GcTaskKind::LiveTable {
                return Err(CatalogError::Busy.into());
            }
            return Ok(true);
        }
        Ok(false)
    }

    async fn fence_write_owner(&self, intent: &FileWriteIntent) -> Result<(), GcWorkError> {
        let key = FileWriteIntent::fence_key(intent.owner);
        let encoded = key.encode()?;
        let bytes = StorageRecord::FileWriteIntent(Box::new(intent.clone())).encode()?;
        match self
            .repository
            .store
            .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
            .await
            .map_err(CatalogError::from)?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) => {
                let StorageRecord::FileWriteIntent(fence) = StorageRecord::decode(&key, &value.bytes)? else {
                    return Err(ValidationError::Record.into());
                };
                if fence.owner != intent.owner || !fence.deleting {
                    return Err(ValidationError::IdentityMismatch.into());
                }
                Ok(())
            }
            CasOutcome::Conflict(None) => Err(CatalogError::Conflict.into()),
        }
    }
}
