use crate::{catalog::CasOutcome, operation::mutation_identity};

use super::{
    file_key, location_key, CandidatePhase, CatalogError, GcCandidate, GcPhase, GcStalledReason, GcTask,
    GcTaskKind, GcWorkError, GcWorker, IcebergKey, ReclaimOutcome, ReclaimStep, StorageRecord,
    ValidationError,
};

impl GcWorker {
    pub(super) async fn sweep(&self, task: &GcTask, now_ms: u64) -> Result<GcTask, GcWorkError> {
        self.verify_inactive(task).await?;
        if now_ms < task.not_before_ms {
            return Err(CatalogError::Busy.into());
        }
        let scan = Self::candidates(task);
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
        if task.kind != GcTaskKind::RetiredCatalog {
            self.repository.verify_table_fence(task).await?;
        }
        let StorageRecord::GcCandidate(candidate) = StorageRecord::decode(&key, &item.value)? else {
            return Err(ValidationError::Record.into());
        };
        self.repository.verify_claim(&candidate).await?;
        if task.kind == GcTaskKind::LiveTable
            && self.repository.proof_contains(task, candidate.file.file).await?
        {
            next.scan_after.clone_from(&item.key);
            self.repository.update(task, &next).await?;
            return Ok(next);
        }
        if candidate.task == task.identity {
            if now_ms < candidate.not_before_ms {
                if task.kind == GcTaskKind::LiveTable {
                    next.scan_after.clone_from(&item.key);
                    next.stalled = GcStalledReason::Retention;
                    self.repository.update(task, &next).await?;
                    return Ok(next);
                }
                next.retry_at_ms = candidate.not_before_ms;
                next.stalled = GcStalledReason::Retention;
                self.repository.update(task, &next).await?;
                return Ok(next);
            }
            let progress = if candidate.phase == CandidatePhase::Complete {
                if candidate.completed_round > task.sweep_round {
                    return Err(ValidationError::Record.into());
                }
                if candidate.completed_round == task.sweep_round {
                    DeleteProgress::Complete
                } else {
                    DeleteProgress::Accounted
                }
            } else {
                self.delete_candidate(task, &candidate, now_ms, task.sweep_round)
                    .await?
            };
            match progress {
                DeleteProgress::Advanced => {}
                DeleteProgress::Accounted => next.scan_after.clone_from(&item.key),
                DeleteProgress::Complete => {
                    next.scan_after.clone_from(&item.key);
                    next.deleted = next
                        .deleted
                        .checked_add(1)
                        .ok_or(ValidationError::GenerationExhausted)?;
                    next.reclaimed_bytes = next
                        .reclaimed_bytes
                        .checked_add(candidate.file.length)
                        .ok_or(ValidationError::GenerationExhausted)?;
                }
                DeleteProgress::Deferred => {
                    next.deferred_ranges = true;
                    next.scan_after.clone_from(&item.key);
                    next.stalled = GcStalledReason::UnsupportedRange;
                }
            }
        } else if candidate.phase == CandidatePhase::Complete {
            next.scan_after.clone_from(&item.key);
        } else {
            self.repository.adopt_candidate(task, &candidate).await?;
        }
        self.repository.update(task, &next).await?;
        Ok(next)
    }

    async fn finish_sweep(
        &self,
        task: &GcTask,
        mut next: GcTask,
        now_ms: u64,
    ) -> Result<GcTask, GcWorkError> {
        next.phase = if task.deferred_ranges {
            next.stalled = GcStalledReason::UnsupportedRange;
            GcPhase::Waiting
        } else if task.kind == GcTaskKind::RetiredCatalog {
            GcPhase::CleanupSystem
        } else {
            GcPhase::Complete
        };
        next.scan_after.clear();
        next.retry_at_ms = if next.phase == GcPhase::Waiting {
            now_ms
                .checked_add(u64::from(self.limits.retry_max_ms))
                .ok_or(ValidationError::Deadline)?
        } else {
            next.stalled = GcStalledReason::None;
            0
        };
        if task.kind == GcTaskKind::LiveTable {
            self.repository.release_table_fence(task).await?;
            next.fenced = false;
            next.phase = GcPhase::Complete;
        } else if task.kind == GcTaskKind::PurgeTable && next.phase == GcPhase::Complete {
            self.repository.release_table_fence(task).await?;
            next.fenced = false;
        }
        self.repository.update(task, &next).await?;
        Ok(next)
    }

    async fn delete_candidate(
        &self,
        task: &GcTask,
        candidate: &GcCandidate,
        now_ms: u64,
        sweep_round: u64,
    ) -> Result<DeleteProgress, GcWorkError> {
        if now_ms < candidate.not_before_ms {
            return Err(CatalogError::Busy.into());
        }
        if let Some(part) = &candidate.part {
            let key = part.key();
            let stored = self
                .repository
                .store
                .get(&key.encode()?)
                .await
                .map_err(CatalogError::from)?;
            if stored.is_none() && (!candidate.cursor.frames.is_empty() || candidate.cursor.pending.is_some())
            {
                return Err(ValidationError::Record.into());
            }
            if let Some(stored) = stored {
                if StorageRecord::decode(&key, &stored.bytes)?
                    != StorageRecord::MultipartPart(Box::new(part.clone()))
                {
                    return Err(CatalogError::Conflict.into());
                }
            }
            if !self.repository.part_is_abandoned(task, part, now_ms).await? {
                return Err(CatalogError::Busy.into());
            }
        }
        let mut next = candidate.clone();
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        next.phase = CandidatePhase::Deleting;
        if candidate.phase == CandidatePhase::Retained {
            self.repository.candidate(Some(candidate), &next).await?;
            return Ok(DeleteProgress::Advanced);
        }
        if let Some(root) = &candidate.cursor.pending {
            if self.blocks.reclaim(root).await? == ReclaimOutcome::Deferred {
                next.phase = CandidatePhase::Deferred;
                self.repository.candidate(Some(candidate), &next).await?;
                return Ok(DeleteProgress::Deferred);
            }
            next.cursor = candidate.cursor.acknowledge(root)?;
        } else {
            if candidate.cursor.frames.last().is_some_and(|frame| {
                frame.root.height > 0 && frame.root.logical_length > u64::from(self.limits.step_bytes)
            }) {
                return Err(super::FileIoError::Bounds.into());
            }
            match candidate.cursor.next(self.blocks.as_ref()).await? {
                ReclaimStep::Descended(cursor) | ReclaimStep::Delete(cursor) => next.cursor = cursor,
                ReclaimStep::Complete => {
                    self.remove_file_authority(candidate).await?;
                    next.phase = CandidatePhase::Complete;
                    next.completed_round = sweep_round;
                }
            }
        }
        self.repository.candidate(Some(candidate), &next).await?;
        Ok(if next.phase == CandidatePhase::Complete {
            DeleteProgress::Complete
        } else {
            DeleteProgress::Advanced
        })
    }

    async fn remove_file_authority(&self, candidate: &GcCandidate) -> Result<(), GcWorkError> {
        if let Some(part) = &candidate.part {
            return self
                .remove_record(&part.key(), &StorageRecord::MultipartPart(Box::new(part.clone())))
                .await;
        }
        let mapping = crate::file::FileMapping {
            file: candidate.file.file,
            location: candidate.file.location.clone(),
        };
        self.remove_record(
            &location_key(&mapping.location),
            &StorageRecord::FileMapping(mapping.clone()),
        )
        .await?;
        self.remove_record(
            &file_key(mapping.location.table().catalog, mapping.file),
            &StorageRecord::File(Box::new(candidate.file.clone())),
        )
        .await
    }

    async fn remove_record(&self, key: &IcebergKey, record: &StorageRecord) -> Result<(), GcWorkError> {
        let key = key.encode()?;
        let bytes = record.encode()?;
        match self
            .repository
            .store
            .delete_gc_record(&key, &bytes, mutation_identity(&key, Some(&bytes), &[]))
            .await
            .map_err(CatalogError::from)?
        {
            CasOutcome::Applied(_) | CasOutcome::Conflict(None) => Ok(()),
            CasOutcome::Conflict(_) => Err(CatalogError::Conflict.into()),
        }
    }
}

enum DeleteProgress {
    Advanced,
    Accounted,
    Complete,
    Deferred,
}
