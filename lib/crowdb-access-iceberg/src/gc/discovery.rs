use crate::{
    catalog::{CatalogContext, CatalogError},
    error::ValidationError,
    key::{CatalogScope, IcebergKey, OperationId},
    record::StorageRecord,
    table::{TableHead, TableLifecycle},
};

use super::{
    CandidatePhase, GcCandidate, GcLimits, GcPhase, GcRepository, GcScan, GcStalledReason, GcTask,
    GcTaskKind, TreeReclaimCursor,
};

impl GcTask {
    /// # Errors
    /// Rejects incoherent roots, invalid budgets and overflowing retention deadlines.
    pub fn plan(
        context: CatalogContext,
        identity: OperationId,
        head: Option<TableHead>,
        now_ms: u64,
        limits: GcLimits,
    ) -> Result<Self, ValidationError> {
        limits.validate()?;
        let kind = match &head {
            None => GcTaskKind::RetiredCatalog,
            Some(head) if head.lifecycle == TableLifecycle::Tombstone => GcTaskKind::PurgeTable,
            Some(head) if head.lifecycle == TableLifecycle::Ready => GcTaskKind::LiveTable,
            Some(_) => return Err(ValidationError::Record),
        };
        let task = Self {
            proof: super::GcProofState::default(),
            sweep_round: 0,
            deferred_ranges: false,
            context,
            identity,
            kind,
            phase: GcPhase::Discover,
            revision: 1,
            created_ms: now_ms,
            not_before_ms: now_ms
                .checked_add(limits.minimum_retention_ms)
                .ok_or(ValidationError::Deadline)?,
            retry_at_ms: 0,
            attempts: 0,
            paused: false,
            fenced: false,
            stalled: GcStalledReason::None,
            head,
            scan_after: Vec::new(),
            queue_read: 0,
            queue_write: 0,
            marked: 0,
            deleted: 0,
            reclaimed_bytes: 0,
        };
        task.validate()?;
        Ok(task)
    }
}

impl GcRepository {
    /// Adds at most one bounded page of file candidates without authorizing deletion.
    /// # Errors
    /// Rejects malformed files, changed task progress and failed durable writes.
    pub async fn discover_files(
        &self,
        task: &GcTask,
        limits: GcLimits,
        now_ms: u64,
    ) -> Result<GcTask, CatalogError> {
        limits.validate()?;
        task.validate()?;
        if !matches!(task.phase, GcPhase::Discover | GcPhase::Rescan) || task.paused {
            return Err(CatalogError::Busy);
        }
        let scan = GcScan {
            catalog: task.context.catalog,
            scope: Some(CatalogScope::File),
            prefix: Vec::new(),
            after: task.scan_after.clone(),
            items: usize::from(limits.page_items),
            bytes: (crate::record::MAX_RECORD_BYTES + crate::key::MAX_KEY_BYTES)
                .min(limits.step_bytes as usize),
        };
        let page = self.store.scan_gc(scan.clone()).await?;
        scan.validate_page(&page)?;
        for item in &page.items {
            let key = IcebergKey::decode(&item.key)?;
            let StorageRecord::File(file) = StorageRecord::decode(&key, &item.value)? else {
                return Err(ValidationError::Record.into());
            };
            if task
                .head
                .as_ref()
                .is_some_and(|head| head.table != file.location.table().table)
            {
                continue;
            }
            let candidate = GcCandidate {
                completed_round: 0,
                task: task.identity,
                generation: task.head.as_ref().map_or(0, |head| head.generation),
                first_seen_ms: now_ms.max(task.created_ms),
                not_before_ms: now_ms
                    .max(task.created_ms)
                    .checked_add(limits.minimum_retention_ms)
                    .ok_or(ValidationError::Deadline)?,
                revision: 1,
                phase: CandidatePhase::Retained,
                cursor: TreeReclaimCursor::new(&file)?,
                file: *file,
            };
            self.claim_candidate(&candidate).await?;
        }
        let mut next = task.progress()?;
        if let Some(last) = page.items.last() {
            next.scan_after.clone_from(&last.key);
        } else {
            next.scan_after.clear();
            if task.phase == GcPhase::Rescan {
                next.phase = GcPhase::Sweep;
                next.deferred_ranges = false;
                next.sweep_round = task
                    .sweep_round
                    .checked_add(1)
                    .ok_or(ValidationError::GenerationExhausted)?;
            } else {
                next.phase = GcPhase::Roots;
            }
        }
        self.update(task, &next).await?;
        Ok(next)
    }
}
