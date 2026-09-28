use crate::{
    catalog::{CatalogContext, CatalogError},
    error::ValidationError,
    file::MultipartPhase,
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
            discovery_scope: 0,
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
            quarantined_from: None,
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
            scope: Some(match task.discovery_scope {
                0 => CatalogScope::File,
                1 => CatalogScope::MultipartPart,
                _ => CatalogScope::MultipartSession,
            }),
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
            if let Some(candidate) = self
                .discovery_candidate(task, &key, &item.value, limits, now_ms)
                .await?
            {
                self.claim_candidate(&candidate).await?;
            }
        }
        let mut next = task.progress()?;
        if let Some(last) = page.items.last() {
            next.scan_after.clone_from(&last.key);
        } else {
            next.scan_after.clear();
            if task.discovery_scope < 2 {
                next.discovery_scope += 1;
            } else if task.phase == GcPhase::Rescan {
                next.discovery_scope = 0;
                next.phase = if task.kind == GcTaskKind::RetiredCatalog {
                    GcPhase::PreSweepSystem
                } else {
                    GcPhase::Sweep
                };
                next.deferred_ranges = false;
                next.sweep_round = task
                    .sweep_round
                    .checked_add(1)
                    .ok_or(ValidationError::GenerationExhausted)?;
            } else {
                next.discovery_scope = 0;
                next.phase = if task.kind == GcTaskKind::RetiredCatalog {
                    GcPhase::RootsSystem
                } else {
                    GcPhase::Roots
                };
            }
        }
        self.update(task, &next).await?;
        Ok(next)
    }

    async fn discovery_candidate(
        &self,
        task: &GcTask,
        key: &IcebergKey,
        bytes: &[u8],
        limits: GcLimits,
        now_ms: u64,
    ) -> Result<Option<GcCandidate>, CatalogError> {
        let (file, part, assembly) = match key {
            IcebergKey::Catalog {
                scope: CatalogScope::File,
                ..
            } => {
                let StorageRecord::File(file) = StorageRecord::decode(key, bytes)? else {
                    return Err(ValidationError::Record.into());
                };
                (*file, None, None)
            }
            IcebergKey::Catalog {
                scope: CatalogScope::MultipartPart,
                ..
            } => {
                let StorageRecord::MultipartPart(part) = StorageRecord::decode(key, bytes)? else {
                    return Err(ValidationError::Record.into());
                };
                if !self.part_is_abandoned(task, &part, now_ms).await? {
                    return Ok(None);
                }
                (GcCandidate::part_file(&part)?, Some(*part), None)
            }
            IcebergKey::Catalog {
                scope: CatalogScope::MultipartSession,
                ..
            } => {
                let StorageRecord::MultipartSession(session) = StorageRecord::decode(key, bytes)? else {
                    return Err(ValidationError::Record.into());
                };
                if session
                    .completion
                    .as_ref()
                    .and_then(|completion| completion.progress.writer.as_ref())
                    .is_none()
                    || !self.session_is_abandoned(task, &session, now_ms).await?
                {
                    return Ok(None);
                }
                (GcCandidate::assembly_file(&session)?, None, Some(session))
            }
            _ => return Err(ValidationError::Record.into()),
        };
        if task
            .head
            .as_ref()
            .is_some_and(|head| head.table != file.location.table().table)
        {
            return Ok(None);
        }
        let mut candidate = GcCandidate {
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
            file,
            part,
            assembly,
            next_root: 0,
        };
        candidate.cursor = candidate.initial_cursor()?;
        Ok(Some(candidate))
    }

    pub(super) async fn part_is_abandoned(
        &self,
        task: &GcTask,
        part: &crate::file::MultipartPart,
        now_ms: u64,
    ) -> Result<bool, CatalogError> {
        let key = IcebergKey::Catalog {
            catalog: task.context.catalog,
            scope: CatalogScope::MultipartSession,
            suffix: part.upload.as_bytes().to_vec(),
        };
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(false);
        };
        let StorageRecord::MultipartSession(session) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        part.validate_for(&session)?;
        self.session_is_abandoned(task, &session, now_ms).await
    }

    pub(super) async fn session_is_abandoned(
        &self,
        task: &GcTask,
        session: &crate::file::MultipartSession,
        now_ms: u64,
    ) -> Result<bool, CatalogError> {
        let terminal = matches!(
            session.phase,
            MultipartPhase::Published | MultipartPhase::Aborted | MultipartPhase::Conflicted
        );
        let authority_key = IcebergKey::Catalog {
            catalog: task.context.catalog,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        };
        let value = self
            .store
            .get(&authority_key.encode()?)
            .await?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::Authority(authority) = StorageRecord::decode(&authority_key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        let deadline = session
            .expires_ms
            .checked_add(authority.admission_bounds.request_ms)
            .and_then(|time| time.checked_add(authority.admission_bounds.clock_skew_ms))
            .ok_or(ValidationError::Deadline)?;
        Ok(terminal && session.context == task.context && session.pending.is_none() && now_ms >= deadline)
    }
}
