use crate::{
    catalog::{check_context, CasOutcome, CatalogContext, CatalogError, CatalogLifecycle, RootState},
    error::ValidationError,
    file::DeletedFile,
    key::{CatalogScope, IcebergKey, OperationId, SystemScope},
    operation::{ledger_locate, LedgerLocation, ManagementAction, ManagementOperation, ManagementPhase},
    record::StorageRecord,
    table::TablePurgeTask,
};

mod purge;
pub use purge::PurgeAdmission;

use crate::operation::mutation_identity;

use super::{CandidatePhase, GcCandidate, GcLimits, GcRepository, GcTask, GcTaskKind, TreeReclaimCursor};

impl GcRepository {
    /// Durably queues physical cleanup after a logical object deletion.
    /// # Errors
    /// Rejects a changed catalog, conflicting file identity, or failed durable queue write.
    pub async fn claim_deleted_file(
        &self,
        context: CatalogContext,
        deleted: &DeletedFile,
        now_ms: u64,
        limits: GcLimits,
    ) -> Result<GcCandidate, CatalogError> {
        deleted.validate()?;
        let task = self.admit_multipart(context, now_ms, limits).await?;
        let candidate = GcCandidate {
            task: task.identity,
            generation: 0,
            first_seen_ms: deleted.deleted_ms,
            not_before_ms: deleted
                .deleted_ms
                .checked_add(GcTask::PURGE_DELAY_MS)
                .ok_or(ValidationError::Deadline)?,
            revision: 1,
            phase: CandidatePhase::Retained,
            completed_round: 0,
            file: deleted.file.clone(),
            part: None,
            assembly: None,
            next_root: 0,
            cursor: TreeReclaimCursor::new(&deleted.file)?,
        };
        self.claim_candidate(&candidate).await
    }
    /// Keeps one bounded multipart cleanup cursor for the active catalog.
    /// # Errors
    /// Rejects a stale context or a conflicting reserved task identity.
    pub async fn admit_multipart(
        &self,
        context: CatalogContext,
        now_ms: u64,
        limits: GcLimits,
    ) -> Result<GcTask, CatalogError> {
        check_context(self.store.as_ref(), context).await?;
        let identity = OperationId::from_bytes(b"multipart-gc-v01")?;
        if let Some(existing) = self.task(context.catalog, identity).await? {
            if existing.context != context || existing.kind != GcTaskKind::MultipartCleanup {
                return Err(CatalogError::Conflict);
            }
            if existing.phase != super::GcPhase::Complete || existing.paused {
                return Ok(existing);
            }
            let mut next = existing.advance()?;
            next.phase = super::GcPhase::Discover;
            next.discovery_scope = 0;
            next.scan_after.clear();
            next.sweep_round = 0;
            return self.exchange_admission_task(Some(&existing), &next).await;
        }
        let mut task = GcTask::plan(context, identity, None, now_ms, limits)?;
        task.kind = GcTaskKind::MultipartCleanup;
        task.not_before_ms = now_ms;
        task.validate()?;
        self.exchange_admission_task(None, &task).await
    }

    async fn exchange_admission_task(
        &self,
        before: Option<&GcTask>,
        task: &GcTask,
    ) -> Result<GcTask, CatalogError> {
        let key = task.key();
        let before = before.map(|task| StorageRecord::GcTask(Box::new(task.clone())));
        let after = StorageRecord::GcTask(Box::new(task.clone()));
        match self.exchange(&key, before.as_ref(), &after).await? {
            CasOutcome::Applied(_) => Ok(task.clone()),
            CasOutcome::Conflict(Some(value)) => {
                let StorageRecord::GcTask(current) = StorageRecord::decode(&key, &value.bytes)? else {
                    return Err(ValidationError::Record.into());
                };
                if current.context != task.context || current.kind != task.kind || current.head != task.head {
                    return Err(CatalogError::Conflict);
                }
                Ok(*current)
            }
            CasOutcome::Conflict(None) => Err(CatalogError::Conflict),
        }
    }

    /// Installs one replayable retired-catalog worker for a completed clear.
    /// # Errors
    /// Rejects unretired authority, stale roots and changed operation identity.
    pub async fn admit_retired(
        &self,
        clear: &ManagementOperation,
        now_ms: u64,
        limits: GcLimits,
    ) -> Result<GcTask, CatalogError> {
        clear.validate()?;
        if clear.request.action != ManagementAction::Clear || clear.phase != ManagementPhase::Complete {
            return Err(CatalogError::Busy);
        }
        let LedgerLocation::Existing(key, stored) =
            ledger_locate(self.store.as_ref(), SystemScope::ManagementOperation, clear.id()).await?
        else {
            return Err(CatalogError::Busy);
        };
        if StorageRecord::decode(&key, &stored.bytes)? != StorageRecord::Management(Box::new(clear.clone())) {
            return Err(CatalogError::Conflict);
        }
        let context = CatalogContext {
            catalog: clear.request.confirmation.ok_or(ValidationError::Record)?,
            activation_epoch: clear.request.expected_epoch,
        };
        context.validate()?;
        let root_key = IcebergKey::System {
            scope: SystemScope::ActiveRoot,
            suffix: Vec::new(),
        };
        let value = self
            .store
            .get(&root_key.encode()?)
            .await?
            .ok_or(CatalogError::Busy)?;
        let StorageRecord::Active(root) = StorageRecord::decode(&root_key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if root.state != RootState::Ready
            || root.context.catalog == context.catalog
            || root.context.activation_epoch <= context.activation_epoch
        {
            return Err(CatalogError::Busy);
        }
        let authority_key = IcebergKey::Catalog {
            catalog: context.catalog,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        };
        let value = self
            .store
            .get(&authority_key.encode()?)
            .await?
            .ok_or(CatalogError::Busy)?;
        let StorageRecord::Authority(authority) = StorageRecord::decode(&authority_key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if authority.lifecycle != CatalogLifecycle::Retired {
            return Err(CatalogError::Busy);
        }
        let identity = clear.request.identity.operation;
        if let Some(existing) = self.task(context.catalog, identity).await? {
            if existing.context != context || existing.kind != GcTaskKind::RetiredCatalog {
                return Err(CatalogError::Conflict);
            }
            return Ok(existing);
        }
        let task = GcTask::plan(context, identity, None, now_ms, limits)?;
        self.exchange_admission_task(None, &task).await
    }

    /// Installs a worker for an already completed table deletion.
    /// # Errors
    /// Unpublished or superseded markers do not authorize physical deletion.
    pub async fn admit_purge(
        &self,
        context: CatalogContext,
        marker: &TablePurgeTask,
        now_ms: u64,
        limits: GcLimits,
    ) -> Result<GcTask, CatalogError> {
        match self.discover_purge(context, marker, now_ms, limits).await? {
            PurgeAdmission::Ready(task) => Ok(*task),
            PurgeAdmission::Cancelled | PurgeAdmission::Deferred => Err(CatalogError::Conflict),
        }
    }
}
