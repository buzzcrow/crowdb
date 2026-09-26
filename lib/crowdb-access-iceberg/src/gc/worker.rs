use std::sync::{atomic::AtomicUsize, Arc};

use crowdb_chunk_client::ReclaimOutcome;

use crate::{
    catalog::{CatalogError, CatalogLifecycle, RootState},
    error::ValidationError,
    file::{file_key, location_key, FileBlockStore, FileIoError},
    key::{CatalogScope, IcebergKey, SystemScope},
    record::StorageRecord,
};

use super::{
    CandidatePhase, GcCandidate, GcLimits, GcPhase, GcRepository, GcScan, GcStalledReason, GcTask,
    GcTaskKind, ReclaimStep,
};

mod admission;
mod assembly;
mod cleanup;
mod inactive;
mod live;
mod sweep;
mod system;
mod terminal;
mod writes;
pub use admission::GcWorkerStatus;

#[derive(Debug, thiserror::Error)]
pub enum GcWorkError {
    #[error(transparent)]
    Mark(#[from] super::GcMarkError),
    #[error("background reclamation step timed out; durable state is retained")]
    Timeout,
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Invalid(#[from] ValidationError),
    #[error(transparent)]
    Io(#[from] FileIoError),
}

pub struct GcWorker {
    repository: GcRepository,
    blocks: Arc<dyn FileBlockStore>,
    limits: GcLimits,
    active: AtomicUsize,
}

impl GcWorker {
    /// # Errors
    /// Rejects invalid independent background budgets.
    pub fn new(
        repository: GcRepository,
        blocks: Arc<dyn FileBlockStore>,
        limits: GcLimits,
    ) -> Result<Self, ValidationError> {
        limits.validate()?;
        Ok(Self {
            repository,
            blocks,
            limits,
            active: AtomicUsize::new(0),
        })
    }

    /// Advances durable work by one bounded discovery, proof or deletion step.
    /// # Errors
    /// Storage and corruption errors leave the last durable continuation intact.
    pub async fn step(&self, task: &GcTask, now_ms: u64) -> Result<GcTask, GcWorkError> {
        task.validate()?;
        if task.paused
            || now_ms < task.retry_at_ms
            || matches!(task.phase, GcPhase::Complete | GcPhase::Quarantined)
        {
            return Ok(task.clone());
        }
        if self
            .repository
            .task(task.context.catalog, task.identity)
            .await?
            .as_ref()
            != Some(task)
        {
            return Err(CatalogError::Conflict.into());
        }
        self.repository.verify_retirement_access(task).await?;
        match task.phase {
            GcPhase::Discover => Ok(self.repository.discover_files(task, self.limits, now_ms).await?),
            GcPhase::Rescan => {
                self.verify_inactive(task).await?;
                if task.kind != GcTaskKind::RetiredCatalog {
                    self.repository.verify_table_fence(task).await?;
                }
                Ok(self.repository.discover_files(task, self.limits, now_ms).await?)
            }
            GcPhase::Roots if task.kind == GcTaskKind::LiveTable => self.live_roots(task, now_ms).await,
            GcPhase::Mark if task.kind == GcTaskKind::LiveTable => self.live_mark(task).await,
            GcPhase::Roots => self.inactive_roots(task, now_ms).await,
            GcPhase::Fence if task.kind == GcTaskKind::LiveTable => self.live_fence(task, now_ms).await,
            GcPhase::Fence => self.fence(task, now_ms).await,
            GcPhase::Sweep => self.sweep(task, now_ms).await,
            GcPhase::SweepWrites => self.sweep_writes(task, now_ms).await,
            GcPhase::CleanupSystem => self.cleanup_system(task, now_ms).await,
            GcPhase::CleanupCatalog => self.cleanup_catalog(task, now_ms).await,
            GcPhase::VerifyCleanup => self.verify_cleanup(task).await,
            GcPhase::CleanupGc => self.cleanup_gc(task).await,
            GcPhase::RootsSystem | GcPhase::PreSweepSystem => self.scan_system_protection(task, now_ms).await,
            GcPhase::Waiting => {
                let mut next = task.advance()?;
                next.phase = if task.kind == GcTaskKind::RetiredCatalog {
                    GcPhase::RootsSystem
                } else {
                    GcPhase::Roots
                };
                next.scan_after.clear();
                next.stalled = GcStalledReason::None;
                self.repository.update(task, &next).await?;
                Ok(next)
            }
            _ => Err(CatalogError::Busy.into()),
        }
    }

    async fn verify_inactive(&self, task: &GcTask) -> Result<(), GcWorkError> {
        let root_key = IcebergKey::System {
            scope: SystemScope::ActiveRoot,
            suffix: Vec::new(),
        };
        let value = self
            .repository
            .store
            .get(&root_key.encode()?)
            .await
            .map_err(CatalogError::from)?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::Active(root) = StorageRecord::decode(&root_key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if root.state != RootState::Ready {
            return Err(CatalogError::Busy.into());
        }
        match task.kind {
            GcTaskKind::RetiredCatalog => {
                if root.context.catalog == task.context.catalog
                    || root.context.activation_epoch <= task.context.activation_epoch
                {
                    return Err(CatalogError::Conflict.into());
                }
                let key = IcebergKey::Catalog {
                    catalog: task.context.catalog,
                    scope: CatalogScope::Authority,
                    suffix: Vec::new(),
                };
                let value = self
                    .repository
                    .store
                    .get(&key.encode()?)
                    .await
                    .map_err(CatalogError::from)?
                    .ok_or(ValidationError::Record)?;
                let StorageRecord::Authority(authority) = StorageRecord::decode(&key, &value.bytes)? else {
                    return Err(ValidationError::Record.into());
                };
                if authority.lifecycle != CatalogLifecycle::Retired {
                    return Err(CatalogError::Busy.into());
                }
            }
            GcTaskKind::PurgeTable => {
                if root.context != task.context {
                    return Err(CatalogError::Conflict.into());
                }
                let purge = crate::table::TablePurgeTask {
                    activation_epoch: task.context.activation_epoch,
                    head: task.head.clone().ok_or(ValidationError::Record)?,
                };
                let key = purge.key();
                let value = self
                    .repository
                    .store
                    .get(&key.encode()?)
                    .await
                    .map_err(CatalogError::from)?
                    .ok_or(ValidationError::Record)?;
                if StorageRecord::decode(&key, &value.bytes)?
                    != StorageRecord::TablePurgeTask(Box::new(purge))
                {
                    return Err(ValidationError::Record.into());
                }
            }
            GcTaskKind::LiveTable => {
                if root.context != task.context || !task.fenced || !task.proof.complete {
                    return Err(CatalogError::Busy.into());
                }
            }
        }
        Ok(())
    }

    fn candidates(&self, task: &GcTask) -> GcScan {
        GcScan {
            catalog: task.context.catalog,
            scope: Some(CatalogScope::GcCandidate),
            prefix: task
                .head
                .as_ref()
                .map_or_else(Vec::new, |head| head.table.as_bytes().to_vec()),
            after: task.scan_after.clone(),
            items: 1,
            bytes: (crate::record::MAX_RECORD_BYTES + crate::key::MAX_KEY_BYTES)
                .min(self.limits.step_bytes as usize),
        }
    }
}
