use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use crate::{catalog::CatalogError, file::FileIoError};

use super::{GcStalledReason, GcTask, GcWorkError, GcWorker};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcWorkerStatus {
    pub active: usize,
    pub concurrency: u16,
    pub step_ms: u32,
    pub step_bytes: u32,
}

struct Permit<'worker>(&'worker AtomicUsize);

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl GcWorker {
    #[must_use]
    pub fn status(&self) -> GcWorkerStatus {
        GcWorkerStatus {
            active: self.active.load(Ordering::Acquire),
            concurrency: self.limits.concurrency,
            step_ms: self.limits.step_ms,
            step_bytes: self.limits.step_bytes,
        }
    }

    /// Runs one independently admitted step and persists bounded retry state on failure.
    /// # Errors
    /// Admission is nonblocking. Unknown progress-write outcomes remain recoverable.
    pub async fn run(&self, task: &GcTask, now_ms: u64) -> Result<GcTask, GcWorkError> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < usize::from(self.limits.concurrency)).then_some(active + 1)
            })
            .map_err(|_| CatalogError::Busy)?;
        let _permit = Permit(&self.active);
        let timeout = Duration::from_millis(u64::from(self.limits.step_ms));
        let error = match tokio::time::timeout(timeout, self.step(task, now_ms)).await {
            Ok(Ok(next)) => return Ok(next),
            Ok(Err(error)) => error,
            Err(_) => GcWorkError::Timeout,
        };
        let reason = stalled_reason(&error);
        tracing::error!(catalog = %task.context.catalog, task = %task.identity,
            error = %error, reason = ?reason, "reclamation step failed; retaining intent and deferring retry");
        let persist = async {
            match self.record_failure(task, reason, now_ms).await {
                Ok(current) => Ok(current),
                Err(error) => {
                    let observed = self.repository.task(task.context.catalog, task.identity).await?;
                    match observed {
                        Some(current) if current.revision > task.revision => Ok(current),
                        _ => Err(error),
                    }
                }
            }
        };
        tokio::time::timeout(timeout, persist)
            .await
            .map_err(|_| GcWorkError::Timeout)?
            .map_err(GcWorkError::from)
    }

    async fn record_failure(
        &self,
        task: &GcTask,
        reason: GcStalledReason,
        now_ms: u64,
    ) -> Result<GcTask, CatalogError> {
        let current = self
            .repository
            .task(task.context.catalog, task.identity)
            .await?
            .ok_or(CatalogError::Conflict)?;
        if current != *task {
            return Ok(current);
        }
        if reason == GcStalledReason::ChangedAuthority {
            if let Some(cancelled) = self.abandon_changed_live(&current).await? {
                return Ok(cancelled);
            }
        }
        if current.phase == super::GcPhase::VerifyCleanup {
            if let Some(owner) = self.repository.retirement(task.context.catalog).await? {
                if owner.identity == current.identity
                    && current.revision.checked_add(1) == Some(owner.revision)
                {
                    self.repository.update(&current, &owner).await?;
                    return Ok(owner);
                }
            }
        }
        self.repository.defer(task, reason, now_ms, self.limits).await
    }
}

fn stalled_reason(error: &GcWorkError) -> GcStalledReason {
    match error {
        GcWorkError::Mark(error) => mark_stalled_reason(error),
        GcWorkError::Invalid(_)
        | GcWorkError::Catalog(CatalogError::Invalid(_))
        | GcWorkError::Io(FileIoError::Invalid(_)) => GcStalledReason::Corruption,
        GcWorkError::Io(FileIoError::Bounds) => GcStalledReason::Resource,
        GcWorkError::Catalog(CatalogError::Conflict | CatalogError::Uninitialized) => {
            GcStalledReason::ChangedAuthority
        }
        GcWorkError::Catalog(CatalogError::Busy | CatalogError::Forbidden) => GcStalledReason::Protected,
        _ => GcStalledReason::Storage,
    }
}

fn mark_stalled_reason(error: &crate::gc::GcMarkError) -> GcStalledReason {
    use crate::{gc::GcMarkError, table::TableMetadataError};
    match error {
        GcMarkError::Catalog(error) => match error {
            CatalogError::Invalid(_) => GcStalledReason::Corruption,
            CatalogError::Conflict | CatalogError::Uninitialized => GcStalledReason::ChangedAuthority,
            CatalogError::Busy | CatalogError::Forbidden => GcStalledReason::Protected,
            CatalogError::Store(_) => GcStalledReason::Storage,
        },
        GcMarkError::Io(FileIoError::Bounds)
        | GcMarkError::Metadata(TableMetadataError::Bounds)
        | GcMarkError::Avro(crate::file::AvroContainerError::Bounds) => GcStalledReason::Resource,
        GcMarkError::Io(FileIoError::Invalid(_))
        | GcMarkError::Invalid(_)
        | GcMarkError::Avro(_)
        | GcMarkError::Metadata(_) => GcStalledReason::Corruption,
        GcMarkError::Io(_) => GcStalledReason::Storage,
    }
}
