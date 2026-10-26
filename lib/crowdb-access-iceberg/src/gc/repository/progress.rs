use crate::{
    catalog::{CasOutcome, CatalogError},
    error::ValidationError,
    record::StorageRecord,
};

use super::super::{GcLimits, GcPhase, GcStalledReason, GcTask};
use super::GcRepository;

impl GcRepository {
    pub(super) fn deferred(
        task: &GcTask,
        reason: GcStalledReason,
        now_ms: u64,
        limits: GcLimits,
    ) -> Result<GcTask, CatalogError> {
        limits.validate()?;
        let mut next = task.advance()?;
        next.attempts = if task.stalled == reason {
            task.attempts
                .checked_add(1)
                .ok_or(ValidationError::GenerationExhausted)?
        } else {
            1
        };
        next.stalled = reason;
        next.retry_at_ms = now_ms
            .checked_add(limits.retry_delay_ms(next.attempts))
            .ok_or(ValidationError::Deadline)?;
        if reason == GcStalledReason::Corruption && next.attempts >= u32::from(limits.corruption_attempts) {
            next.phase = GcPhase::Quarantined;
            next.quarantined_from = Some(task.phase);
        }
        Ok(next)
    }

    pub(in crate::gc) async fn defer_observed(
        &self,
        task: &GcTask,
        reason: GcStalledReason,
        now_ms: u64,
        limits: GcLimits,
    ) -> Result<GcTask, CatalogError> {
        let next = Self::deferred(task, reason, now_ms, limits)?;
        self.update_observed(task, &next).await
    }

    pub(in crate::gc) async fn update_observed(
        &self,
        before: &GcTask,
        after: &GcTask,
    ) -> Result<GcTask, CatalogError> {
        if !same_binding(before, after) || before.revision.checked_add(1) != Some(after.revision) {
            return Err(CatalogError::Conflict);
        }
        let key = before.key();
        let expected = StorageRecord::GcTask(Box::new(before.clone()));
        let next = StorageRecord::GcTask(Box::new(after.clone()));
        match self.exchange(&key, Some(&expected), &next).await? {
            CasOutcome::Applied(_) => Ok(after.clone()),
            CasOutcome::Conflict(Some(value)) => {
                let StorageRecord::GcTask(observed) = StorageRecord::decode(&key, &value.bytes)? else {
                    return Err(ValidationError::Record.into());
                };
                if !same_binding(before, &observed) || observed.revision <= before.revision {
                    return Err(CatalogError::Conflict);
                }
                Ok(*observed)
            }
            CasOutcome::Conflict(None) => Err(CatalogError::Conflict),
        }
    }
}

fn same_binding(before: &GcTask, after: &GcTask) -> bool {
    before.key() == after.key()
        && before.context == after.context
        && before.kind == after.kind
        && before.created_ms == after.created_ms
        && before.not_before_ms == after.not_before_ms
        && before.head == after.head
}
