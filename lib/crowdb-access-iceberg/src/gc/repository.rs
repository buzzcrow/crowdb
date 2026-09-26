use std::sync::Arc;

use crate::{
    catalog::{CasOutcome, CatalogError},
    error::ValidationError,
    key::{CatalogId, CatalogScope, IcebergKey, OperationId},
    operation::mutation_identity,
    record::StorageRecord,
};

use super::{GcCandidate, GcPage, GcStalledReason, GcStore, GcTask};

#[derive(Clone)]
pub struct GcRepository {
    pub(super) store: Arc<dyn GcStore>,
}

impl GcRepository {
    #[must_use]
    pub fn new(store: Arc<dyn GcStore>) -> Self {
        Self { store }
    }

    /// # Errors
    /// Rejects malformed tasks or an identity already bound to another task.
    pub async fn create(&self, task: &GcTask) -> Result<(), CatalogError> {
        self.change(&task.key(), None, &StorageRecord::GcTask(Box::new(task.clone())))
            .await
    }

    /// # Errors
    /// Rejects malformed records or storage failures.
    pub async fn task(
        &self,
        catalog: CatalogId,
        identity: OperationId,
    ) -> Result<Option<GcTask>, CatalogError> {
        let key = IcebergKey::Catalog {
            catalog,
            scope: CatalogScope::GcTask,
            suffix: identity.as_bytes().to_vec(),
        };
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        let StorageRecord::GcTask(task) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        Ok(Some(*task))
    }

    /// # Errors
    /// Rejects stale progress, ownership changes or revision gaps.
    pub async fn update(&self, before: &GcTask, after: &GcTask) -> Result<(), CatalogError> {
        if before.key() != after.key()
            || before.context != after.context
            || before.kind != after.kind
            || before.revision.checked_add(1) != Some(after.revision)
            || before.created_ms != after.created_ms
            || before.not_before_ms != after.not_before_ms
            || before.head != after.head
        {
            return Err(CatalogError::Conflict);
        }
        self.change(
            &before.key(),
            Some(&StorageRecord::GcTask(Box::new(before.clone()))),
            &StorageRecord::GcTask(Box::new(after.clone())),
        )
        .await
    }

    /// # Errors
    /// Rejects stale operator controls and storage failures.
    pub async fn pause(&self, task: &GcTask, paused: bool) -> Result<GcTask, CatalogError> {
        let mut next = task.advance()?;
        next.paused = paused;
        self.update(task, &next).await?;
        Ok(next)
    }

    /// # Errors
    /// Rejects stale progress and invalid retry deadlines.
    pub async fn defer(
        &self,
        task: &GcTask,
        reason: GcStalledReason,
        now_ms: u64,
        limits: super::GcLimits,
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
            next.phase = super::GcPhase::Quarantined;
        }
        self.update(task, &next).await?;
        Ok(next)
    }

    /// # Errors
    /// Rejects a page identity already bound to different immutable bytes.
    pub async fn put_page(&self, page: &GcPage) -> Result<(), CatalogError> {
        self.change(&page.key(), None, &StorageRecord::GcPage(Box::new(page.clone())))
            .await
    }

    /// # Errors
    /// Rejects conflicting candidate identities or stale deletion progress.
    pub async fn candidate(
        &self,
        before: Option<&GcCandidate>,
        after: &GcCandidate,
    ) -> Result<(), CatalogError> {
        if let Some(before) = before {
            if before.key() != after.key()
                || before.task != after.task
                || before.file != after.file
                || before.first_seen_ms != after.first_seen_ms
                || before.not_before_ms != after.not_before_ms
                || before.revision.checked_add(1) != Some(after.revision)
            {
                return Err(CatalogError::Conflict);
            }
        }
        let before = before.map(|candidate| StorageRecord::GcCandidate(Box::new(candidate.clone())));
        self.change(
            &after.key(),
            before.as_ref(),
            &StorageRecord::GcCandidate(Box::new(after.clone())),
        )
        .await
    }

    pub(super) async fn change(
        &self,
        key: &IcebergKey,
        before: Option<&StorageRecord>,
        after: &StorageRecord,
    ) -> Result<(), CatalogError> {
        let key = key.encode()?;
        let after = after.encode()?;
        let before = before.map(StorageRecord::encode).transpose()?;
        let expected = before.as_deref();
        match self
            .store
            .compare_exchange(&key, expected, &after, mutation_identity(&key, expected, &after))
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) if value.bytes == after => Ok(()),
            CasOutcome::Conflict(_) => Err(CatalogError::Conflict),
        }
    }
}
