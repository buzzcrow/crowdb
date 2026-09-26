use crate::{
    catalog::CatalogError,
    error::ValidationError,
    record::StorageRecord,
    table::{head_key, TableHead, TableLifecycle},
};

use super::{GcPhase, GcRepository, GcStalledReason, GcTask, GcTaskKind};

impl GcRepository {
    /// Stops a previously admitted live pass without deleting further files.
    /// # Errors
    /// Rejects changed ownership or an unresolved table fence.
    pub async fn retire_live(&self, task: &GcTask) -> Result<GcTask, CatalogError> {
        task.validate()?;
        if task.kind != GcTaskKind::LiveTable {
            return Err(ValidationError::Record.into());
        }
        if self.task(task.context.catalog, task.identity).await?.as_ref() != Some(task) {
            return Err(CatalogError::Conflict);
        }
        if task.phase == GcPhase::Complete {
            return Ok(task.clone());
        }
        let expected = fenced_head(task)?;
        let key = head_key(expected.catalog, expected.table);
        if let Some(value) = self.store.get(&key.encode()?).await? {
            let StorageRecord::TableHead(current) = StorageRecord::decode(&key, &value.bytes)? else {
                return Err(ValidationError::Record.into());
            };
            if current.as_ref() == &expected {
                self.release_table_fence(task).await?;
            } else if current.lifecycle == TableLifecycle::Reclaiming
                && current.pending_operation == Some(task.identity)
            {
                return Err(CatalogError::Conflict);
            }
        }
        let mut next = task.progress()?;
        next.phase = GcPhase::Complete;
        next.quarantined_from = None;
        next.fenced = false;
        next.paused = false;
        next.stalled = GcStalledReason::None;
        next.scan_after.clear();
        next.discovery_scope = 0;
        next.retry_at_ms = 0;
        self.update(task, &next).await?;
        Ok(next)
    }

    pub(super) async fn table_fence_released(&self, task: &GcTask) -> Result<bool, CatalogError> {
        let mut released = task.head.clone().ok_or(ValidationError::Record)?;
        released.operation_fence = released
            .operation_fence
            .checked_add(2)
            .ok_or(ValidationError::GenerationExhausted)?;
        let key = head_key(released.catalog, released.table);
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(false);
        };
        Ok(StorageRecord::decode(&key, &value.bytes)? == StorageRecord::TableHead(Box::new(released)))
    }

    /// # Errors
    /// Rejects changed heads and unresolved foreground publications.
    pub async fn fence_table(&self, task: &GcTask) -> Result<TableHead, CatalogError> {
        task.validate()?;
        if self.task(task.context.catalog, task.identity).await?.as_ref() != Some(task) {
            return Err(CatalogError::Conflict);
        }
        let before = task.head.as_ref().ok_or(ValidationError::Record)?;
        if before.lifecycle == TableLifecycle::Reclaiming
            || (before.lifecycle == TableLifecycle::Ready && before.pending_operation.is_some())
        {
            return Err(CatalogError::Busy);
        }
        let fenced = fenced_head(task)?;
        self.change(
            &head_key(before.catalog, before.table),
            Some(&StorageRecord::TableHead(Box::new(before.clone()))),
            &StorageRecord::TableHead(Box::new(fenced.clone())),
        )
        .await?;
        Ok(fenced)
    }

    /// # Errors
    /// Rejects lost ownership; no deletion may follow a failed verification.
    pub async fn verify_table_fence(&self, task: &GcTask) -> Result<(), CatalogError> {
        let expected = fenced_head(task)?;
        let key = head_key(expected.catalog, expected.table);
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(CatalogError::Conflict)?;
        if StorageRecord::decode(&key, &value.bytes)? != StorageRecord::TableHead(Box::new(expected)) {
            return Err(CatalogError::Conflict);
        }
        Ok(())
    }

    /// # Errors
    /// Rejects lost ownership or revision exhaustion; the publication fence never rolls back.
    pub async fn release_table_fence(&self, task: &GcTask) -> Result<(), CatalogError> {
        let before = fenced_head(task)?;
        let mut released = task.head.clone().ok_or(ValidationError::Record)?;
        released.operation_fence = before
            .operation_fence
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        self.change(
            &head_key(before.catalog, before.table),
            Some(&StorageRecord::TableHead(Box::new(before))),
            &StorageRecord::TableHead(Box::new(released)),
        )
        .await
    }
}

fn fenced_head(task: &GcTask) -> Result<TableHead, ValidationError> {
    task.validate()?;
    let mut head = task.head.clone().ok_or(ValidationError::Record)?;
    head.operation_fence = head
        .operation_fence
        .checked_add(1)
        .ok_or(ValidationError::GenerationExhausted)?;
    head.lifecycle = TableLifecycle::Reclaiming;
    head.pending_operation = Some(task.identity);
    head.validate()?;
    Ok(head)
}
