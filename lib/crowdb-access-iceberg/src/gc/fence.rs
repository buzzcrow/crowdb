use crate::{
    catalog::CatalogError,
    error::ValidationError,
    record::StorageRecord,
    table::{head_key, TableHead, TableLifecycle},
};

use super::{GcRepository, GcTask};

impl GcRepository {
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
