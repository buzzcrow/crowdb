use crate::{
    catalog::{check_context, CatalogContext, CatalogError},
    error::ValidationError,
    key::OperationId,
    record::StorageRecord,
    table::{head_key, TablePurgeTask},
};

use super::{GcLimits, GcRepository, GcTask, GcTaskKind};

impl GcRepository {
    /// Installs one replayable purge worker for a durable table purge marker.
    /// # Errors
    /// Rejects stale catalog epochs, changed markers and conflicting task identities.
    pub async fn admit_purge(
        &self,
        context: CatalogContext,
        marker: &TablePurgeTask,
        now_ms: u64,
        limits: GcLimits,
    ) -> Result<GcTask, CatalogError> {
        marker.validate()?;
        if marker.head.catalog != context.catalog || marker.activation_epoch != context.activation_epoch {
            return Err(ValidationError::IdentityMismatch.into());
        }
        check_context(self.store.as_ref(), context).await?;
        let key = marker.key();
        let value = self.store.get(&key.encode()?).await?.ok_or(CatalogError::Busy)?;
        if StorageRecord::decode(&key, &value.bytes)?
            != StorageRecord::TablePurgeTask(Box::new(marker.clone()))
        {
            return Err(CatalogError::Conflict);
        }
        let identity = OperationId::from_bytes(marker.head.table.as_bytes())?;
        if let Some(existing) = self.task(context.catalog, identity).await? {
            if existing.context != context
                || existing.kind != GcTaskKind::PurgeTable
                || existing.head.as_ref() != Some(&marker.head)
            {
                return Err(CatalogError::Conflict);
            }
            return Ok(existing);
        }
        let head_key = head_key(context.catalog, marker.head.table);
        let head = self
            .store
            .get(&head_key.encode()?)
            .await?
            .ok_or(CatalogError::Busy)?;
        if StorageRecord::decode(&head_key, &head.bytes)?
            != StorageRecord::TableHead(Box::new(marker.head.clone()))
        {
            return Err(CatalogError::Conflict);
        }
        let task = GcTask::plan(context, identity, Some(marker.head.clone()), now_ms, limits)?;
        self.create(&task).await?;
        Ok(task)
    }
}
