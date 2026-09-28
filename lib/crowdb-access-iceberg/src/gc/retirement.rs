use crate::{
    catalog::CatalogError,
    error::ValidationError,
    key::{CatalogId, IcebergKey},
    record::StorageRecord,
};

use super::{GcPhase, GcRepository, GcTask};

impl GcRepository {
    pub(super) async fn retirement(&self, catalog: CatalogId) -> Result<Option<GcTask>, CatalogError> {
        let key = GcTask::retirement_key(catalog);
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        let StorageRecord::GcTask(task) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        Ok(Some(*task))
    }

    pub(super) async fn verify_retirement_access(&self, task: &GcTask) -> Result<(), CatalogError> {
        if let Some(owner) = self.retirement(task.context.catalog).await? {
            if owner.context != task.context
                || owner.identity != task.identity
                || !matches!(
                    task.phase,
                    GcPhase::VerifyCleanup | GcPhase::CleanupGc | GcPhase::Complete
                )
            {
                return Err(CatalogError::Busy);
            }
        }
        Ok(())
    }

    pub(super) async fn check_retirement_mutation(
        &self,
        key: &IcebergKey,
        record: &StorageRecord,
    ) -> Result<(), CatalogError> {
        let IcebergKey::Catalog { catalog, .. } = key else {
            return Ok(());
        };
        let Some(owner) = self.retirement(*catalog).await? else {
            return Ok(());
        };
        match record {
            StorageRecord::GcTask(task)
                if task.context == owner.context
                    && task.identity == owner.identity
                    && (key == &owner.key() || key == &GcTask::retirement_key(*catalog))
                    && matches!(task.phase, GcPhase::CleanupGc | GcPhase::Complete) =>
            {
                Ok(())
            }
            _ => Err(CatalogError::Busy),
        }
    }
}
