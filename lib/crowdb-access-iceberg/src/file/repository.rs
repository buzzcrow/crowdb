use std::sync::Arc;

use crate::catalog::{check_context, CasOutcome, CatalogContext, CatalogError, CatalogStore};
use crate::error::ValidationError;
use crate::operation::mutation_identity;
use crate::record::StorageRecord;

use super::{file_key, location_key, FileLocation, FileMapping, FileRecord};

#[derive(Clone)]
pub struct FileRepository {
    store: Arc<dyn CatalogStore>,
}

impl FileRepository {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self { store }
    }

    /// # Errors
    /// Rejects retired contexts, corrupt bindings and missing published authorities.
    pub async fn load(
        &self,
        context: CatalogContext,
        location: &FileLocation,
    ) -> Result<Option<FileRecord>, CatalogError> {
        self.check_context(context, location).await?;
        let result = self.resolve(location).await?;
        self.check_context(context, location).await?;
        Ok(result)
    }

    /// Publishes a sealed candidate; callers must verify chunk bytes and format before calling.
    /// # Errors
    /// Rejects invalid records, changed content, retired contexts and uncertain writes.
    pub async fn publish(
        &self,
        context: CatalogContext,
        candidate: &FileRecord,
    ) -> Result<FileRecord, CatalogError> {
        candidate.validate()?;
        self.check_context(context, &candidate.location).await?;
        if let Some(existing) = self.resolve(&candidate.location).await? {
            self.check_context(context, &candidate.location).await?;
            return compatible(existing, candidate);
        }
        self.stage(candidate).await?;
        self.check_context(context, &candidate.location).await?;
        let key = location_key(&candidate.location).encode()?;
        let bytes = StorageRecord::FileMapping(FileMapping {
            location: candidate.location.clone(),
            file: candidate.file,
        })
        .encode()?;
        let result = self
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await?;
        let published = match result {
            CasOutcome::Applied(_) => candidate.clone(),
            CasOutcome::Conflict(_) => self
                .resolve(&candidate.location)
                .await?
                .ok_or(ValidationError::Record)?,
        };
        self.check_context(context, &candidate.location).await?;
        compatible(published, candidate)
    }

    async fn stage(&self, candidate: &FileRecord) -> Result<(), CatalogError> {
        let key = file_key(candidate.location.table().catalog, candidate.file).encode()?;
        let bytes = StorageRecord::File(Box::new(candidate.clone())).encode()?;
        if let Some(existing) = self.store.get(&key).await? {
            return if existing.bytes == bytes {
                Ok(())
            } else {
                Err(CatalogError::Conflict)
            };
        }
        match self
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(existing)) if existing.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(CatalogError::Conflict),
        }
    }

    async fn resolve(&self, location: &FileLocation) -> Result<Option<FileRecord>, CatalogError> {
        let key = location_key(location);
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        let StorageRecord::FileMapping(mapping) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        let key = file_key(location.table().catalog, mapping.file);
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::File(record) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if record.location != *location {
            return Err(ValidationError::IdentityMismatch.into());
        }
        Ok(Some(*record))
    }

    async fn check_context(
        &self,
        context: CatalogContext,
        location: &FileLocation,
    ) -> Result<(), CatalogError> {
        if context.catalog != location.table().catalog {
            return Err(ValidationError::IdentityMismatch.into());
        }
        check_context(self.store.as_ref(), context).await
    }
}

fn compatible(existing: FileRecord, candidate: &FileRecord) -> Result<FileRecord, CatalogError> {
    if existing.digest != candidate.digest
        || existing.length != candidate.length
        || existing.kind != candidate.kind
        || existing.format != candidate.format
    {
        return Err(CatalogError::Conflict);
    }
    Ok(existing)
}
