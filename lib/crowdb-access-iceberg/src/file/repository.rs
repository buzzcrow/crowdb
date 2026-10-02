use std::sync::Arc;

use crate::catalog::{check_context, CasOutcome, CatalogContext, CatalogError, CatalogStore};
use crate::error::ValidationError;
use crate::gc::GcCandidate;
use crate::operation::mutation_identity;
use crate::record::StorageRecord;

use super::{file_key, location_key, DeletedFile, FileLocation, FileRecord};

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
        self.resolve(location).await
    }

    /// Publishes a candidate after callers verify transfer integrity and durable storage.
    /// # Errors
    /// Rejects invalid records, changed content, retired contexts and uncertain writes.
    pub async fn publish(
        &self,
        context: CatalogContext,
        candidate: &FileRecord,
    ) -> Result<FileRecord, CatalogError> {
        candidate.validate()?;
        self.publish_inner(context, candidate).await
    }

    /// Marks one proven-unreferenced object for delayed physical cleanup.
    /// # Errors
    /// Rejects a changed location record or retired catalog.
    pub async fn mark_deleted(
        &self,
        context: CatalogContext,
        expected: &FileRecord,
        now_ms: u64,
    ) -> Result<DeletedFile, CatalogError> {
        let location = &expected.location;
        self.check_context(context, location).await?;
        let key = location_key(location);
        let encoded = key.encode()?;
        let Some(current) = self.store.get(&encoded).await? else {
            return Err(CatalogError::Conflict);
        };
        if let StorageRecord::DeletedFile(deleted) = StorageRecord::decode(&key, &current.bytes)? {
            return if deleted.file == *expected {
                Ok(*deleted)
            } else {
                Err(CatalogError::Conflict)
            };
        }
        let file = self.resolve_value(location, &current.bytes).await?;
        if file != *expected {
            return Err(CatalogError::Conflict);
        }
        let deleted = DeletedFile {
            file,
            deleted_ms: now_ms,
        };
        let after = StorageRecord::DeletedFile(Box::new(deleted.clone())).encode()?;
        match self
            .store
            .compare_exchange(
                &encoded,
                Some(&current.bytes),
                &after,
                mutation_identity(&encoded, Some(&current.bytes), &after),
            )
            .await?
        {
            CasOutcome::Applied(_) => Ok(deleted),
            CasOutcome::Conflict(Some(value))
                if matches!(
                    StorageRecord::decode(&key, &value.bytes)?,
                    StorageRecord::DeletedFile(_)
                ) =>
            {
                let StorageRecord::DeletedFile(current) = StorageRecord::decode(&key, &value.bytes)? else {
                    unreachable!()
                };
                if current.file == *expected {
                    Ok(*current)
                } else {
                    Err(CatalogError::Conflict)
                }
            }
            CasOutcome::Conflict(_) => Err(CatalogError::Conflict),
        }
    }

    /// Returns a durable deletion marker for a retry after an uncertain response.
    /// # Errors
    /// Rejects corrupt records and stale catalog contexts.
    pub async fn deleted(
        &self,
        context: CatalogContext,
        location: &FileLocation,
    ) -> Result<Option<DeletedFile>, CatalogError> {
        self.check_context(context, location).await?;
        let key = location_key(location);
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        match StorageRecord::decode(&key, &value.bytes)? {
            StorageRecord::DeletedFile(deleted) => Ok(Some(*deleted)),
            _ => Ok(None),
        }
    }

    async fn publish_inner(
        &self,
        context: CatalogContext,
        candidate: &FileRecord,
    ) -> Result<FileRecord, CatalogError> {
        candidate.validate()?;
        self.check_context(context, &candidate.location).await?;
        let key = location_key(&candidate.location).encode()?;
        let bytes = StorageRecord::File(Box::new(candidate.clone())).encode()?;
        let result = self
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await?;
        let published = match result {
            CasOutcome::Applied(_) => candidate.clone(),
            CasOutcome::Conflict(Some(value)) => {
                let record_key = location_key(&candidate.location);
                if let StorageRecord::DeletedFile(deleted) = StorageRecord::decode(&record_key, &value.bytes)?
                {
                    if candidate.file == deleted.file.file {
                        return Err(CatalogError::Conflict);
                    }
                    let gc_key = GcCandidate::deleted_key(&deleted.file);
                    let Some(claim) = self.store.get(&gc_key.encode()?).await? else {
                        return Err(CatalogError::Busy);
                    };
                    let StorageRecord::GcCandidate(claim) = StorageRecord::decode(&gc_key, &claim.bytes)?
                    else {
                        return Err(ValidationError::Record.into());
                    };
                    if claim.file != deleted.file {
                        return Err(CatalogError::Conflict);
                    }
                    match self
                        .store
                        .compare_exchange(
                            &key,
                            Some(&value.bytes),
                            &bytes,
                            mutation_identity(&key, Some(&value.bytes), &bytes),
                        )
                        .await?
                    {
                        CasOutcome::Applied(_) => candidate.clone(),
                        CasOutcome::Conflict(_) => return Err(CatalogError::Conflict),
                    }
                } else {
                    self.resolve_value(&candidate.location, &value.bytes).await?
                }
            }
            CasOutcome::Conflict(None) => return Err(CatalogError::Busy),
        };
        compatible(published, candidate)
    }

    async fn resolve(&self, location: &FileLocation) -> Result<Option<FileRecord>, CatalogError> {
        let key = location_key(location);
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        if matches!(
            StorageRecord::decode(&key, &value.bytes)?,
            StorageRecord::DeletedFile(_)
        ) {
            return Ok(None);
        }
        self.resolve_value(location, &value.bytes).await.map(Some)
    }

    async fn resolve_value(&self, location: &FileLocation, bytes: &[u8]) -> Result<FileRecord, CatalogError> {
        let key = location_key(location);
        let record = match StorageRecord::decode(&key, bytes)? {
            StorageRecord::File(record) => record,
            StorageRecord::FileMapping(mapping) => {
                let key = file_key(location.table().catalog, mapping.file);
                let value = self
                    .store
                    .get(&key.encode()?)
                    .await?
                    .ok_or(ValidationError::Record)?;
                let StorageRecord::File(record) = StorageRecord::decode(&key, &value.bytes)? else {
                    return Err(ValidationError::Record.into());
                };
                record
            }
            StorageRecord::DeletedFile(_) => return Err(CatalogError::Conflict),
            _ => return Err(ValidationError::Record.into()),
        };
        if record.location != *location {
            return Err(ValidationError::IdentityMismatch.into());
        }
        Ok(*record)
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
        || existing.content.etag() != candidate.content.etag()
    {
        return Err(CatalogError::Conflict);
    }
    Ok(existing)
}
