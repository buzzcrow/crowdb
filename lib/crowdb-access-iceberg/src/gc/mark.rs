use crate::{
    catalog::CatalogError,
    error::ValidationError,
    file::{file_key, location_key, FileIoError, FileRecord},
    record::StorageRecord,
    table::TableMetadataError,
};

use super::GcRepository;

#[derive(Debug, thiserror::Error)]
pub enum GcMarkError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Invalid(#[from] ValidationError),
    #[error(transparent)]
    Io(#[from] FileIoError),
    #[error(transparent)]
    Metadata(#[from] TableMetadataError),
    #[error(transparent)]
    Avro(#[from] crate::file::AvroContainerError),
}

impl GcRepository {
    pub(super) async fn resolve_gc_file(
        &self,
        location: &crate::file::FileLocation,
    ) -> Result<FileRecord, CatalogError> {
        let key = location_key(location);
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::FileMapping(mapping) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        let key = file_key(location.table().catalog, mapping.file);
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::File(file) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if file.location != *location {
            return Err(ValidationError::IdentityMismatch.into());
        }
        Ok(*file)
    }
}
