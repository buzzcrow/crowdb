use crate::{
    error::ValidationError,
    key::{StorageKey, TableStorageKey, TableStorageScope},
    operation::{RequestIdentity, RETRY_WINDOW_MS},
};

use super::{DirectFileLocations, FileRecord, TableLocation};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeInjection {
    pub identity: RequestIdentity,
    pub activation_epoch: u64,
    pub principal: [u8; 32],
    pub stem: String,
    pub file: FileRecord,
}

impl NativeInjection {
    /// # Errors
    /// Rejects legacy ownership, unsupported content and unbounded retention.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.file.validate()?;
        DirectFileLocations::validate_stem(&self.stem)?;
        if self.activation_epoch == 0
            || self.file.location.table().storage_namespace.is_none()
            || self.stem.contains('/')
            || !self
                .file
                .location
                .relative_key()
                .starts_with(&format!("f/v1/{}.", self.stem))
        {
            return Err(ValidationError::Record);
        }
        self.identity
            .issued_ms
            .checked_add(RETRY_WINDOW_MS)
            .ok_or(ValidationError::Deadline)?;
        Ok(())
    }

    /// # Errors
    /// Rejects legacy storage identities.
    pub fn key(&self) -> Result<StorageKey, ValidationError> {
        injection_key(self.file.location.table(), self.identity)
    }

    #[must_use]
    pub fn retained(&self, now_ms: u64) -> bool {
        now_ms <= self.identity.issued_ms.saturating_add(RETRY_WINDOW_MS)
    }
}

fn injection_key(table: TableLocation, identity: RequestIdentity) -> Result<StorageKey, ValidationError> {
    Ok(StorageKey::NativeTable(TableStorageKey {
        table: table.storage_path().ok_or(ValidationError::Key)?,
        scope: TableStorageScope::Operation,
        suffix: format!("{}/injection", identity.operation),
    }))
}
