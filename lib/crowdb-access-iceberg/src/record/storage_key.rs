use crate::{
    error::ValidationError,
    key::{IcebergKey, StorageKey},
};

use super::StorageRecord;

/// Only versioned Iceberg storage keys can bind a decoded authority record.
pub trait StorageRecordKey: sealed::Sealed {
    /// # Errors
    /// Rejects a record belonging to another key, storage owner or layout.
    fn validate_record(&self, record: &StorageRecord) -> Result<(), ValidationError>;
}

impl StorageRecordKey for IcebergKey {
    fn validate_record(&self, record: &StorageRecord) -> Result<(), ValidationError> {
        record.validate_key(self)
    }
}

impl StorageRecordKey for StorageKey {
    fn validate_record(&self, record: &StorageRecord) -> Result<(), ValidationError> {
        record.validate_storage_key(self)
    }
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for crate::key::IcebergKey {}
    impl Sealed for crate::key::StorageKey {}
}
