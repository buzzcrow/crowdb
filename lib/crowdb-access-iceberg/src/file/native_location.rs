use std::{fmt, str::FromStr};

use crate::{error::ValidationError, key::StorageTablePath};

use super::{location::validate_relative_key, MAX_OBJECT_KEY_BYTES};

pub const NATIVE_ICEBERG_BUCKET: &str = "iceberg";

/// A canonical SDK-visible object name in native Iceberg storage.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NativeFileLocation {
    table: StorageTablePath,
    relative_key: String,
}

impl StorageTablePath {
    #[must_use]
    pub fn uri(self) -> String {
        format!("s3://{NATIVE_ICEBERG_BUCKET}/{self}")
    }

    /// # Errors
    /// Rejects unsafe relative keys and object keys exceeding the SDK byte limit.
    pub fn file(self, relative_key: &str) -> Result<NativeFileLocation, ValidationError> {
        validate_native_relative_key(relative_key)?;
        Ok(NativeFileLocation {
            table: self,
            relative_key: relative_key.to_owned(),
        })
    }

    /// Admission for names chosen by ordinary SDK writers.
    ///
    /// # Errors
    /// Rejects invalid keys and the server-owned direct locator directory.
    pub fn logical_file(self, relative_key: &str) -> Result<NativeFileLocation, ValidationError> {
        if relative_key == "f" || relative_key.starts_with("f/") {
            return Err(ValidationError::Key);
        }
        self.file(relative_key)
    }
}

impl NativeFileLocation {
    /// # Errors
    /// Rejects foreign buckets, malformed table prefixes and unsafe filenames.
    pub fn from_object_key(bucket: &str, key: &str) -> Result<Self, ValidationError> {
        if bucket != NATIVE_ICEBERG_BUCKET {
            return Err(ValidationError::Key);
        }
        if key.len() > MAX_OBJECT_KEY_BYTES {
            return Err(ValidationError::KeyTooLarge);
        }
        let (table, relative) = StorageTablePath::split_object_key(key)?;
        table.file(relative)
    }

    #[must_use]
    pub const fn table(&self) -> StorageTablePath {
        self.table
    }

    #[must_use]
    pub fn relative_key(&self) -> &str {
        &self.relative_key
    }

    #[must_use]
    pub fn object_key(&self) -> String {
        format!("{}{}", self.table, self.relative_key)
    }
}

impl fmt::Display for NativeFileLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "s3://{NATIVE_ICEBERG_BUCKET}/{}{}",
            self.table, self.relative_key
        )
    }
}

impl FromStr for NativeFileLocation {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let body = value.strip_prefix("s3://").ok_or(ValidationError::Key)?;
        let (bucket, key) = body.split_once('/').ok_or(ValidationError::Key)?;
        Self::from_object_key(bucket, key)
    }
}

pub(crate) fn validate_native_relative_key(key: &str) -> Result<(), ValidationError> {
    if key.len() > MAX_OBJECT_KEY_BYTES - StorageTablePath::PREFIX_BYTES {
        return Err(ValidationError::KeyTooLarge);
    }
    validate_relative_key(key)
}
