use std::{fmt, str::FromStr};

use crate::error::ValidationError;

use super::{CatalogId, NamespaceId, TableId};

/// Immutable storage ownership, independent of a table's current name or namespace.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StorageTablePath {
    pub catalog: CatalogId,
    pub storage_namespace: NamespaceId,
    pub table: TableId,
}

impl StorageTablePath {
    pub const PREFIX_BYTES: usize = 99;

    /// # Errors
    /// Rejects aliases, noncanonical IDs and incomplete table prefixes.
    pub fn from_prefix(prefix: &str) -> Result<Self, ValidationError> {
        let (path, relative) = Self::split_object_key(prefix)?;
        if !relative.is_empty() {
            return Err(ValidationError::Key);
        }
        Ok(path)
    }

    /// Separates a complete immutable table prefix from its undecoded suffix.
    ///
    /// # Errors
    /// Rejects missing components, zero IDs and noncanonical identity encodings.
    pub fn split_object_key(key: &str) -> Result<(Self, &str), ValidationError> {
        let mut fields = key.splitn(4, '/');
        let catalog = fields.next().ok_or(ValidationError::Key)?;
        let namespace = fields.next().ok_or(ValidationError::Key)?;
        let table = fields.next().ok_or(ValidationError::Key)?;
        let relative = fields.next().ok_or(ValidationError::Key)?;
        let path = Self {
            catalog: canonical_id(catalog)?,
            storage_namespace: canonical_id(namespace)?,
            table: canonical_id(table)?,
        };
        Ok((path, relative))
    }
}

impl fmt::Display for StorageTablePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}/{}/{}/",
            self.catalog, self.storage_namespace, self.table
        )
    }
}

impl FromStr for StorageTablePath {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_prefix(value)
    }
}

pub(super) fn canonical_id<T: FromStr<Err = ValidationError>>(value: &str) -> Result<T, ValidationError> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ValidationError::Key);
    }
    value.parse()
}
