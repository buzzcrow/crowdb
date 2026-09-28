use std::fmt;
use std::str::FromStr;

use data_encoding::BASE32_NOPAD;

use crate::error::ValidationError;
use crate::key::{CatalogId, TableId};

pub const MAX_OBJECT_KEY_BYTES: usize = 1024;
const TABLE_PREFIX_BYTES: usize = 35;
const BUCKET_BYTES: usize = 34;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TableLocation {
    pub catalog: CatalogId,
    pub table: TableId,
}

impl TableLocation {
    #[must_use]
    pub fn bucket(self) -> String {
        format!(
            "iceberg-{}",
            BASE32_NOPAD.encode(self.catalog.as_bytes()).to_ascii_lowercase()
        )
    }

    #[must_use]
    pub fn object_prefix(self) -> String {
        format!("t/{}/", self.table)
    }

    /// # Errors
    /// Rejects keys outside this table or keys that cannot be represented exactly.
    pub fn file(self, relative_key: &str) -> Result<FileLocation, ValidationError> {
        validate_relative_key(relative_key)?;
        Ok(FileLocation {
            table: self,
            relative_key: relative_key.to_owned(),
        })
    }
}

impl fmt::Display for TableLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "s3://{}/{}", self.bucket(), self.object_prefix())
    }
}

impl FromStr for TableLocation {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let body = value.strip_prefix("s3://").ok_or(ValidationError::Key)?;
        let (bucket, prefix) = body.split_once('/').ok_or(ValidationError::Key)?;
        if prefix.len() != TABLE_PREFIX_BYTES || !prefix.ends_with('/') {
            return Err(ValidationError::Key);
        }
        parse_table(bucket, &prefix[..prefix.len() - 1])
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FileLocation {
    table: TableLocation,
    relative_key: String,
}

impl FileLocation {
    /// # Errors
    /// Rejects noncanonical identity components and escaping or oversized keys.
    pub fn from_object_key(bucket: &str, key: &str) -> Result<Self, ValidationError> {
        if key.len() > MAX_OBJECT_KEY_BYTES {
            return Err(ValidationError::KeyTooLarge);
        }
        let rest = key.strip_prefix("t/").ok_or(ValidationError::Key)?;
        let (table, relative) = rest.split_once('/').ok_or(ValidationError::Key)?;
        let table = parse_table(bucket, &format!("t/{table}"))?;
        table.file(relative)
    }

    #[must_use]
    pub const fn table(&self) -> TableLocation {
        self.table
    }

    #[must_use]
    pub fn relative_key(&self) -> &str {
        &self.relative_key
    }

    #[must_use]
    pub fn object_key(&self) -> String {
        format!("{}{}", self.table.object_prefix(), self.relative_key)
    }
}

impl fmt::Display for FileLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}{}", self.table, self.relative_key)
    }
}

impl FromStr for FileLocation {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > 5 + BUCKET_BYTES + 1 + MAX_OBJECT_KEY_BYTES {
            return Err(ValidationError::KeyTooLarge);
        }
        let body = value.strip_prefix("s3://").ok_or(ValidationError::Key)?;
        let (bucket, key) = body.split_once('/').ok_or(ValidationError::Key)?;
        Self::from_object_key(bucket, key)
    }
}

fn parse_table(bucket: &str, prefix: &str) -> Result<TableLocation, ValidationError> {
    if bucket.len() != BUCKET_BYTES || prefix.len() != TABLE_PREFIX_BYTES - 1 {
        return Err(ValidationError::Key);
    }
    let encoded = bucket.strip_prefix("iceberg-").ok_or(ValidationError::Key)?;
    let bytes = BASE32_NOPAD
        .decode(encoded.to_ascii_uppercase().as_bytes())
        .map_err(|_| ValidationError::Key)?;
    let catalog = CatalogId::from_bytes(&bytes)?;
    let table = prefix.strip_prefix("t/").ok_or(ValidationError::Key)?.parse()?;
    let location = TableLocation { catalog, table };
    if location.bucket() != bucket || format!("t/{table}") != prefix {
        return Err(ValidationError::Key);
    }
    Ok(location)
}

pub(crate) fn validate_relative_key(key: &str) -> Result<(), ValidationError> {
    if key.len() > MAX_OBJECT_KEY_BYTES - TABLE_PREFIX_BYTES {
        return Err(ValidationError::KeyTooLarge);
    }
    if key.is_empty()
        || key.starts_with('/')
        || key
            .chars()
            .any(|character| character.is_control() || matches!(character, '\\' | '?' | '#'))
        || key.split('/').any(|segment| matches!(segment, "." | ".."))
    {
        return Err(ValidationError::Key);
    }
    Ok(())
}
