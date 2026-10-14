use std::ops::Range;

use crate::{error::ValidationError, file::validate_native_relative_key};

use super::{storage_path::canonical_id, OperationId, StorageTablePath, MAX_KEY_BYTES};

const HEADER: &[u8] = b"ICE\0\x02";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableStorageScope {
    Head,
    File,
    Projection,
    Operation,
    Multipart,
    Gc,
}

impl TableStorageScope {
    const fn component(self) -> &'static str {
        match self {
            Self::Head => "head",
            Self::File => "file",
            Self::Projection => "projection",
            Self::Operation => "operation",
            Self::Multipart => "multipart",
            Self::Gc => "gc",
        }
    }

    fn parse(value: &str) -> Result<Self, ValidationError> {
        match value {
            "head" => Ok(Self::Head),
            "file" => Ok(Self::File),
            "projection" => Ok(Self::Projection),
            "operation" => Ok(Self::Operation),
            "multipart" => Ok(Self::Multipart),
            "gc" => Ok(Self::Gc),
            _ => Err(ValidationError::Key),
        }
    }
}

/// Version-isolated table keys; filenames are their only file authority suffix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableStorageKey {
    pub table: StorageTablePath,
    pub scope: TableStorageScope,
    pub suffix: String,
}

impl TableStorageKey {
    /// # Errors
    /// Rejects unsafe suffixes, invalid head keys and oversized keys.
    pub fn encode(&self) -> Result<Vec<u8>, ValidationError> {
        self.validate()?;
        let mut bytes = scope_prefix(self.table, self.scope);
        bytes.extend_from_slice(self.suffix.as_bytes());
        if bytes.len() > MAX_KEY_BYTES {
            return Err(ValidationError::KeyTooLarge);
        }
        Ok(bytes)
    }

    /// # Errors
    /// Rejects legacy keys, unknown scopes, aliases and invalid suffixes.
    pub fn decode(bytes: &[u8]) -> Result<Self, ValidationError> {
        if bytes.len() > MAX_KEY_BYTES {
            return Err(ValidationError::KeyTooLarge);
        }
        let payload = bytes.strip_prefix(HEADER).ok_or(ValidationError::Key)?;
        let payload = std::str::from_utf8(payload).map_err(|_| ValidationError::Key)?;
        let (table, relative) = StorageTablePath::split_object_key(payload)?;
        let (component, suffix) = relative.split_once('/').unwrap_or((relative, ""));
        let scope = TableStorageScope::parse(component)?;
        if (scope == TableStorageScope::Head) == relative.contains('/') {
            return Err(ValidationError::Key);
        }
        let key = Self {
            table,
            scope,
            suffix: suffix.to_owned(),
        };
        key.validate()?;
        Ok(key)
    }

    #[must_use]
    pub fn table_range(table: StorageTablePath) -> Range<Vec<u8>> {
        let mut start = HEADER.to_vec();
        start.extend_from_slice(table.to_string().as_bytes());
        prefix_range(start)
    }

    /// A bounded ordered file scan, including arbitrary public filename prefixes.
    ///
    /// # Errors
    /// Rejects escaping prefixes and prefixes exceeding the object-key byte limit.
    pub fn file_range(
        table: StorageTablePath,
        filename_prefix: &str,
    ) -> Result<Range<Vec<u8>>, ValidationError> {
        if !filename_prefix.is_empty() {
            validate_native_relative_key(filename_prefix)?;
        }
        let mut start = scope_prefix(table, TableStorageScope::File);
        start.extend_from_slice(filename_prefix.as_bytes());
        Ok(prefix_range(start))
    }

    fn validate(&self) -> Result<(), ValidationError> {
        match self.scope {
            TableStorageScope::Head if self.suffix.is_empty() => Ok(()),
            TableStorageScope::Head => Err(ValidationError::Key),
            TableStorageScope::File => validate_native_relative_key(&self.suffix),
            TableStorageScope::Projection => {
                crate::file::validate_relative_key(&self.suffix)?;
                let (generation, child) = self.suffix.split_once('/').ok_or(ValidationError::Key)?;
                let number = generation.parse::<u64>().map_err(|_| ValidationError::Key)?;
                if number == 0 || number.to_string() != generation || child.is_empty() {
                    return Err(ValidationError::Key);
                }
                Ok(())
            }
            TableStorageScope::Operation | TableStorageScope::Multipart | TableStorageScope::Gc => {
                crate::file::validate_relative_key(&self.suffix)?;
                let identity = self.suffix.split('/').next().ok_or(ValidationError::Key)?;
                canonical_id::<OperationId>(identity).map(|_| ())
            }
        }
    }
}

fn scope_prefix(table: StorageTablePath, scope: TableStorageScope) -> Vec<u8> {
    let mut bytes = HEADER.to_vec();
    bytes.extend_from_slice(table.to_string().as_bytes());
    bytes.extend_from_slice(scope.component().as_bytes());
    if scope != TableStorageScope::Head {
        bytes.push(b'/');
    }
    bytes
}

fn prefix_range(start: Vec<u8>) -> Range<Vec<u8>> {
    let mut end = start.clone();
    while let Some(byte) = end.pop() {
        if byte != u8::MAX {
            end.push(byte + 1);
            break;
        }
    }
    start..end
}
