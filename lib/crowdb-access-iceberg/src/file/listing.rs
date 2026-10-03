//! Table-scoped ordered listing of selected immutable file records.

use crowdb_chunk_kv_client::MultiScanRequest;
use crowdb_protocol::chunk_kv::ScanDirection;

use crate::error::ValidationError;
use crate::key::{CatalogScope, IcebergKey};

use super::{TableLocation, MAX_OBJECT_KEY_BYTES};

mod repository;
mod token;
pub use token::FileListTokens;

pub const MAX_LIST_SCAN_ITEMS: usize = 256;
pub const MAX_LIST_SCAN_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileListRequest {
    pub table: TableLocation,
    pub prefix: String,
    pub delimiter: Option<String>,
    pub encoding_url: bool,
    pub max_keys: u16,
    pub continuation_token: Option<String>,
    pub start_after: Option<String>,
}

impl FileListRequest {
    /// # Errors
    /// Rejects requests outside the explicit table prefix and unsupported bounds.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.validate_prefix(&self.prefix)?;
        if self.max_keys > 1_000
            || self.delimiter.as_deref().is_some_and(|value| value != "/")
            || self
                .continuation_token
                .as_ref()
                .is_some_and(|token| token.len() > 4_096)
            || (self.start_after.is_some() && self.continuation_token.is_some())
        {
            return Err(ValidationError::Key);
        }
        if let Some(after) = &self.start_after {
            self.validate_prefix(after)?;
        }
        Ok(())
    }

    fn validate_prefix(&self, value: &str) -> Result<(), ValidationError> {
        let relative = value
            .strip_prefix(&self.table.object_prefix())
            .ok_or(ValidationError::Key)?;
        if value.len() > MAX_OBJECT_KEY_BYTES {
            return Err(ValidationError::KeyTooLarge);
        }
        if !relative.is_empty() {
            super::validate_relative_key(relative)?;
        }
        Ok(())
    }

    fn bounds(&self) -> Result<(Vec<u8>, Vec<u8>), ValidationError> {
        self.validate()?;
        let relative = self
            .prefix
            .strip_prefix(&self.table.object_prefix())
            .ok_or(ValidationError::Key)?;
        let mut start = IcebergKey::catalog_range(self.table.catalog).start;
        start.push(CatalogScope::FileLocation as u8);
        start.extend_from_slice(self.table.table.as_bytes());
        start.extend_from_slice(relative.as_bytes());
        let end = successor(&start).ok_or(ValidationError::Key)?;
        Ok((start, end))
    }

    fn storage_key(&self, object_key: &str) -> Result<Vec<u8>, ValidationError> {
        let relative = object_key
            .strip_prefix(&self.table.object_prefix())
            .ok_or(ValidationError::Key)?;
        let mut key = IcebergKey::catalog_range(self.table.catalog).start;
        key.push(CatalogScope::FileLocation as u8);
        key.extend_from_slice(self.table.table.as_bytes());
        key.extend_from_slice(relative.as_bytes());
        Ok(key)
    }
}

#[derive(Clone, Debug)]
pub struct FileLocationScan {
    pub listing: FileListRequest,
    pub start: Vec<u8>,
}

impl FileLocationScan {
    /// # Errors
    /// Rejects a cursor outside the validated table/prefix interval.
    pub fn request(&self) -> Result<MultiScanRequest, ValidationError> {
        let (lower, end) = self.listing.bounds()?;
        if self.start < lower || self.start >= end {
            return Err(ValidationError::Key);
        }
        Ok(MultiScanRequest {
            start: Some(self.start.clone()),
            end: Some(end),
            direction: ScanDirection::Forward,
            max_items: MAX_LIST_SCAN_ITEMS,
            max_bytes: MAX_LIST_SCAN_BYTES,
            continuation: None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListedFile {
    pub key: String,
    pub length: u64,
    pub etag: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileListPage {
    pub files: Vec<ListedFile>,
    pub common_prefixes: Vec<String>,
    pub next_continuation_token: Option<String>,
}

fn successor(value: &[u8]) -> Option<Vec<u8>> {
    let mut upper = value.to_vec();
    while let Some(byte) = upper.pop() {
        if byte != u8::MAX {
            upper.push(byte + 1);
            return Some(upper);
        }
    }
    None
}
