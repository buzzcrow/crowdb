use crowdb_protocol::common::ChunkId;

use crate::error::ValidationError;
use crate::key::{CatalogId, FileId, TableId};

use super::content::{MAX_CHUNK_DIRECTORY_BYTES, MAX_CHUNK_TREE_HEIGHT};
use super::{ChunkRoot, TableLocation};

pub const MAX_DIRECTORY_ENTRIES: usize = 256;
const HEADER_BYTES: usize = 56;
const ENTRY_BYTES: usize = 89;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileIdentity {
    pub table: TableLocation,
    pub file: FileId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkEntry {
    pub length: u64,
    pub root: ChunkRoot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkDirectory {
    pub owner: FileIdentity,
    pub height: u8,
    pub entries: Vec<ChunkEntry>,
}

impl ChunkDirectory {
    /// # Errors
    /// Rejects unbounded fanout, invalid child heights and overflowing file spans.
    pub fn length(&self) -> Result<u64, ValidationError> {
        if self.height == 0
            || self.height > MAX_CHUNK_TREE_HEIGHT
            || self.entries.is_empty()
            || self.entries.len() > MAX_DIRECTORY_ENTRIES
        {
            return Err(ValidationError::Record);
        }
        let mut length = 0_u64;
        for entry in &self.entries {
            entry.root.validate()?;
            if entry.length == 0
                || entry.root.height + 1 != self.height
                || (entry.root.height == 0 && entry.root.logical_length != entry.length)
            {
                return Err(ValidationError::Record);
            }
            length = length.checked_add(entry.length).ok_or(ValidationError::Record)?;
        }
        Ok(length)
    }

    /// # Errors
    /// Rejects invalid directories before allocating their encoded representation.
    pub fn encode(&self) -> Result<Vec<u8>, ValidationError> {
        self.length()?;
        let mut bytes = Vec::with_capacity(HEADER_BYTES + ENTRY_BYTES * self.entries.len());
        bytes.extend_from_slice(b"ICEN\x01");
        bytes.extend_from_slice(self.owner.table.catalog.as_bytes());
        bytes.extend_from_slice(self.owner.table.table.as_bytes());
        bytes.extend_from_slice(self.owner.file.as_bytes());
        bytes.push(self.height);
        let count = u16::try_from(self.entries.len()).map_err(|_| ValidationError::Record)?;
        bytes.extend_from_slice(&count.to_be_bytes());
        for entry in &self.entries {
            bytes.extend_from_slice(&entry.length.to_be_bytes());
            for field in [
                entry.root.chunk.high,
                entry.root.chunk.low,
                entry.root.offset,
                entry.root.physical_length,
                entry.root.logical_offset,
                entry.root.logical_length,
            ] {
                bytes.extend_from_slice(&field.to_be_bytes());
            }
            bytes.push(entry.root.height);
            bytes.extend_from_slice(&entry.root.digest);
        }
        Ok(bytes)
    }

    /// # Errors
    /// Rejects wrong owner/span, malformed versions, oversized pages and invalid children.
    pub fn decode(
        bytes: &[u8],
        owner: FileIdentity,
        height: u8,
        length: u64,
    ) -> Result<Self, ValidationError> {
        if bytes.len() < HEADER_BYTES || bytes.len() as u64 > MAX_CHUNK_DIRECTORY_BYTES {
            return Err(ValidationError::RecordTooLarge);
        }
        let mut cursor = Cursor(bytes);
        if cursor.take::<5>()? != *b"ICEN\x01" {
            return Err(ValidationError::Record);
        }
        let actual_owner = FileIdentity {
            table: TableLocation {
                catalog: CatalogId::from_bytes(&cursor.take::<16>()?)?,
                table: TableId::from_bytes(&cursor.take::<16>()?)?,
            },
            file: FileId::from_bytes(&cursor.take::<16>()?)?,
        };
        if actual_owner != owner || cursor.take::<1>()?[0] != height {
            return Err(ValidationError::IdentityMismatch);
        }
        let count = usize::from(u16::from_be_bytes(cursor.take()?));
        if count > MAX_DIRECTORY_ENTRIES || cursor.0.len() != count * ENTRY_BYTES {
            return Err(ValidationError::Record);
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(ChunkEntry {
                length: cursor.number()?,
                root: ChunkRoot {
                    chunk: ChunkId {
                        high: cursor.number()?,
                        low: cursor.number()?,
                    },
                    offset: cursor.number()?,
                    physical_length: cursor.number()?,
                    logical_offset: cursor.number()?,
                    logical_length: cursor.number()?,
                    height: cursor.take::<1>()?[0],
                    digest: cursor.take()?,
                },
            });
        }
        let directory = Self {
            owner,
            height,
            entries,
        };
        if directory.length()? != length {
            return Err(ValidationError::Record);
        }
        Ok(directory)
    }
}

struct Cursor<'bytes>(&'bytes [u8]);

impl Cursor<'_> {
    fn take<const COUNT: usize>(&mut self) -> Result<[u8; COUNT], ValidationError> {
        let bytes = self.0.get(..COUNT).ok_or(ValidationError::Record)?;
        let result = bytes.try_into().map_err(|_| ValidationError::Record)?;
        self.0 = &self.0[COUNT..];
        Ok(result)
    }

    fn number(&mut self) -> Result<u64, ValidationError> {
        Ok(u64::from_be_bytes(self.take()?))
    }
}
