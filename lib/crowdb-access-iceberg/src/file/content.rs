use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::common::ChunkId;
use sha2::{Digest, Sha256};

use crate::error::ValidationError;

use super::FileKind;

pub const MAX_INLINE_BYTES: usize = 16 * 1024;
pub const MAX_COMPRESSION_INPUT_BYTES: usize = 64 * 1024;
pub const MAX_CHUNK_DIRECTORY_BYTES: u64 = 32 * 1024;
pub const MAX_CHUNK_TREE_HEIGHT: u8 = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InlineCodec {
    Raw,
    Lz4,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkRoot {
    pub chunk: ChunkId,
    pub offset: u64,
    pub physical_length: u64,
    pub logical_offset: u64,
    pub logical_length: u64,
    pub height: u8,
    pub digest: [u8; 32],
}

impl ChunkRoot {
    /// # Errors
    /// Rejects empty, overflowing, or unbounded directory references.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.chunk == ChunkId::default()
            || self.physical_length == 0
            || self.logical_length == 0
            || self.offset.checked_add(self.physical_length).is_none()
            || self.logical_offset.checked_add(self.logical_length).is_none()
            || self.height > MAX_CHUNK_TREE_HEIGHT
            || (self.height == 0 && self.logical_length > super::blocks::MAX_FILE_BLOCK_BYTES as u64)
            || (self.height > 0 && self.logical_length > MAX_CHUNK_DIRECTORY_BYTES)
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileContent {
    Inline { codec: InlineCodec, bytes: Vec<u8> },
    Chunks { root: Option<ChunkRoot> },
    Locations { bytes: Vec<u8>, etag: String },
}

impl FileContent {
    /// Encodes complete Chunk locations once, after the whole file is durable.
    /// # Errors
    /// Rejects gaps, overlapping locations and metadata that cannot fit one record.
    pub fn from_locations(
        locations: &[Location],
        length: u64,
        etag: String,
    ) -> Result<Self, ValidationError> {
        validate_locations(locations, length)?;
        validate_etag(&etag)?;
        let bytes = bincode::serialize(locations).map_err(|_| ValidationError::Record)?;
        if bytes.len() > crate::record::MAX_RECORD_BYTES - 4096 {
            return Err(ValidationError::RecordTooLarge);
        }
        Ok(Self::Locations { bytes, etag })
    }

    /// # Errors
    /// Rejects malformed location encodings and inconsistent logical ranges.
    pub fn locations(&self, length: u64) -> Result<Option<Vec<Location>>, ValidationError> {
        let Self::Locations { bytes, etag } = self else {
            return Ok(None);
        };
        validate_etag(etag)?;
        if bytes.len() < 8 || bytes.len() > crate::record::MAX_RECORD_BYTES - 4096 {
            return Err(ValidationError::RecordTooLarge);
        }
        let count = u64::from_le_bytes(bytes[..8].try_into().map_err(|_| ValidationError::Record)?);
        if count > 1250 {
            return Err(ValidationError::RecordTooLarge);
        }
        let locations: Vec<Location> = bincode::deserialize(bytes).map_err(|_| ValidationError::Record)?;
        validate_locations(&locations, length)?;
        Ok(Some(locations))
    }

    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        match self {
            Self::Locations { etag, .. } => Some(etag),
            _ => None,
        }
    }

    pub(crate) fn validate(&self, length: u64, digest: &[u8; 32]) -> Result<(), ValidationError> {
        match self {
            Self::Inline { .. } => {
                self.inline_bytes(length, digest)?;
            }
            Self::Chunks { root: None } if length == 0 && *digest == <[u8; 32]>::from(Sha256::digest([])) => {
            }
            Self::Chunks { root: None } => return Err(ValidationError::Record),
            Self::Chunks { root: Some(root) } => {
                root.validate()?;
                if length == 0
                    || (root.height == 0 && (root.logical_length != length || root.digest != *digest))
                {
                    return Err(ValidationError::Record);
                }
            }
            Self::Locations { .. } => {
                if *digest != [0; 32] {
                    return Err(ValidationError::Record);
                }
                self.locations(length)?;
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn select_inline(kind: FileKind, input: &[u8]) -> Option<Self> {
        if !kind.allows_inline() || input.len() > MAX_COMPRESSION_INPUT_BYTES {
            return None;
        }
        if input.len() <= MAX_INLINE_BYTES {
            return Some(Self::Inline {
                codec: InlineCodec::Raw,
                bytes: input.to_vec(),
            });
        }
        let bytes = lz4_flex::block::compress(input);
        (bytes.len() <= MAX_INLINE_BYTES).then_some(Self::Inline {
            codec: InlineCodec::Lz4,
            bytes,
        })
    }

    /// # Errors
    /// Rejects invalid lengths, codecs, decompression and canonical-byte digests.
    pub fn inline_bytes(&self, length: u64, digest: &[u8; 32]) -> Result<Option<Vec<u8>>, ValidationError> {
        let Self::Inline { codec, bytes } = self else {
            return Ok(None);
        };
        if bytes.len() > MAX_INLINE_BYTES || length > MAX_COMPRESSION_INPUT_BYTES as u64 {
            return Err(ValidationError::RecordTooLarge);
        }
        let length = usize::try_from(length).map_err(|_| ValidationError::RecordTooLarge)?;
        let decoded = match codec {
            InlineCodec::Raw if bytes.len() == length => bytes.clone(),
            InlineCodec::Raw => return Err(ValidationError::Record),
            InlineCodec::Lz4 => {
                let mut output = vec![0; length];
                let written = lz4_flex::block::decompress_into(bytes, &mut output)
                    .map_err(|_| ValidationError::Record)?;
                if written != length {
                    return Err(ValidationError::Record);
                }
                output
            }
        };
        if <[u8; 32]>::from(Sha256::digest(&decoded)) != *digest {
            return Err(ValidationError::Record);
        }
        Ok(Some(decoded))
    }
}

fn validate_locations(locations: &[Location], length: u64) -> Result<(), ValidationError> {
    let mut cursor = 0;
    for location in locations {
        if location.chunk_id.is_none()
            || location.length == 0
            || location.logical_length == 0
            || location.logical_offset != cursor
            || location.offset.checked_add(location.length).is_none()
        {
            return Err(ValidationError::Record);
        }
        cursor = cursor
            .checked_add(location.logical_length)
            .ok_or(ValidationError::Record)?;
    }
    if cursor != length {
        return Err(ValidationError::Record);
    }
    Ok(())
}

fn validate_etag(etag: &str) -> Result<(), ValidationError> {
    let (digest, count) = etag
        .split_once('-')
        .map_or((etag, None), |(digest, count)| (digest, Some(count)));
    if digest.len() != 32
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || count.is_some_and(|count| {
            count.is_empty() || count.starts_with('0') || count.parse::<u16>().map_or(true, |n| n == 0)
        })
    {
        return Err(ValidationError::Record);
    }
    Ok(())
}
