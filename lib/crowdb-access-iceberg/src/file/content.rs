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
}

impl FileContent {
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
