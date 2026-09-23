use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{ChunkIoClient, ChunkIoWriter};
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::frame::MAX_FRAME_PAYLOAD_BYTES;
use sha2::{Digest, Sha256};

use crate::error::ValidationError;

use super::{ChunkRoot, FileIdentity};

pub const MAX_FILE_BLOCK_BYTES: usize = 256 * 1024;
pub const NATIVE_FILE_BLOCK_BYTES: usize = MAX_FRAME_PAYLOAD_BYTES;

#[derive(Debug, thiserror::Error)]
pub enum FileIoError {
    #[error(transparent)]
    Invalid(#[from] ValidationError),
    #[error(transparent)]
    Write(#[from] crowdb_chunk_client::IoError),
    #[error(transparent)]
    Read(#[from] crowdb_chunk_client::ReadError),
    #[error("file IO bounds exceeded")]
    Bounds,
    #[error("file writer or reader has already failed or finished")]
    Finished,
}

#[async_trait]
pub trait FileBlockStore: Send + Sync {
    async fn put(&self, owner: FileIdentity, height: u8, bytes: &[u8]) -> Result<ChunkRoot, FileIoError>;
    async fn read(&self, root: &ChunkRoot) -> Result<Vec<u8>, FileIoError>;
}

#[derive(Clone)]
pub struct NativeFileBlocks {
    client: ChunkIoClient,
}

impl NativeFileBlocks {
    #[must_use]
    pub fn new(client: ChunkIoClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl FileBlockStore for NativeFileBlocks {
    async fn put(&self, owner: FileIdentity, height: u8, bytes: &[u8]) -> Result<ChunkRoot, FileIoError> {
        if bytes.is_empty()
            || bytes.len() > NATIVE_FILE_BLOCK_BYTES
            || height > super::content::MAX_CHUNK_TREE_HEIGHT
            || (height > 0 && bytes.len() as u64 > super::content::MAX_CHUNK_DIRECTORY_BYTES)
        {
            return Err(FileIoError::Bounds);
        }
        let mut key = b"iceberg-file-block-v1".to_vec();
        key.extend_from_slice(owner.table.catalog.as_bytes());
        key.extend_from_slice(owner.table.table.as_bytes());
        key.extend_from_slice(owner.file.as_bytes());
        let mut writer = self.client.prepare_small_write_for_key(bytes.len(), &key).await?;
        writer.on_data(Bytes::copy_from_slice(bytes)).await?;
        let locations = writer.finish_durable().await?;
        let [location] = locations.as_slice() else {
            return Err(ValidationError::Record.into());
        };
        let root = ChunkRoot {
            chunk: location.chunk_id.ok_or(ValidationError::Record)?,
            offset: location.offset,
            physical_length: location.length,
            logical_offset: location.logical_offset,
            logical_length: location.logical_length,
            height,
            digest: Sha256::digest(bytes).into(),
        };
        verify_block(&root, bytes)?;
        Ok(root)
    }

    async fn read(&self, root: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        root.validate()?;
        if root.logical_length > MAX_FILE_BLOCK_BYTES as u64 {
            return Err(FileIoError::Bounds);
        }
        let location = Location {
            chunk_id: Some(root.chunk),
            offset: root.offset,
            length: root.physical_length,
            logical_offset: root.logical_offset,
            logical_length: root.logical_length,
        };
        let bytes = self.client.read_object(&[location]).await?;
        verify_block(root, &bytes)?;
        Ok(bytes.to_vec())
    }
}

pub(super) fn verify_block(root: &ChunkRoot, bytes: &[u8]) -> Result<(), FileIoError> {
    root.validate()?;
    if bytes.len() > MAX_FILE_BLOCK_BYTES
        || bytes.len() as u64 != root.logical_length
        || <[u8; 32]>::from(Sha256::digest(bytes)) != root.digest
    {
        return Err(ValidationError::Record.into());
    }
    Ok(())
}
