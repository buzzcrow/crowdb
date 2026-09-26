use std::sync::Arc;

use super::{
    verify_block, ChunkDirectory, ChunkRoot, FileBlockStore, FileDigest, FileIdentity, FileIoError,
    FileTreeWriter, MAX_CHUNK_TREE_HEIGHT, MAX_DIRECTORY_ENTRIES,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileWriterCheckpoint {
    pub root: ChunkRoot,
}

impl FileTreeWriter {
    pub(crate) async fn checkpoint_roots(
        store: Arc<dyn FileBlockStore>,
        owner: FileIdentity,
        checkpoint: &FileWriterCheckpoint,
    ) -> Result<Vec<super::ChunkEntry>, FileIoError> {
        let writer = Self::restore(store, owner, 1, checkpoint)
            .await
            .map_err(|error| match error {
                FileIoError::Bounds => FileIoError::Invalid(crate::error::ValidationError::Record),
                error => error,
            })?;
        Ok(writer.levels.into_iter().rev().flatten().collect())
    }

    /// Flushes pending bytes and persists the bounded frontier in a chunk block.
    /// Only the returned fixed-size root belongs in a durable operation record.
    /// # Errors
    /// Poisons the writer on uncertain writes; previously persisted checkpoints survive.
    pub async fn checkpoint(&mut self) -> Result<FileWriterCheckpoint, FileIoError> {
        if self.failed {
            return Err(FileIoError::Finished);
        }
        self.failed = true;
        if !self.pending.is_empty() {
            self.flush_leaf().await?;
        }
        let bytes = encode(self)?;
        let root = self.store.put(self.owner, 0, &bytes).await?;
        verify_block(&root, &bytes)?;
        if root.height != 0 {
            return Err(FileIoError::Bounds);
        }
        self.failed = false;
        Ok(FileWriterCheckpoint { root })
    }

    /// Restores a writer on another instance from its chunk-resident frontier.
    /// # Errors
    /// Rejects wrong identities, corrupted state, invalid heights and byte coverage.
    pub async fn restore(
        store: Arc<dyn FileBlockStore>,
        owner: FileIdentity,
        block_bytes: usize,
        checkpoint: &FileWriterCheckpoint,
    ) -> Result<Self, FileIoError> {
        checkpoint.root.validate()?;
        if checkpoint.root.height != 0 {
            return Err(FileIoError::Bounds);
        }
        let bytes = store.read(&checkpoint.root).await?;
        verify_block(&checkpoint.root, &bytes)?;
        decode(store, owner, block_bytes, &bytes)
    }
}

fn encode(writer: &FileTreeWriter) -> Result<Vec<u8>, FileIoError> {
    let mut bytes = b"ICFW\x01".to_vec();
    let digest = writer.digest.checkpoint();
    bytes.extend_from_slice(
        &u16::try_from(digest.len())
            .map_err(|_| FileIoError::Bounds)?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&digest);
    for (level, entries) in writer.levels.iter().enumerate() {
        if entries.len() >= MAX_DIRECTORY_ENTRIES {
            return Err(FileIoError::Bounds);
        }
        let encoded = if entries.is_empty() {
            Vec::new()
        } else {
            ChunkDirectory {
                owner: writer.owner,
                height: u8::try_from(level + 1).map_err(|_| FileIoError::Bounds)?,
                entries: entries.clone(),
            }
            .encode_frontier()?
        };
        bytes.extend_from_slice(
            &u32::try_from(encoded.len())
                .map_err(|_| FileIoError::Bounds)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&encoded);
    }
    if bytes.len() > super::MAX_FILE_BLOCK_BYTES {
        return Err(FileIoError::Bounds);
    }
    Ok(bytes)
}

fn decode(
    store: Arc<dyn FileBlockStore>,
    owner: FileIdentity,
    block_bytes: usize,
    mut bytes: &[u8],
) -> Result<FileTreeWriter, FileIoError> {
    if take(&mut bytes, 5)? != b"ICFW\x01" {
        return Err(FileIoError::Bounds);
    }
    let digest_length = u16::from_be_bytes(take(&mut bytes, 2)?.try_into().map_err(|_| FileIoError::Bounds)?);
    let digest = FileDigest::restore(owner, take(&mut bytes, usize::from(digest_length))?)?;
    let mut writer = FileTreeWriter::new(store, owner, block_bytes)?;
    writer.length = digest.length();
    writer.digest = digest;
    let mut covered = 0_u64;
    for level in 0..=MAX_CHUNK_TREE_HEIGHT {
        let length = u32::from_be_bytes(take(&mut bytes, 4)?.try_into().map_err(|_| FileIoError::Bounds)?);
        let encoded = take(
            &mut bytes,
            usize::try_from(length).map_err(|_| FileIoError::Bounds)?,
        )?;
        if encoded.is_empty() {
            continue;
        }
        let directory = ChunkDirectory::decode_frontier(encoded, owner, level + 1)?;
        covered = covered
            .checked_add(directory.frontier_length()?)
            .ok_or(FileIoError::Bounds)?;
        if directory.entries.len() >= MAX_DIRECTORY_ENTRIES {
            return Err(FileIoError::Bounds);
        }
        writer.levels[usize::from(level)] = directory.entries;
    }
    if !bytes.is_empty() || covered != writer.length {
        return Err(FileIoError::Bounds);
    }
    Ok(writer)
}

fn take<'a>(bytes: &mut &'a [u8], length: usize) -> Result<&'a [u8], FileIoError> {
    let result = bytes.get(..length).ok_or(FileIoError::Bounds)?;
    *bytes = &bytes[length..];
    Ok(result)
}
