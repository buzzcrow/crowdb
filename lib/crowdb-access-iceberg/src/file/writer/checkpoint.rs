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
        checkpoint.root.validate()?;
        if checkpoint.root.height != 0 {
            return Err(crate::error::ValidationError::Record.into());
        }
        let bytes = store.read(&checkpoint.root).await?;
        verify_block(&checkpoint.root, &bytes)?;
        decode(owner, &bytes).map_err(|error| match error {
            FileIoError::Bounds => FileIoError::Invalid(crate::error::ValidationError::Record),
            error => error,
        })
    }
}

fn decode(owner: FileIdentity, mut bytes: &[u8]) -> Result<Vec<super::ChunkEntry>, FileIoError> {
    if take(&mut bytes, 5)? != b"ICFW\x01" {
        return Err(FileIoError::Bounds);
    }
    let digest_length = u16::from_be_bytes(take(&mut bytes, 2)?.try_into().map_err(|_| FileIoError::Bounds)?);
    let digest = FileDigest::restore(owner, take(&mut bytes, usize::from(digest_length))?)?;
    let mut roots = Vec::new();
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
        roots.splice(0..0, directory.entries);
    }
    if !bytes.is_empty() || covered != digest.length() {
        return Err(FileIoError::Bounds);
    }
    Ok(roots)
}

fn take<'a>(bytes: &mut &'a [u8], length: usize) -> Result<&'a [u8], FileIoError> {
    let result = bytes.get(..length).ok_or(FileIoError::Bounds)?;
    *bytes = &bytes[length..];
    Ok(result)
}
