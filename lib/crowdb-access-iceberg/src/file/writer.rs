use std::sync::Arc;

use super::blocks::verify_block;
use super::content::MAX_CHUNK_TREE_HEIGHT;
use super::{
    ChunkDirectory, ChunkEntry, ChunkRoot, FileBlockStore, FileDigest, FileIdentity, FileIoError,
    MAX_DIRECTORY_ENTRIES, MAX_FILE_BLOCK_BYTES,
};

mod checkpoint;
pub use checkpoint::FileWriterCheckpoint;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileTree {
    pub root: Option<ChunkRoot>,
    pub length: u64,
    pub digest: [u8; 32],
}

pub struct FileTreeWriter {
    store: Arc<dyn FileBlockStore>,
    owner: FileIdentity,
    block_bytes: usize,
    pending: Vec<u8>,
    levels: Vec<Vec<ChunkEntry>>,
    length: u64,
    digest: FileDigest,
    failed: bool,
}

impl FileTreeWriter {
    /// # Errors
    /// Rejects unbounded leaf buffers before retaining any input.
    pub fn new(
        store: Arc<dyn FileBlockStore>,
        owner: FileIdentity,
        block_bytes: usize,
    ) -> Result<Self, FileIoError> {
        if block_bytes == 0 || block_bytes > MAX_FILE_BLOCK_BYTES {
            return Err(FileIoError::Bounds);
        }
        Ok(Self {
            store,
            owner,
            block_bytes,
            pending: Vec::with_capacity(block_bytes),
            levels: vec![Vec::new(); usize::from(MAX_CHUNK_TREE_HEIGHT) + 1],
            length: 0,
            digest: FileDigest::new(owner),
            failed: false,
        })
    }

    #[must_use]
    pub const fn length(&self) -> u64 {
        self.length
    }

    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.pending.capacity()
            + self
                .levels
                .iter()
                .map(|level| level.capacity() * std::mem::size_of::<ChunkEntry>())
                .sum::<usize>()
    }

    /// # Errors
    /// Stops permanently on uncertain storage failure; never deletes accepted blocks.
    pub async fn push(&mut self, mut bytes: &[u8]) -> Result<(), FileIoError> {
        if self.failed {
            return Err(FileIoError::Finished);
        }
        self.failed = true;
        self.length = self
            .length
            .checked_add(bytes.len() as u64)
            .ok_or(FileIoError::Bounds)?;
        self.digest.update(bytes)?;
        while !bytes.is_empty() {
            let count = bytes.len().min(self.block_bytes - self.pending.len());
            self.pending.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if self.pending.len() == self.block_bytes {
                self.flush_leaf().await?;
            }
        }
        self.failed = false;
        Ok(())
    }

    /// # Errors
    /// Propagates storage and tree-height limits without publishing partial files.
    pub async fn finish(mut self) -> Result<FileTree, FileIoError> {
        if self.failed {
            return Err(FileIoError::Finished);
        }
        if !self.pending.is_empty() {
            self.flush_leaf().await?;
        }
        for level in 0..self.levels.len() {
            if self.levels[level].is_empty() {
                continue;
            }
            if self.levels[level].len() == 1 && self.levels[level + 1..].iter().all(Vec::is_empty) {
                let entry = self.levels[level].pop().ok_or(FileIoError::Bounds)?;
                if entry.length != self.length {
                    return Err(FileIoError::Bounds);
                }
                return Ok(FileTree {
                    root: Some(entry.root),
                    length: self.length,
                    digest: self.digest.finish(),
                });
            }
            let entry = self.flush_directory(level).await?;
            self.append(entry).await?;
        }
        if self.length != 0 {
            return Err(FileIoError::Bounds);
        }
        Ok(FileTree {
            root: None,
            length: 0,
            digest: self.digest.finish(),
        })
    }

    async fn flush_leaf(&mut self) -> Result<(), FileIoError> {
        let root = self.store.put(self.owner, 0, &self.pending).await?;
        verify_block(&root, &self.pending)?;
        if root.height != 0 {
            return Err(FileIoError::Bounds);
        }
        let entry = ChunkEntry {
            length: self.pending.len() as u64,
            root,
        };
        self.pending.clear();
        self.append(entry).await
    }

    async fn append(&mut self, mut entry: ChunkEntry) -> Result<(), FileIoError> {
        loop {
            let level = usize::from(entry.root.height);
            let entries = self.levels.get_mut(level).ok_or(FileIoError::Bounds)?;
            entries.push(entry);
            if entries.len() < MAX_DIRECTORY_ENTRIES {
                return Ok(());
            }
            entry = self.flush_directory(level).await?;
        }
    }

    async fn flush_directory(&mut self, level: usize) -> Result<ChunkEntry, FileIoError> {
        let height = u8::try_from(level + 1).map_err(|_| FileIoError::Bounds)?;
        if height > MAX_CHUNK_TREE_HEIGHT {
            return Err(FileIoError::Bounds);
        }
        let directory = ChunkDirectory {
            owner: self.owner,
            height,
            entries: std::mem::take(&mut self.levels[level]),
        };
        let length = directory.length()?;
        let bytes = directory.encode()?;
        let root = self.store.put(self.owner, height, &bytes).await?;
        verify_block(&root, &bytes)?;
        if root.height != height {
            return Err(FileIoError::Bounds);
        }
        Ok(ChunkEntry { length, root })
    }
}
