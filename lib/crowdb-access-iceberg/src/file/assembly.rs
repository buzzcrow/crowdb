use std::sync::Arc;

use super::{
    FileBlockStore, FileIdentity, FileIoError, FileReader, FileTree, FileTreeWriter, FileWriterCheckpoint,
    MAX_READ_FRAME_BYTES,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartFingerprint {
    pub owner: FileIdentity,
    pub length: u64,
    pub digest: [u8; 32],
}

pub struct AssemblyPart {
    pub ordinal: u16,
    pub owner: FileIdentity,
    pub tree: FileTree,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssemblyProgress {
    pub selection: [u8; 32],
    pub next_part: u16,
    pub part_offset: u64,
    pub completed_bytes: u64,
    pub writer: Option<FileWriterCheckpoint>,
    pub active: Option<PartFingerprint>,
    pub part_digest: Option<Vec<u8>>,
}

pub struct FileAssembly {
    store: Arc<dyn FileBlockStore>,
    owner: FileIdentity,
    parts: u16,
    next_part: u16,
    max_file_bytes: u64,
    frame_bytes: usize,
    writer: FileTreeWriter,
    failed: bool,
}

impl FileAssembly {
    /// Creates request-local assembly with bounded read frames and writer buffers.
    /// Failed or cancelled requests discard the copier and restart from complete parts.
    /// # Errors
    /// Rejects invalid part counts, file limits and buffer bounds before any I/O.
    pub fn new(
        store: Arc<dyn FileBlockStore>,
        owner: FileIdentity,
        parts: u16,
        max_file_bytes: u64,
        frame_bytes: usize,
        block_bytes: usize,
    ) -> Result<Self, FileIoError> {
        if parts == 0
            || parts > 10_000
            || max_file_bytes == 0
            || max_file_bytes > u64::MAX / 8
            || frame_bytes == 0
            || frame_bytes > MAX_READ_FRAME_BYTES
        {
            return Err(FileIoError::Bounds);
        }
        let writer = FileTreeWriter::new(store.clone(), owner, block_bytes)?;
        Ok(Self {
            store,
            owner,
            parts,
            next_part: 0,
            max_file_bytes,
            frame_bytes,
            writer,
            failed: false,
        })
    }

    /// Copies one complete part while verifying its full digest, without saving progress.
    /// At most the current and next read frames overlap with one bounded writer.
    /// # Errors
    /// Rejects reordered or foreign parts, byte excess, corruption and storage failures.
    /// After failure this copier cannot be resumed or finalized.
    pub async fn push(&mut self, part: &AssemblyPart) -> Result<(), FileIoError> {
        if self.failed {
            return Err(FileIoError::Finished);
        }
        self.failed = true;
        let length = self
            .writer
            .length()
            .checked_add(part.tree.length)
            .ok_or(FileIoError::Bounds)?;
        if part.ordinal != self.next_part
            || part.ordinal >= self.parts
            || part.owner.table != self.owner.table
            || part.owner.file == self.owner.file
            || length > self.max_file_bytes
        {
            return Err(FileIoError::Bounds);
        }
        let mut reader = FileReader::from_tree(
            self.store.clone(),
            part.owner,
            part.tree.clone(),
            None,
            self.frame_bytes,
        )?;
        let mut pending = reader.next().await?;
        while let Some(bytes) = pending {
            let (next, ()) = tokio::try_join!(reader.next(), self.writer.push(&bytes))?;
            pending = next;
        }
        if self.writer.length() != length {
            return Err(FileIoError::Bounds);
        }
        self.next_part += 1;
        self.failed = false;
        Ok(())
    }

    /// Finishes the complete request; semantic sealing and final publication remain separate.
    /// # Errors
    /// Rejects incomplete selections and any earlier copy failure.
    pub async fn finish(self) -> Result<FileTree, FileIoError> {
        if self.failed || self.next_part != self.parts {
            return Err(FileIoError::Finished);
        }
        self.writer.finish().await
    }
}
