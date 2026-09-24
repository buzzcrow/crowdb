use std::sync::Arc;

use super::{
    ByteRange, FileBlockStore, FileDigest, FileIdentity, FileIoError, FileReader, FileTree, FileTreeWriter,
    FileWriterCheckpoint, MAX_FILE_BLOCK_BYTES,
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
    selection: [u8; 32],
    parts: u16,
    max_file_bytes: u64,
    step_bytes: usize,
    block_bytes: usize,
}

impl FileAssembly {
    /// Builds a bounded copier for a selection already frozen by a durable journal.
    /// The journal must supply the selected part at each ordinal and CAS progress;
    /// this byte engine neither authorizes uploads nor publishes file locations.
    /// # Errors
    /// Rejects invalid part counts, file limits and per-step memory/work bounds.
    pub fn new(
        store: Arc<dyn FileBlockStore>,
        owner: FileIdentity,
        selection: [u8; 32],
        parts: u16,
        max_file_bytes: u64,
        step_bytes: usize,
        block_bytes: usize,
    ) -> Result<Self, FileIoError> {
        if parts == 0
            || parts > 10_000
            || max_file_bytes == 0
            || max_file_bytes > u64::MAX / 8
            || step_bytes == 0
            || step_bytes > 1024 * 1024
            || block_bytes == 0
            || block_bytes > MAX_FILE_BLOCK_BYTES
        {
            return Err(FileIoError::Bounds);
        }
        Ok(Self {
            store,
            owner,
            selection,
            parts,
            max_file_bytes,
            step_bytes,
            block_bytes,
        })
    }

    #[must_use]
    pub fn begin(&self) -> AssemblyProgress {
        AssemblyProgress {
            selection: self.selection,
            next_part: 0,
            part_offset: 0,
            completed_bytes: 0,
            writer: None,
            active: None,
            part_digest: None,
        }
    }

    /// Copies at most one byte window from one selected part and checkpoints it.
    /// Old progress remains reusable after a lost or failed reply; orphan writes stay retained.
    /// # Errors
    /// Rejects changed selections/parts, invalid progress, byte excess and storage failures.
    pub async fn advance(
        &self,
        progress: &AssemblyProgress,
        part: &AssemblyPart,
    ) -> Result<AssemblyProgress, FileIoError> {
        self.validate(progress)?;
        let fingerprint = PartFingerprint {
            owner: part.owner,
            length: part.tree.length,
            digest: part.tree.digest,
        };
        if part.ordinal != progress.next_part
            || part.ordinal >= self.parts
            || part.owner.table != self.owner.table
            || progress.part_offset > part.tree.length
            || progress
                .active
                .as_ref()
                .is_some_and(|active| *active != fingerprint)
        {
            return Err(FileIoError::Bounds);
        }
        let remaining = part.tree.length - progress.part_offset;
        if progress
            .completed_bytes
            .checked_add(remaining)
            .map_or(true, |size| size > self.max_file_bytes)
        {
            return Err(FileIoError::Bounds);
        }
        let count = remaining.min(self.step_bytes as u64);
        let end = progress.part_offset + count;
        let mut reader = FileReader::from_tree(
            self.store.clone(),
            part.owner,
            part.tree.clone(),
            Some(ByteRange {
                start: progress.part_offset,
                end,
            }),
            16 * 1024,
        )?;
        let mut writer = self.writer(progress).await?;
        let mut part_digest = match &progress.part_digest {
            Some(bytes) => FileDigest::restore(part.owner, bytes)?,
            None => FileDigest::new(part.owner),
        };
        if part_digest.length() != progress.part_offset {
            return Err(FileIoError::Bounds);
        }
        let mut pending = reader.next().await?;
        while let Some(bytes) = pending {
            part_digest.update(&bytes)?;
            let (next, ()) = tokio::try_join!(reader.next(), writer.push(&bytes))?;
            pending = next;
        }
        if writer.length() != progress.completed_bytes + count {
            return Err(FileIoError::Bounds);
        }
        let complete = end == part.tree.length;
        let part_checkpoint = if complete {
            if part_digest.finish() != part.tree.digest {
                return Err(crate::error::ValidationError::Record.into());
            }
            None
        } else {
            Some(part_digest.checkpoint())
        };
        let checkpoint = writer.checkpoint().await?;
        Ok(AssemblyProgress {
            selection: self.selection,
            next_part: progress.next_part + u16::from(complete),
            part_offset: if complete { 0 } else { end },
            completed_bytes: writer.length(),
            writer: Some(checkpoint),
            active: (!complete).then_some(fingerprint),
            part_digest: part_checkpoint,
        })
    }

    /// Finalizes bytes only; semantic sealing and immutable publication are separate.
    /// # Errors
    /// Rejects incomplete selections, corrupt checkpoints and storage failures.
    pub async fn finish(&self, progress: &AssemblyProgress) -> Result<FileTree, FileIoError> {
        self.validate(progress)?;
        if progress.next_part != self.parts || progress.part_offset != 0 || progress.active.is_some() {
            return Err(FileIoError::Bounds);
        }
        self.writer(progress).await?.finish().await
    }

    fn validate(&self, progress: &AssemblyProgress) -> Result<(), FileIoError> {
        if progress.selection != self.selection
            || progress.next_part > self.parts
            || progress.completed_bytes > self.max_file_bytes
            || progress.part_offset > progress.completed_bytes
            || (progress.part_offset > 0) != progress.active.is_some()
            || progress.active.is_some() != progress.part_digest.is_some()
            || (progress.writer.is_none() && (progress.next_part != 0 || progress.completed_bytes != 0))
        {
            return Err(FileIoError::Bounds);
        }
        Ok(())
    }

    async fn writer(&self, progress: &AssemblyProgress) -> Result<FileTreeWriter, FileIoError> {
        let writer = match &progress.writer {
            Some(checkpoint) => {
                FileTreeWriter::restore(self.store.clone(), self.owner, self.block_bytes, checkpoint).await?
            }
            None => FileTreeWriter::new(self.store.clone(), self.owner, self.block_bytes)?,
        };
        if writer.length() != progress.completed_bytes {
            return Err(FileIoError::Bounds);
        }
        Ok(writer)
    }
}
