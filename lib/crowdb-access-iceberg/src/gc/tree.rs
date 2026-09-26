use sha2::{Digest, Sha256};

use crate::{
    error::ValidationError,
    file::{ChunkDirectory, ChunkRoot, FileBlockStore, FileContent, FileIdentity, FileIoError, FileRecord},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReclaimFrame {
    pub root: ChunkRoot,
    pub length: u64,
    pub next_child: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeReclaimCursor {
    pub owner: FileIdentity,
    pub frames: Vec<ReclaimFrame>,
    pub pending: Option<ChunkRoot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReclaimStep {
    Descended(TreeReclaimCursor),
    Delete(TreeReclaimCursor),
    Complete,
}

impl TreeReclaimCursor {
    /// # Errors
    /// Rejects an invalid immutable file authority before creating a deletion cursor.
    pub fn new(file: &FileRecord) -> Result<Self, ValidationError> {
        file.validate()?;
        let frames = match &file.content {
            FileContent::Chunks { root: Some(root) } => vec![ReclaimFrame {
                root: root.clone(),
                length: file.length,
                next_child: 0,
            }],
            _ => Vec::new(),
        };
        Ok(Self {
            owner: FileIdentity {
                table: file.location.table(),
                file: file.file,
            },
            frames,
            pending: None,
        })
    }

    /// # Errors
    /// Rejects oversized stacks, inconsistent heights and invalid pending work.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.frames.len() > 9 {
            return Err(ValidationError::RecordTooLarge);
        }
        for (index, frame) in self.frames.iter().enumerate() {
            frame.root.validate()?;
            if frame.length == 0
                || frame.next_child > 256
                || (frame.root.height == 0
                    && (frame.next_child != 0 || frame.length != frame.root.logical_length))
                || (index > 0 && self.frames[index - 1].root.height != frame.root.height + 1)
            {
                return Err(ValidationError::Record);
            }
        }
        if let Some(root) = &self.pending {
            root.validate()?;
            if self
                .frames
                .last()
                .is_some_and(|frame| frame.root.height != root.height + 1)
            {
                return Err(ValidationError::Record);
            }
        }
        Ok(())
    }

    /// Plans at most one directory read or one deletion, without modifying storage.
    /// The returned cursor must be durable before dispatching its pending deletion.
    /// # Errors
    /// Rejects corrupt directories and incomplete pending deletions.
    pub async fn next(&self, blocks: &dyn FileBlockStore) -> Result<ReclaimStep, FileIoError> {
        self.validate()?;
        if self.pending.is_some() {
            return Ok(ReclaimStep::Delete(self.clone()));
        }
        let Some(frame) = self.frames.last() else {
            return Ok(ReclaimStep::Complete);
        };
        let mut next = self.clone();
        if frame.root.height > 0 {
            let bytes = blocks.read(&frame.root).await?;
            if bytes.len() as u64 != frame.root.logical_length
                || <[u8; 32]>::from(Sha256::digest(&bytes)) != frame.root.digest
            {
                return Err(ValidationError::Record.into());
            }
            let directory = ChunkDirectory::decode(&bytes, self.owner, frame.root.height, frame.length)?;
            if usize::from(frame.next_child) > directory.entries.len() {
                return Err(ValidationError::Record.into());
            }
            if let Some(entry) = directory.entries.get(usize::from(frame.next_child)) {
                next.frames.last_mut().ok_or(ValidationError::Record)?.next_child += 1;
                next.frames.push(ReclaimFrame {
                    root: entry.root.clone(),
                    length: entry.length,
                    next_child: 0,
                });
                return Ok(ReclaimStep::Descended(next));
            }
        }
        next.pending = Some(next.frames.pop().ok_or(ValidationError::Record)?.root);
        next.validate()?;
        Ok(ReclaimStep::Delete(next))
    }

    /// # Errors
    /// Rejects an acknowledgement for a different physical deletion intent.
    pub fn acknowledge(&self, root: &ChunkRoot) -> Result<Self, ValidationError> {
        self.validate()?;
        if self.pending.as_ref() != Some(root) {
            return Err(ValidationError::IdentityMismatch);
        }
        let mut next = self.clone();
        next.pending = None;
        Ok(next)
    }
}
