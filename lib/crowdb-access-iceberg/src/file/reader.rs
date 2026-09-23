use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::error::ValidationError;

use super::blocks::verify_block;
use super::{ByteRange, ChunkDirectory, FileBlockStore, FileContent, FileIdentity, FileIoError, FileRecord};

pub const MAX_READ_FRAME_BYTES: usize = 64 * 1024;

pub struct FileReader {
    store: Arc<dyn FileBlockStore>,
    record: FileRecord,
    cursor: u64,
    end: u64,
    frame_bytes: usize,
    cached: Option<(u64, Vec<u8>)>,
    digest: Option<Sha256>,
    failed: bool,
}

impl FileReader {
    /// # Errors
    /// Rejects invalid records, out-of-file intervals and unbounded frame sizes.
    pub fn new(
        store: Arc<dyn FileBlockStore>,
        record: FileRecord,
        range: Option<ByteRange>,
        frame_bytes: usize,
    ) -> Result<Self, FileIoError> {
        record.validate()?;
        let range = range.unwrap_or(ByteRange {
            start: 0,
            end: record.length,
        });
        if range.start > range.end
            || range.end > record.length
            || frame_bytes == 0
            || frame_bytes > MAX_READ_FRAME_BYTES
        {
            return Err(FileIoError::Bounds);
        }
        let cached = record
            .content
            .inline_bytes(record.length, &record.digest)?
            .map(|bytes| (0, bytes));
        let digest = (range.start == 0 && range.end == record.length).then(Sha256::new);
        Ok(Self {
            store,
            record,
            cursor: range.start,
            end: range.end,
            frame_bytes,
            cached,
            digest,
            failed: false,
        })
    }

    #[must_use]
    pub fn retained_payload_bytes(&self) -> usize {
        self.cached.as_ref().map_or(0, |(_, bytes)| bytes.capacity())
    }

    /// # Errors
    /// Stops permanently on corruption or storage failure; performs no speculative reads.
    pub async fn next(&mut self) -> Result<Option<Vec<u8>>, FileIoError> {
        if self.failed {
            return Err(FileIoError::Finished);
        }
        if self.cursor == self.end {
            return Ok(None);
        }
        self.failed = true;
        if !self
            .cached
            .as_ref()
            .is_some_and(|(start, bytes)| self.cursor >= *start && self.cursor - *start < bytes.len() as u64)
        {
            self.cached = None;
            self.cached = Some(self.select_leaf().await?);
        }
        let (start, bytes) = self.cached.as_ref().ok_or(ValidationError::Record)?;
        let offset = usize::try_from(self.cursor - *start).map_err(|_| FileIoError::Bounds)?;
        let count = self
            .frame_bytes
            .min(bytes.len() - offset)
            .min(usize::try_from(self.end - self.cursor).unwrap_or(usize::MAX));
        let result = bytes[offset..offset + count].to_vec();
        self.cursor += count as u64;
        if let Some(digest) = &mut self.digest {
            digest.update(&result);
        }
        if self.cursor == self.end {
            if let Some(digest) = self.digest.take() {
                if <[u8; 32]>::from(digest.finalize()) != self.record.digest {
                    return Err(ValidationError::Record.into());
                }
            }
        }
        self.failed = false;
        Ok(Some(result))
    }

    async fn select_leaf(&self) -> Result<(u64, Vec<u8>), FileIoError> {
        let FileContent::Chunks { root: Some(root) } = &self.record.content else {
            return Err(ValidationError::Record.into());
        };
        let mut root = root.clone();
        let mut start = 0;
        let mut length = self.record.length;
        let owner = FileIdentity {
            table: self.record.location.table(),
            file: self.record.file,
        };
        while root.height > 0 {
            let bytes = self.store.read(&root).await?;
            verify_block(&root, &bytes)?;
            let directory = ChunkDirectory::decode(&bytes, owner, root.height, length)?;
            let mut selected = None;
            for entry in directory.entries {
                if self.cursor - start < entry.length {
                    selected = Some(entry);
                    break;
                }
                start = start.checked_add(entry.length).ok_or(ValidationError::Record)?;
            }
            let entry = selected.ok_or(ValidationError::Record)?;
            root = entry.root;
            length = entry.length;
        }
        if root.logical_length != length {
            return Err(ValidationError::Record.into());
        }
        let bytes = self.store.read(&root).await?;
        verify_block(&root, &bytes)?;
        Ok((start, bytes))
    }
}
