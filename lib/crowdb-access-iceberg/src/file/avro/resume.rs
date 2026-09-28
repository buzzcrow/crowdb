use std::sync::Arc;

use super::{input::Input, AvroBlocks, AvroContainerError, AvroLimits};
use crate::file::{ByteRange, FileBlockStore, FileDigest, FileIdentity, FileReader, FileRecord};

impl AvroBlocks {
    /// Opens or resumes at a verified OCF block boundary using trusted durable state.
    /// # Errors
    /// Rejects foreign digest state, invalid offsets and malformed canonical headers.
    pub async fn resume(
        store: Arc<dyn FileBlockStore>,
        record: FileRecord,
        limits: AvroLimits,
        checkpoint: Option<&[u8]>,
    ) -> Result<Self, AvroContainerError> {
        let mut blocks = Self::open_inner(store.clone(), record.clone(), limits, true).await?;
        let Some(checkpoint) = checkpoint else {
            return Ok(blocks);
        };
        let digest = FileDigest::restore(
            FileIdentity {
                table: record.location.table(),
                file: record.file,
            },
            checkpoint,
        )?;
        let position = digest.length();
        if position < blocks.header.length || position > record.length {
            return Err(AvroContainerError::Framing);
        }
        let end = record.length;
        let reader = FileReader::new(store, record, Some(ByteRange { start: position, end }), 16 * 1024)?;
        blocks.input = Input::new(reader, end);
        blocks.input.position = position;
        blocks.input.digest = Some(digest);
        Ok(blocks)
    }

    /// # Errors
    /// Rejects non-resumable, cancelled or failed readers.
    pub fn checkpoint(&self) -> Result<Vec<u8>, AvroContainerError> {
        if self.failed {
            return Err(AvroContainerError::Failed);
        }
        self.input
            .digest
            .as_ref()
            .map(FileDigest::checkpoint)
            .ok_or(AvroContainerError::Failed)
    }
}
