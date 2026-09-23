use crate::{IoError, Result};

use super::{OwnedChunk, PendingObject};

impl OwnedChunk {
    pub(super) async fn confirm_batch_publication(
        &mut self,
        batch: &[PendingObject],
        end: u64,
    ) -> Result<()> {
        if !batch.iter().any(|object| object.durable_completion) {
            return Ok(());
        }
        self.flush_pending_advance().await?;
        if self.chunk.acknowledged_cursor < end {
            self.start_pending_advance(end)?;
            self.flush_pending_advance().await?;
        }
        if self.chunk.acknowledged_cursor < end {
            return Err(IoError::MetadataConflict(
                "durable completion did not cover the object".into(),
            ));
        }
        Ok(())
    }
}
