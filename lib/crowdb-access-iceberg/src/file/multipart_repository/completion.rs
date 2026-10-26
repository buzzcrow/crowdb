use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::catalog::CatalogError;
use crate::error::ValidationError;
use crate::file::{
    AssemblyPart, AssemblyProgress, FileAssembly, FileBlockStore, FileIoError, FileTree, MultipartCompletion,
    MultipartPhase, MultipartSelection, MultipartSession,
};
use crate::operation::PayloadStore;

use super::{check_live, increment, MultipartMutation, MultipartRepository};

#[derive(Debug, thiserror::Error)]
pub enum MultipartWorkError {
    #[error("legacy multipart byte progress cannot be resumed; abort and create a new upload")]
    LegacyProgress,
    #[error("legacy multipart assembly exceeds the complete request byte limit")]
    CopyLimit,
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    File(#[from] FileIoError),
    #[error(transparent)]
    Invalid(#[from] ValidationError),
}

impl MultipartRepository {
    /// Loads the durable Complete selection, including any streamed part snapshots.
    /// # Errors
    /// Rejects missing, corrupt or foreign selection payloads.
    pub async fn load_selection(
        &self,
        session: &MultipartSession,
    ) -> Result<MultipartSelection, CatalogError> {
        session.validate()?;
        let completion = session.completion.as_ref().ok_or(ValidationError::Record)?;
        let bytes = PayloadStore::new(self.store.clone())
            .get(&completion.selection)
            .await?;
        let selection = MultipartSelection::decode(&bytes)?;
        if selection.count() != completion.selected_parts {
            return Err(ValidationError::Record.into());
        }
        Ok(selection)
    }

    /// Freezes a caller-selected part revision list; byte work verifies each selected part.
    /// # Errors
    /// Rejects expired sessions, unresolved mutations and invalid selection bounds.
    pub async fn freeze_completion(
        &self,
        session: &MultipartSession,
        selection: &MultipartSelection,
        now_ms: u64,
    ) -> Result<MultipartMutation<()>, CatalogError> {
        session.validate()?;
        check_live(session, now_ms)?;
        if session.phase != MultipartPhase::Open {
            return Err(CatalogError::Conflict);
        }
        if session.pending.is_some() {
            return Err(CatalogError::Busy);
        }
        if (selection.snapshots().is_none() && selection.parts().len() > usize::from(session.part_count))
            || selection
                .parts()
                .iter()
                .any(|part| part.number > session.limits.max_parts)
        {
            return Err(ValidationError::Record.into());
        }
        let mut next = increment(session)?;
        let current = self.load(session.context, session.upload).await?;
        if current.as_ref() != Some(session) {
            return Ok(MultipartMutation::Conflict(current));
        }
        let reference = PayloadStore::new(self.store.clone())
            .put(session.context.catalog, session.upload, &selection.encode())
            .await?;
        next.phase = MultipartPhase::Completing;
        next.completion = Some(MultipartCompletion {
            progress: AssemblyProgress {
                selection: reference.digest,
                next_part: 0,
                part_offset: 0,
                completed_bytes: 0,
                writer: None,
                active: None,
                part_digest: None,
            },
            selection: reference,
            selected_parts: selection.count(),
            candidate: None,
            publication: None,
        });
        self.exchange_state(session, &next, ()).await
    }

    /// Copies the complete selected legacy tree parts within this request's byte budget.
    /// No byte cursor or writer checkpoint is persisted; retries start from whole parts.
    /// # Errors
    /// Rejects stale sessions, selection changes, excess total bytes and corrupt parts.
    pub async fn assemble(
        &self,
        session: &MultipartSession,
        selection: &MultipartSelection,
        blocks: Arc<dyn FileBlockStore>,
        max_copy_bytes: u64,
        block_bytes: usize,
    ) -> Result<FileTree, MultipartWorkError> {
        session.validate()?;
        let completion = session.completion.as_ref().ok_or(ValidationError::Record)?;
        if session.phase != MultipartPhase::Completing {
            return Err(CatalogError::Conflict.into());
        }
        if completion.progress.writer.is_some() {
            // Legacy partial frontiers remain owned by their original GC record.
            return Err(MultipartWorkError::LegacyProgress);
        }
        if selection.count() != completion.selected_parts
            || <[u8; 32]>::from(Sha256::digest(selection.encode())) != completion.selection.digest
        {
            return Err(ValidationError::Record.into());
        }
        if self.load(session.context, session.upload).await?.as_ref() != Some(session) {
            return Err(CatalogError::Conflict.into());
        }
        let mut parts = Vec::with_capacity(usize::from(selection.count()));
        let mut length = 0_u64;
        let max_bytes = max_copy_bytes.min(session.limits.max_file_bytes);
        if max_bytes == 0 {
            return Err(MultipartWorkError::CopyLimit);
        }
        for selected in selection.parts() {
            let part = self.selected_part(session, selected).await?;
            let tree = part.tree.ok_or(ValidationError::Record)?;
            length = length
                .checked_add(tree.length)
                .ok_or(MultipartWorkError::CopyLimit)?;
            if length > max_bytes {
                return Err(MultipartWorkError::CopyLimit);
            }
            parts.push((part.owner, tree));
        }
        let mut assembly = FileAssembly::new(
            blocks,
            session.owner,
            selection.count(),
            max_bytes,
            16 * 1024,
            block_bytes,
        )?;
        for (ordinal, (owner, tree)) in parts.into_iter().enumerate() {
            assembly
                .push(&AssemblyPart {
                    ordinal: u16::try_from(ordinal).map_err(|_| ValidationError::Record)?,
                    owner,
                    tree,
                })
                .await?;
        }
        let tree = assembly.finish().await?;
        if self.load(session.context, session.upload).await?.as_ref() != Some(session) {
            return Err(CatalogError::Conflict.into());
        }
        Ok(tree)
    }
}
