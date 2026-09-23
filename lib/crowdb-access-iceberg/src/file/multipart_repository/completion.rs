use std::sync::Arc;

use crate::catalog::CatalogError;
use crate::error::ValidationError;
use crate::file::{
    AssemblyPart, AssemblyProgress, FileAssembly, FileBlockStore, FileIoError, FileTree, MultipartCompletion,
    MultipartPhase, MultipartSelection, MultipartSession,
};
use crate::operation::PayloadStore;

use super::{check_live, increment, MultipartRepository};

#[derive(Debug, thiserror::Error)]
pub enum MultipartWorkError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    File(#[from] FileIoError),
    #[error(transparent)]
    Invalid(#[from] ValidationError),
}

impl MultipartRepository {
    /// Freezes a caller-selected part revision list; byte work verifies each selected part.
    /// # Errors
    /// Rejects expired sessions, unresolved mutations and invalid selection bounds.
    pub async fn freeze_completion(
        &self,
        session: &MultipartSession,
        selection: &MultipartSelection,
        now_ms: u64,
    ) -> Result<bool, CatalogError> {
        session.validate()?;
        check_live(session, now_ms)?;
        if session.phase != MultipartPhase::Open {
            return Err(CatalogError::Conflict);
        }
        if session.pending.is_some() {
            return Err(CatalogError::Busy);
        }
        if selection.parts().len() > usize::from(session.part_count)
            || selection
                .parts()
                .iter()
                .any(|part| part.number > session.limits.max_parts)
        {
            return Err(ValidationError::Record.into());
        }
        let mut next = increment(session)?;
        if self.load(session.context, session.upload).await?.as_ref() != Some(session) {
            return Ok(false);
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
        });
        self.exchange(session, &next).await
    }

    /// Copies one bounded byte window and conditionally advances durable progress.
    /// # Errors
    /// Rejects stale sessions, changed selections, missing parts and corrupt bytes.
    pub async fn advance_completion(
        &self,
        session: &MultipartSession,
        blocks: Arc<dyn FileBlockStore>,
        step_bytes: usize,
        block_bytes: usize,
    ) -> Result<bool, MultipartWorkError> {
        let completion = completing(session)?;
        let assembly = assembly(session, completion, blocks, step_bytes, block_bytes)?;
        let mut next = increment(session)?;
        if self.load(session.context, session.upload).await?.as_ref() != Some(session) {
            return Ok(false);
        }
        let bytes = PayloadStore::new(self.store.clone())
            .get(&completion.selection)
            .await?;
        let selection = MultipartSelection::decode(&bytes)?;
        if selection.parts().len() != usize::from(completion.selected_parts) {
            return Err(ValidationError::Record.into());
        }
        let selected = selection
            .parts()
            .get(usize::from(completion.progress.next_part))
            .ok_or(ValidationError::Record)?;
        let part = self
            .part(session, selected.number)
            .await?
            .ok_or(ValidationError::Record)?;
        if part.revision != selected.revision || part.tree.digest != selected.digest {
            return Err(ValidationError::Record.into());
        }
        let progress = assembly
            .advance(
                &completion.progress,
                &AssemblyPart {
                    ordinal: completion.progress.next_part,
                    owner: part.owner,
                    tree: part.tree,
                },
            )
            .await?;
        next.completion.as_mut().ok_or(ValidationError::Record)?.progress = progress;
        Ok(self.exchange(session, &next).await?)
    }

    /// Returns assembled bytes for semantic sealing, without publishing a file location.
    /// # Errors
    /// Rejects stale sessions, incomplete selections and invalid writer checkpoints.
    pub async fn assembled_tree(
        &self,
        session: &MultipartSession,
        blocks: Arc<dyn FileBlockStore>,
        block_bytes: usize,
    ) -> Result<FileTree, MultipartWorkError> {
        let completion = completing(session)?;
        if self.load(session.context, session.upload).await?.as_ref() != Some(session) {
            return Err(CatalogError::Conflict.into());
        }
        let tree = assembly(session, completion, blocks, 1, block_bytes)?
            .finish(&completion.progress)
            .await?;
        if self.load(session.context, session.upload).await?.as_ref() != Some(session) {
            return Err(CatalogError::Conflict.into());
        }
        Ok(tree)
    }
}

fn completing(session: &MultipartSession) -> Result<&MultipartCompletion, MultipartWorkError> {
    session.validate()?;
    if session.phase != MultipartPhase::Completing {
        return Err(CatalogError::Conflict.into());
    }
    session
        .completion
        .as_ref()
        .ok_or_else(|| ValidationError::Record.into())
}

fn assembly(
    session: &MultipartSession,
    completion: &MultipartCompletion,
    blocks: Arc<dyn FileBlockStore>,
    step_bytes: usize,
    block_bytes: usize,
) -> Result<FileAssembly, FileIoError> {
    FileAssembly::new(
        blocks,
        session.owner,
        completion.selection.digest,
        completion.selected_parts,
        session.limits.max_file_bytes,
        step_bytes,
        block_bytes,
    )
}
