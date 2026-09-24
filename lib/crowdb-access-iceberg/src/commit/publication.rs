use std::sync::Arc;

use super::{
    prepare_table_commit, CommitProofError, CommitProofLimits, PreparedTableCommit, TableCommitJournal,
    TableCommitOperation, TableCommitOutcome, TableCommitPhase as Phase,
};
use crate::{
    catalog::{CatalogContext, CatalogError, CatalogStore},
    error::ValidationError,
    file::{FileBlockStore, FileIoError},
    key::{FileId, OperationId},
};

mod candidate;
mod completion;

#[derive(Debug, thiserror::Error)]
pub enum CommitPublicationError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error(transparent)]
    Proof(#[from] CommitProofError),
    #[error(transparent)]
    Storage(#[from] FileIoError),
    #[error(transparent)]
    Metadata(#[from] crate::table::TableMetadataError),
}

struct Publisher {
    store: Arc<dyn CatalogStore>,
    blocks: Arc<dyn FileBlockStore>,
    journal: TableCommitJournal,
}

impl PreparedTableCommit {
    /// Publishes exactly this validated generation or records a final CAS conflict.
    /// # Errors
    /// Unknown storage outcomes retain durable intent and must be recovered, never rebased.
    pub async fn publish(self) -> Result<TableCommitOutcome, CommitPublicationError> {
        let publisher = Publisher::new(self.store.clone(), self.blocks.clone());
        let mut operation = self.operation;
        publisher.current(&operation).await?;
        candidate::response(self.document.selected_head(), self.document.canonical())?;
        if operation.phase == Phase::Prepared {
            let mut next = advance(&operation, Phase::Validated)?;
            next.candidate = Some(self.document.selected_head().clone());
            publisher.change(&operation, &next).await?;
            operation = next;
        }
        if operation.phase == Phase::Validated {
            let next = advance(&operation, Phase::Writing)?;
            publisher.change(&operation, &next).await?;
            operation = next;
        }
        if operation.phase != Phase::Writing
            || operation.candidate.as_ref() != Some(self.document.selected_head())
        {
            return Err(CatalogError::Conflict.into());
        }
        publisher.write_candidate(&operation, &self.document).await?;
        let next = advance(&operation, Phase::Publishing)?;
        publisher.change(&operation, &next).await?;
        publisher.finish(next).await
    }
}

/// Resumes the same durable update on another instance, including a lost head-CAS response.
/// # Errors
/// Rejects retired domains, stale input validation, malformed intent and unknown storage outcomes.
pub async fn recover_table_commit(
    store: Arc<dyn CatalogStore>,
    blocks: Arc<dyn FileBlockStore>,
    context: CatalogContext,
    identity: OperationId,
    limits: CommitProofLimits,
) -> Result<TableCommitOutcome, CommitPublicationError> {
    let publisher = Publisher::new(store.clone(), blocks.clone());
    let operation = publisher
        .journal
        .load(context, identity)
        .await?
        .ok_or(CatalogError::Conflict)?;
    if matches!(
        operation.phase,
        Phase::Publishing | Phase::Published | Phase::Complete | Phase::Rejected
    ) {
        return publisher.finish(operation).await;
    }
    if let Some(rejected) = publisher.reject_superseded(&operation).await? {
        return publisher.finish(rejected).await;
    }
    let target = if let Some(candidate) = &operation.candidate {
        candidate.clone()
    } else {
        let mut head = operation.before.clone();
        head.generation = head.generation.checked_add(1).ok_or(ValidationError::Record)?;
        head.operation_fence = head
            .operation_fence
            .checked_add(1)
            .ok_or(ValidationError::Record)?;
        head.pending_operation = Some(identity);
        head.metadata_file = FileId::from_bytes(identity.as_bytes())?;
        head.metadata_location = head.metadata_location.table().file(&format!(
            "metadata/{}-{}.metadata.json",
            head.generation, identity
        ))?;
        head
    };
    Box::pin(prepare_table_commit(store, blocks, &operation, target, limits))
        .await?
        .publish()
        .await
}

impl Publisher {
    fn new(store: Arc<dyn CatalogStore>, blocks: Arc<dyn FileBlockStore>) -> Self {
        Self {
            journal: TableCommitJournal::new(store.clone()),
            store,
            blocks,
        }
    }

    async fn current(&self, operation: &TableCommitOperation) -> Result<(), CommitPublicationError> {
        if self
            .journal
            .load(operation.context, operation.identity.operation)
            .await?
            .as_ref()
            != Some(operation)
        {
            return Err(CatalogError::Conflict.into());
        }
        Ok(())
    }

    async fn change(
        &self,
        before: &TableCommitOperation,
        after: &TableCommitOperation,
    ) -> Result<(), CommitPublicationError> {
        if !self.journal.advance(before, after).await? {
            return Err(CatalogError::Busy.into());
        }
        Ok(())
    }
}

fn advance(operation: &TableCommitOperation, phase: Phase) -> Result<TableCommitOperation, ValidationError> {
    let mut next = operation.clone();
    next.revision = next.revision.checked_add(1).ok_or(ValidationError::Record)?;
    next.phase = phase;
    Ok(next)
}
