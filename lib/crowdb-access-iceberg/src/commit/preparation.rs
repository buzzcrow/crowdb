use std::sync::Arc;

use super::{
    evaluate_metadata_updates, CommitRequest, CommitRequestLimits, EvaluatedMetadata, EvaluationError,
    EvaluationLimits, TableCommitJournal, TableCommitOperation, TableCommitPhase,
};
use crate::{
    catalog::{CatalogError, CatalogStore},
    file::{FileBlockStore, FileRepository},
    operation::PayloadStore,
    table::{read_table_metadata_document, SelectedTable, TableHead, TableMetadataError, TableRepository},
};

#[derive(Clone, Copy, Debug)]
pub struct CommitPreparationLimits {
    pub request: CommitRequestLimits,
    pub evaluation: EvaluationLimits,
}

#[derive(Debug, thiserror::Error)]
pub enum CommitPreparationError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Metadata(#[from] TableMetadataError),
    #[error(transparent)]
    Evaluation(#[from] EvaluationError),
}

/// Rebuilds one candidate from its durable request and original canonical generation.
/// The result is structural evaluation only; file proofs and publication are separate.
/// # Errors
/// Rejects stale phases/heads, corrupted payloads, changed recovery targets and invalid updates.
pub async fn evaluate_durable_commit(
    store: Arc<dyn CatalogStore>,
    blocks: Arc<dyn FileBlockStore>,
    operation: &TableCommitOperation,
    target: TableHead,
    limits: CommitPreparationLimits,
) -> Result<EvaluatedMetadata, CommitPreparationError> {
    operation.validate().map_err(CatalogError::from)?;
    if !matches!(
        operation.phase,
        TableCommitPhase::Prepared | TableCommitPhase::Validated | TableCommitPhase::Writing
    ) || operation
        .candidate
        .as_ref()
        .is_some_and(|candidate| candidate != &target)
    {
        return Err(CatalogError::Conflict.into());
    }
    if operation.input.length > limits.request.json.bytes {
        return Err(TableMetadataError::Bounds.into());
    }
    let journal = TableCommitJournal::new(store.clone());
    ensure_operation(&journal, operation).await?;
    let payload = PayloadStore::new(store.clone()).get(&operation.input).await?;
    let request = CommitRequest::decode(&payload, limits.request)?;
    let metadata = FileRepository::new(store.clone())
        .load_for_commit(operation.context, &operation.before.metadata_location)
        .await?
        .ok_or(TableMetadataError::Binding)?;
    let selected = SelectedTable {
        head: operation.before.clone(),
        metadata,
    };
    let tables = TableRepository::new(store);
    tables.ensure_current(operation.context, &selected).await?;
    let prior = read_table_metadata_document(blocks, &selected, limits.evaluation.metadata).await?;
    let evaluated = evaluate_metadata_updates(
        &prior,
        &request,
        target,
        operation.timestamp_ms,
        limits.evaluation,
    )?;
    let mut checked = operation.clone();
    checked.phase = TableCommitPhase::Validated;
    checked.candidate = Some(evaluated.head.clone());
    checked.validate().map_err(CatalogError::from)?;
    if operation
        .candidate
        .as_ref()
        .is_some_and(|candidate| candidate != &evaluated.head)
    {
        return Err(TableMetadataError::Binding.into());
    }
    tables.ensure_current(operation.context, &selected).await?;
    ensure_operation(&journal, operation).await?;
    Ok(evaluated)
}

async fn ensure_operation(
    journal: &TableCommitJournal,
    operation: &TableCommitOperation,
) -> Result<(), CatalogError> {
    if journal
        .load(operation.context, operation.identity.operation)
        .await?
        .as_ref()
        != Some(operation)
    {
        return Err(CatalogError::Conflict);
    }
    Ok(())
}
