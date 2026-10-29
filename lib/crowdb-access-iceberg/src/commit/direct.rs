use std::sync::Arc;

use crate::{
    catalog::{CasOutcome, CatalogContext, CatalogError, CatalogStore},
    error::ValidationError,
    file::{FileBlockStore, FileIoError, FileRepository},
    key::FileId,
    operation::{mutation_identity, RequestIdentity},
    record::StorageRecord,
    table::{
        read_table_metadata_document, storage_head_key, SelectedTable, TableHead, TableLifecycle,
        TableMetadataError,
    },
};

use super::{evaluate_metadata_updates, publication, CommitProofLimits, CommitRequest, EvaluationError};

#[derive(Debug, thiserror::Error)]
pub enum DirectCommitError {
    #[error("table commit was not applied because the selected head changed")]
    Conflict,
    #[error("table commit outcome is uncertain; reload the table before retrying")]
    Uncertain,
    #[error("cannot expire a current or referenced snapshot")]
    ReferencedSnapshot,
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Evaluation(#[from] EvaluationError),
    #[error(transparent)]
    Metadata(#[from] TableMetadataError),
    #[error(transparent)]
    Storage(#[from] FileIoError),
    #[error(transparent)]
    Publication(#[from] publication::CommitPublicationError),
    #[error(transparent)]
    Validation(#[from] ValidationError),
}

/// Publishes one independently prepared table generation through one head CAS.
/// A reused operation ID always names the same metadata path, preventing a
/// retry from applying the update to a newer base generation.
/// # Errors
/// Returns an uncertain outcome when another generation may hide a prior success.
#[allow(clippy::too_many_arguments)]
pub async fn publish_direct_commit(
    store: Arc<dyn CatalogStore>,
    blocks: Arc<dyn FileBlockStore>,
    context: CatalogContext,
    before: TableHead,
    request: &CommitRequest,
    identity: RequestIdentity,
    binding: [u8; 32],
    now_ms: u64,
    limits: CommitProofLimits,
) -> Result<Vec<u8>, DirectCommitError> {
    identity.validate(now_ms)?;
    if before.catalog != context.catalog || before.lifecycle != TableLifecycle::Ready {
        return Err(DirectCommitError::Conflict);
    }
    if before.pending_operation.is_some() {
        return Err(DirectCommitError::Uncertain);
    }
    let file = FileId::from_bytes(identity.operation.as_bytes())?;
    let replay = before.metadata_file == file;
    if replay && before.commit_binding != Some(binding) {
        return Err(if before.commit_binding.is_some() {
            DirectCommitError::Conflict
        } else {
            DirectCommitError::Uncertain
        });
    }
    let metadata = FileRepository::new(store.clone())
        .load(context, &before.metadata_location)
        .await?
        .ok_or(TableMetadataError::Binding)?;
    let prior = read_table_metadata_document(
        blocks.clone(),
        &SelectedTable {
            head: before.clone(),
            metadata,
        },
        limits.preparation.evaluation.metadata,
    )
    .await?;
    if replay {
        return Ok(publication::metadata_response(&before, prior.canonical())?);
    }
    validate_guard(&store, context, &before, request, identity, &prior).await?;
    let mut candidate = before.clone();
    candidate.commit_binding = Some(binding);
    candidate.generation = candidate
        .generation
        .checked_add(1)
        .ok_or(ValidationError::Record)?;
    candidate.operation_fence = candidate
        .operation_fence
        .checked_add(1)
        .ok_or(ValidationError::Record)?;
    candidate.metadata_file = file;
    candidate.metadata_location = before
        .metadata_location
        .table()
        .file(&format!("metadata/{}.metadata.json", identity.operation))?;
    let evaluated = evaluate_metadata_updates(
        &prior,
        request,
        candidate,
        i64::try_from(identity.issued_ms).map_err(|_| ValidationError::Deadline)?,
        limits.preparation.evaluation,
    )?;
    let candidate = evaluated.head;
    let document = evaluated.document;
    let body = publication::metadata_response(&candidate, document.canonical())?;
    match publication::write_metadata_file(store.clone(), blocks, context, &document).await {
        Ok(()) => {}
        Err(publication::CommitPublicationError::Catalog(CatalogError::Conflict)) => {
            return Err(DirectCommitError::Uncertain);
        }
        Err(error) => return Err(error.into()),
    }
    let key = storage_head_key(before.storage_location()).encode()?;
    let expected = StorageRecord::TableHead(Box::new(before.clone())).encode()?;
    let value = StorageRecord::TableHead(Box::new(candidate.clone())).encode()?;
    let outcome = store
        .compare_exchange(
            &key,
            Some(&expected),
            &value,
            mutation_identity(&key, Some(&expected), &value),
        )
        .await
        .map_err(CatalogError::from)?;
    match outcome {
        CasOutcome::Applied(_) => Ok(body),
        CasOutcome::Conflict(Some(observed)) if observed.bytes == value => Ok(body),
        CasOutcome::Conflict(Some(observed)) => {
            let StorageRecord::TableHead(current) =
                StorageRecord::decode(&storage_head_key(before.storage_location()), &observed.bytes)?
            else {
                return Err(ValidationError::Record.into());
            };
            if current.generation == candidate.generation && current.metadata_file != candidate.metadata_file
            {
                Err(DirectCommitError::Conflict)
            } else {
                Err(DirectCommitError::Uncertain)
            }
        }
        CasOutcome::Conflict(None) => Err(DirectCommitError::Uncertain),
    }
}

async fn validate_guard(
    store: &Arc<dyn CatalogStore>,
    context: CatalogContext,
    before: &TableHead,
    request: &CommitRequest,
    identity: RequestIdentity,
    prior: &crate::table::TableMetadataDocument,
) -> Result<(), DirectCommitError> {
    if let Some(base) = &request.base_metadata_location {
        let operation_location = before
            .metadata_location
            .table()
            .file(&format!("metadata/{}.metadata.json", identity.operation))?;
        // A later generation can conceal an earlier successful publication.
        if *base != before.metadata_location.to_string() {
            if FileRepository::new(store.clone())
                .load(context, &operation_location)
                .await?
                .is_some()
            {
                return Err(DirectCommitError::Uncertain);
            }
            return Err(DirectCommitError::Conflict);
        }
        let metadata: serde_json::Value =
            serde_json::from_slice(prior.canonical()).map_err(TableMetadataError::from)?;
        for update in &request.updates {
            if let super::TableUpdate::RemoveSnapshots { snapshot_ids } = update {
                for id in snapshot_ids {
                    if metadata["current-snapshot-id"].as_i64() == Some(*id)
                        || metadata["refs"].as_object().is_some_and(|refs| {
                            refs.values()
                                .any(|reference| reference["snapshot-id"].as_i64() == Some(*id))
                        })
                    {
                        return Err(DirectCommitError::ReferencedSnapshot);
                    }
                }
            }
        }
    }
    Ok(())
}
