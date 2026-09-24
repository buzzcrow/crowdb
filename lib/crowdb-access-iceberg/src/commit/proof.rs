use std::sync::Arc;
mod rejection;
pub(in crate::commit) use rejection::invalid_files;

use super::{
    evaluate_durable_commit, CandidateAuxiliaryLimits, CandidateFileSource, CandidateSnapshotLimits,
    CommitPreparationError, CommitPreparationLimits, PriorManifestLimits, PriorManifestSource,
    TableCommitJournal, TableCommitOperation,
};
use crate::{
    catalog::{CatalogError, CatalogStore},
    file::{FileBlockStore, FileRepository},
    manifest::{SnapshotManifestError, SnapshotValidationError},
    table::{
        read_table_metadata_document, SelectedTable, TableHead, TableMetadataDocument, TableMetadataError,
    },
};

#[derive(Clone, Copy, Debug)]
pub struct CommitProofLimits {
    pub preparation: CommitPreparationLimits,
    pub prior: PriorManifestLimits,
    pub snapshots: CandidateSnapshotLimits,
    pub auxiliary: CandidateAuxiliaryLimits,
}

#[derive(Debug, thiserror::Error)]
pub enum CommitProofError {
    #[error(transparent)]
    Preparation(#[from] CommitPreparationError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Metadata(#[from] TableMetadataError),
    #[error(transparent)]
    Manifest(#[from] SnapshotManifestError),
    #[error(transparent)]
    Files(#[from] SnapshotValidationError),
    #[error("partition statistics selected-use validation is not enabled")]
    UnsupportedPartitionStatistics,
}

/// Generation-bound evidence for the enabled canonical selected-file validation profile.
/// Construction requires durable ordered evaluation and all retained snapshot checks.
pub struct PreparedTableCommit {
    pub(super) store: Arc<dyn CatalogStore>,
    pub(super) blocks: Arc<dyn FileBlockStore>,
    pub(super) operation: TableCommitOperation,
    pub(super) document: Arc<TableMetadataDocument>,
}

impl PreparedTableCommit {
    #[must_use]
    pub fn head(&self) -> &TableHead {
        self.document.selected_head()
    }
}

/// Builds non-forgeable evidence without writing candidate bytes or changing the head.
/// # Errors
/// Rejects stale journal/head state, unsupported selected uses and any bounded file-check failure.
pub async fn prepare_table_commit(
    store: Arc<dyn CatalogStore>,
    blocks: Arc<dyn FileBlockStore>,
    operation: &TableCommitOperation,
    target: TableHead,
    limits: CommitProofLimits,
) -> Result<PreparedTableCommit, CommitProofError> {
    let evaluated = evaluate_durable_commit(
        store.clone(),
        blocks.clone(),
        operation,
        target,
        limits.preparation,
    )
    .await?;
    let document = Arc::new(evaluated.document);
    if document
        .fields()
        .get("partition-statistics")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|entries| !entries.is_empty())
    {
        return Err(CommitProofError::UnsupportedPartitionStatistics);
    }
    let selected = SelectedTable {
        head: operation.before.clone(),
        metadata: FileRepository::new(store.clone())
            .load(operation.context, &operation.before.metadata_location)
            .await?
            .ok_or(TableMetadataError::Binding)?,
    };
    let prior_document =
        read_table_metadata_document(blocks.clone(), &selected, limits.preparation.evaluation.metadata)
            .await?;
    let prior = Arc::new(
        PriorManifestSource::build(
            store.clone(),
            blocks.clone(),
            operation.context,
            &selected,
            &prior_document,
            limits.prior,
        )
        .await?,
    );
    let source = Arc::new(CandidateFileSource::new(
        store.clone(),
        blocks.clone(),
        operation.context,
        prior,
        document.clone(),
        limits.prior.manifests.framing,
    )?);
    Box::pin(
        source
            .clone()
            .validate_snapshots(&prior_document, limits.snapshots),
    )
    .await?;
    source.validate_auxiliary_files(limits.auxiliary).await?;
    if TableCommitJournal::new(store.clone())
        .load(operation.context, operation.identity.operation)
        .await?
        .as_ref()
        != Some(operation)
    {
        return Err(CatalogError::Conflict.into());
    }
    Ok(PreparedTableCommit {
        store,
        blocks,
        operation: operation.clone(),
        document,
    })
}
