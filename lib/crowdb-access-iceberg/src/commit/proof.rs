use std::sync::Arc;
mod rejection;
pub(in crate::commit) use rejection::invalid_files;

use super::{
    evaluate_durable_commit, CandidateAuxiliaryLimits, CandidateSnapshotLimits, CommitPreparationError,
    CommitPreparationLimits, PriorManifestLimits, TableCommitOperation,
};
use crate::{
    catalog::{CatalogError, CatalogStore},
    file::FileBlockStore,
    manifest::{SnapshotManifestError, SnapshotValidationError},
    table::{TableHead, TableMetadataDocument, TableMetadataError},
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
}

/// Generation-bound structural evaluation for one table update.
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
/// Rejects stale journal/head state and invalid metadata updates.
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
    Ok(PreparedTableCommit {
        store,
        blocks,
        operation: operation.clone(),
        document: Arc::new(evaluated.document),
    })
}
