use std::sync::Arc;

use super::{Error, TableCreateOperation, TableCreator};
use crate::{
    commit::{CandidateFileSource, CommitProofError},
    file::FileBlockStore,
    table::TableMetadataDocument,
};

impl TableCreator {
    pub(in crate::commit::create::publisher) async fn validate_initial_files(
        &self,
        operation: &TableCreateOperation,
        blocks: Arc<dyn FileBlockStore>,
        document: Arc<TableMetadataDocument>,
    ) -> Result<(), Error> {
        let limits = self
            .staged_limits
            .as_ref()
            .ok_or(Error::Unsupported("staged commit file limits"))?;
        if document
            .fields()
            .get("partition-statistics")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|entries| !entries.is_empty())
        {
            return Err(CommitProofError::UnsupportedPartitionStatistics.into());
        }
        let source = Arc::new(
            CandidateFileSource::for_creation(
                self.store.clone(),
                blocks,
                operation,
                document,
                limits.snapshots.files.manifests.framing,
            )
            .map_err(CommitProofError::from)?,
        );
        Box::pin(source.clone().validate_initial_snapshots(limits.snapshots))
            .await
            .map_err(CommitProofError::from)?;
        source
            .validate_auxiliary_files(limits.auxiliary)
            .await
            .map_err(CommitProofError::from)?;
        Ok(())
    }
}

pub(in crate::commit::create::publisher) fn definite_validation_failure(error: &Error) -> bool {
    use crate::file::ParquetMetadataError;
    use crate::manifest::{SelectedParquetError, SnapshotManifestError, SnapshotValidationError};
    matches!(
        error,
        Error::Proof(CommitProofError::Files(
            SnapshotValidationError::Binding
                | SnapshotValidationError::Bounds
                | SnapshotValidationError::Unsupported
                | SnapshotValidationError::Unavailable
                | SnapshotValidationError::Parquet(
                    SelectedParquetError::Delete
                        | SelectedParquetError::Schema
                        | SelectedParquetError::Unsupported
                        | SelectedParquetError::Binding
                        | SelectedParquetError::Rows
                        | SelectedParquetError::Metadata(
                            ParquetMetadataError::Invalid
                                | ParquetMetadataError::Bounds
                                | ParquetMetadataError::Unsupported
                        )
                )
                | SnapshotValidationError::Manifest(
                    SnapshotManifestError::Bounds
                        | SnapshotManifestError::RowIds
                        | SnapshotManifestError::Unavailable
                        | SnapshotManifestError::Identity(_)
                )
        ))
    )
}
