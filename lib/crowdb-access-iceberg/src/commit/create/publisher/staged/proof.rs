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
    matches!(error, Error::Proof(CommitProofError::Files(error)) if crate::commit::proof::invalid_files(error))
}
