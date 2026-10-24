use std::sync::Arc;

use super::{
    evaluate_table_creation, CreateTableRequest, TableCreateJournal, TableCreateOperation,
    TableCreatePhase as Phase,
};
use crate::{
    catalog::{check_context, CatalogContext, CatalogError, CatalogStore},
    commit::{CommitPublicationError as Error, TableCommitOutcome},
    error::ValidationError,
    file::{FileBlockStore, TableLocation},
    key::{FileId, OperationId, TableId},
    namespace::{NamespaceIdentifier, NamespaceRepository, NamespaceStore},
    operation::{PayloadStore, RequestIdentity, MAX_PAYLOAD_BYTES},
    table::{TableHead, TableLifecycle, TableMetadataDocument, TableMetadataLimits},
};

mod admission;
mod completion;
mod direct;
mod helping;
mod preparation;
mod reservation;
mod staged;
pub use staged::{StagedCommitLimits, StagedCommitRequest};

#[derive(Clone, Debug)]
pub struct TableCreationRequest {
    pub context: CatalogContext,
    pub identity: RequestIdentity,
    pub principal: String,
    pub namespace: NamespaceIdentifier,
    pub body: Vec<u8>,
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableCreationOutcome {
    pub status: u16,
    pub body: Vec<u8>,
}

#[derive(Clone)]
pub struct TableCreator {
    store: Arc<dyn CatalogStore>,
    names: Arc<dyn NamespaceStore>,
    blocks: Option<Arc<dyn FileBlockStore>>,
    staged_limits: Option<Arc<StagedCommitLimits>>,
    response_reserve: usize,
}

impl TableCreator {
    #[must_use]
    pub fn new<Store: NamespaceStore + 'static>(store: Arc<Store>, blocks: Arc<dyn FileBlockStore>) -> Self {
        Self {
            store: store.clone(),
            names: store,
            blocks: Some(blocks),
            staged_limits: None,
            response_reserve: 0,
        }
    }

    #[must_use]
    pub fn with_response_reserve(mut self, bytes: usize) -> Self {
        self.response_reserve = bytes;
        self
    }

    fn validate_response_size(&self, bytes: &[u8]) -> Result<(), Error> {
        if bytes
            .len()
            .checked_add(self.response_reserve)
            .map_or(true, |length| length > MAX_PAYLOAD_BYTES)
        {
            return Err(crate::table::TableMetadataError::Bounds.into());
        }
        Ok(())
    }

    /// # Errors
    /// Recovery retains the original identity; exhausted helping work remains retryable.
    pub async fn resume(
        &self,
        context: CatalogContext,
        identity: OperationId,
    ) -> Result<TableCommitOutcome, Error> {
        self.resume_with_budget(context, identity, &mut 32).await
    }

    async fn resume_with_budget(
        &self,
        context: CatalogContext,
        identity: OperationId,
        budget: &mut usize,
    ) -> Result<TableCommitOutcome, Error> {
        while *budget > 0 {
            *budget -= 1;
            let operation = self
                .journal()
                .load(context, identity)
                .await?
                .ok_or(ValidationError::Record)?;
            match operation.phase {
                Phase::Staged => return Err(Error::Unsupported("unbound staged table")),
                Phase::Prepared => self.reserve(&operation, budget).await?,
                Phase::Reserved => self.write_metadata(&operation).await?,
                Phase::FilesReady => {
                    if self.parent_ready(&operation).await? {
                        self.journal()
                            .advance(&operation, &operation.next(Phase::Publishing)?)
                            .await?;
                    } else {
                        self.abort(&operation, 404).await?;
                    }
                }
                Phase::Admitting => self.admit(&operation).await?,
                Phase::Admitted => {
                    self.check_admission(&operation).await?;
                    self.journal()
                        .advance(&operation, &operation.next(Phase::Publishing)?)
                        .await?;
                }
                Phase::Publishing => self.publish_head(&operation).await?,
                Phase::Published => self.publish_name(&operation).await?,
                Phase::Aborting => {
                    self.cleanup(&operation).await?;
                    self.journal()
                        .advance(&operation, &operation.next(Phase::Aborted)?)
                        .await?;
                }
                Phase::Complete | Phase::Aborted => {
                    self.cleanup(&operation).await?;
                    let outcome = operation.outcome.ok_or(ValidationError::Record)?;
                    self.payloads().get(&outcome.body).await?;
                    check_context(self.store.as_ref(), context).await?;
                    return Ok(outcome);
                }
            }
        }
        Err(CatalogError::Busy.into())
    }

    async fn write_metadata(&self, operation: &TableCreateOperation) -> Result<(), Error> {
        let blocks = self.blocks.clone().ok_or(CatalogError::Busy)?;
        let bytes = self.payloads().get(&operation.document).await?;
        let document = Arc::new(TableMetadataDocument::parse(
            bytes,
            &operation.candidate,
            limits(),
        )?);
        if operation.stage.is_none() && !document.snapshots().is_empty() {
            return Err(ValidationError::Record.into());
        }
        self.current(operation).await?;
        crate::commit::publication::write_metadata_file(
            self.store.clone(),
            blocks,
            operation.context,
            &document,
        )
        .await?;
        self.journal()
            .advance(operation, &operation.next(Phase::FilesReady)?)
            .await?;
        Ok(())
    }

    fn journal(&self) -> TableCreateJournal {
        TableCreateJournal::new(self.store.clone())
    }

    fn payloads(&self) -> PayloadStore {
        PayloadStore::new(self.store.clone())
    }

    async fn current(&self, operation: &TableCreateOperation) -> Result<(), Error> {
        if self
            .journal()
            .load(operation.context, operation.identity.operation)
            .await?
            .as_ref()
            != Some(operation)
        {
            return Err(CatalogError::Busy.into());
        }
        Ok(())
    }
}

fn limits() -> TableMetadataLimits {
    TableMetadataLimits {
        bytes: MAX_PAYLOAD_BYTES,
        values: 1_000_000,
        depth: 64,
        string_bytes: MAX_PAYLOAD_BYTES,
        collection_entries: 100_000,
    }
}
