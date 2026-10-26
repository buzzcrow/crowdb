use std::sync::Arc;

use super::TableCreateJournal;
use crate::{
    catalog::{check_context, CatalogContext, CatalogError, CatalogStore},
    commit::CommitPublicationError as Error,
    file::FileBlockStore,
    key::{OperationId, TableId},
    namespace::{NamespaceIdentifier, NamespaceStore},
    operation::{RequestIdentity, MAX_PAYLOAD_BYTES},
    table::TableMetadataLimits,
};

mod direct;
mod helping;
mod legacy;
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

    /// Reconciles an existing legacy record from final resource facts only.
    /// # Errors
    /// Unknown writes stop cleanup; missing metadata and initial heads are never published.
    pub async fn resume(&self, context: CatalogContext, identity: OperationId) -> Result<(), Error> {
        let operation = TableCreateJournal::new(self.store.clone())
            .load(context, identity)
            .await?
            .ok_or(CatalogError::Conflict)?;
        self.reconcile_legacy(&operation).await
    }

    /// Cleans an expired legacy draft without resuming its byte or publication work.
    /// # Errors
    /// Rejects missing, foreign or malformed legacy records and uncertain resource writes.
    pub async fn expire_stage(
        &self,
        context: CatalogContext,
        table: TableId,
        now_ms: i64,
    ) -> Result<bool, Error> {
        let identity = OperationId::from_bytes(table.as_bytes())?;
        let operation = TableCreateJournal::new(self.store.clone())
            .load(context, identity)
            .await?
            .ok_or(CatalogError::Conflict)?;
        let stage = operation.stage.as_ref().ok_or(CatalogError::Conflict)?;
        if now_ms < 0 || operation.candidate.table != table {
            return Err(crate::error::ValidationError::IdentityMismatch.into());
        }
        if operation.phase != super::TableCreatePhase::Staged || stage.expires_ms > now_ms {
            return Ok(false);
        }
        legacy::reconcile(self.store.clone(), self.names.clone(), &operation).await?;
        check_context(self.store.as_ref(), context).await?;
        Ok(true)
    }

    pub(crate) async fn reconcile_legacy(
        &self,
        operation: &super::TableCreateOperation,
    ) -> Result<(), Error> {
        legacy::reconcile(self.store.clone(), self.names.clone(), operation).await?;
        check_context(self.store.as_ref(), operation.context).await?;
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
