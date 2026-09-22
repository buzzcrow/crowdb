use std::collections::BTreeMap;
use std::sync::Arc;

use crate::catalog::{CatalogContext, CatalogError};
use crate::error::ValidationError;
use crate::key::NamespaceId;
use crate::operation::{PayloadStore, RequestIdentity};
use crate::record::StorageRecord;

use super::{
    NamespaceAction, NamespaceAuthority, NamespaceIdentifier, NamespaceJournal, NamespaceLifecycle,
    NamespaceOperation, NamespaceOutcome, NamespacePhase, NamespaceProperties, NamespaceRepository,
    NamespaceStore,
};

#[derive(Clone, Debug)]
pub struct NamespaceCreateRequest {
    pub context: CatalogContext,
    pub identity: RequestIdentity,
    pub principal: String,
    pub identifier: NamespaceIdentifier,
    pub properties: NamespaceProperties,
}

pub struct NamespaceCreator {
    pub(super) repository: NamespaceRepository,
    pub(super) names: Arc<dyn NamespaceStore>,
}

impl NamespaceCreator {
    #[must_use]
    pub fn new<Store: NamespaceStore + 'static>(store: Arc<Store>) -> Self {
        Self {
            repository: NamespaceRepository::new(store.clone()),
            names: store,
        }
    }

    /// # Errors
    /// Rejects missing parents, invalid input, changed retries and unavailable storage.
    pub async fn create(&self, request: &NamespaceCreateRequest) -> Result<NamespaceOutcome, CatalogError> {
        if request.principal.is_empty() || request.principal.len() > 256 || request.principal.contains('\0') {
            return Err(ValidationError::Text.into());
        }
        let bytes = serde_json::to_vec(request.properties.entries()).map_err(|_| ValidationError::Record)?;
        let journal = NamespaceJournal::new(self.repository.store.clone());
        if let Some(existing) = journal.load(request.context, request.identity.operation).await? {
            if existing.action != NamespaceAction::Create
                || existing.identity != request.identity
                || existing.principal != request.principal
                || existing.identifier != request.identifier
                || PayloadStore::new(self.repository.store.clone())
                    .get(&existing.input)
                    .await?
                    != bytes
            {
                return Err(CatalogError::Conflict);
            }
            return self.resume(request.context, request.identity.operation).await;
        }
        let parent = match request.identifier.parent() {
            Some(identifier) => Some(
                self.repository
                    .load(request.context, &identifier)
                    .await?
                    .ok_or(ValidationError::Text)?
                    .namespace,
            ),
            None => None,
        };
        let input = crate::operation::PayloadReference {
            catalog: request.context.catalog,
            operation: request.identity.operation,
            digest: [0; 32],
            length: bytes.len(),
        };
        let mut operation = NamespaceOperation {
            context: request.context,
            identity: request.identity,
            principal: request.principal.clone(),
            action: NamespaceAction::Create,
            identifier: request.identifier.clone(),
            namespace: NamespaceId::random(),
            parent,
            phase: NamespacePhase::Prepared,
            revision: 1,
            input,
            mutation: None,
            scan_after: Vec::new(),
            scan_generation: 0,
            outcome: None,
        };
        Self::initial_authority(&operation, request.properties.clone()).encode()?;
        let response = Self::response(&operation, &request.properties)?;
        let payloads = PayloadStore::new(self.repository.store.clone());
        operation.input = payloads
            .put(request.context.catalog, request.identity.operation, &bytes)
            .await?;
        payloads
            .put(request.context.catalog, request.identity.operation, &response)
            .await?;
        journal.begin(operation).await?;
        self.resume(request.context, request.identity.operation).await
    }

    pub(super) async fn properties(
        &self,
        operation: &NamespaceOperation,
    ) -> Result<NamespaceProperties, CatalogError> {
        let bytes = PayloadStore::new(self.repository.store.clone())
            .get(&operation.input)
            .await?;
        let properties: BTreeMap<String, String> =
            serde_json::from_slice(&bytes).map_err(|_| ValidationError::Record)?;
        Ok(NamespaceProperties::new(properties)?)
    }

    pub(super) fn initial_authority(
        operation: &NamespaceOperation,
        properties: NamespaceProperties,
    ) -> StorageRecord {
        StorageRecord::NamespaceAuthority(Box::new(NamespaceAuthority {
            catalog: operation.context.catalog,
            namespace: operation.namespace,
            parent: operation.parent,
            identifier: operation.identifier.clone(),
            name_epoch: 1,
            property_revision: 1,
            admission_fence: 1,
            mutation_revision: 1,
            lifecycle: NamespaceLifecycle::Ready,
            pending_operation: Some(operation.identity.operation),
            properties,
        }))
    }

    pub(super) fn response(
        operation: &NamespaceOperation,
        properties: &NamespaceProperties,
    ) -> Result<Vec<u8>, CatalogError> {
        let bytes = serde_json::to_vec(&serde_json::json!({
            "namespace": operation.identifier.components(), "properties": properties.entries(),
        }))
        .map_err(|_| ValidationError::Record)?;
        if bytes.len() > crate::operation::MAX_PAYLOAD_BYTES {
            return Err(ValidationError::RecordTooLarge.into());
        }
        Ok(bytes)
    }
}
