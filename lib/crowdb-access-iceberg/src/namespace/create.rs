use std::collections::BTreeMap;
use std::sync::Arc;

use crate::catalog::{CatalogContext, CatalogError};
use crate::error::ValidationError;
use crate::operation::{PayloadStore, RequestIdentity};
use crate::record::StorageRecord;

use super::{
    NamespaceAuthority, NamespaceIdentifier, NamespaceLifecycle, NamespaceOperation, NamespaceProperties,
    NamespaceRepository, NamespaceStore,
};

mod resource;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceCreationOutcome {
    pub status: u16,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct NamespaceCreateRequest {
    pub context: CatalogContext,
    pub identity: RequestIdentity,
    pub principal: String,
    pub identifier: NamespaceIdentifier,
    pub properties: NamespaceProperties,
}

#[derive(Clone)]
pub struct NamespaceCreator {
    pub(super) repository: NamespaceRepository,
    pub(super) names: Arc<dyn NamespaceStore>,
}

impl NamespaceCreator {
    pub(crate) fn from_parts(
        store: Arc<dyn crate::catalog::CatalogStore>,
        names: Arc<dyn NamespaceStore>,
    ) -> Self {
        Self {
            repository: NamespaceRepository::from_parts(store, names.clone()),
            names,
        }
    }
    #[must_use]
    pub fn new<Store: NamespaceStore + 'static>(store: Arc<Store>) -> Self {
        Self {
            repository: NamespaceRepository::new(store.clone()),
            names: store,
        }
    }

    /// # Errors
    /// Rejects invalid input, missing parents and unavailable storage. Retries use current resources.
    pub async fn create(
        &self,
        request: &NamespaceCreateRequest,
    ) -> Result<NamespaceCreationOutcome, CatalogError> {
        self.create_resource(request).await
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
            creating_child: None,
            rename: None,
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
