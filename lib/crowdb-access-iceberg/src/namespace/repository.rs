use std::sync::Arc;

use crate::catalog::{CatalogContext, CatalogError, CatalogStore};
use crate::error::ValidationError;
use crate::record::StorageRecord;

use super::{authority_key, name_key, NamespaceAuthority, NamespaceIdentifier, NamespaceMappingState};

#[derive(Clone)]
pub struct NamespaceRepository {
    pub(super) store: Arc<dyn CatalogStore>,
    pub(super) names: Arc<dyn super::NamespaceStore>,
}

impl NamespaceRepository {
    pub(crate) async fn ensure_default(
        &self,
        context: CatalogContext,
        issued_ms: u64,
    ) -> Result<NamespaceAuthority, CatalogError> {
        let identifier = NamespaceIdentifier::new(vec!["default".into()])?;
        if let Some(authority) = self.load(context, &identifier).await? {
            return Ok(authority);
        }
        let outcome = super::NamespaceCreator::from_parts(self.store.clone(), self.names.clone())
            .create(&super::NamespaceCreateRequest {
                context,
                identity: crate::operation::RequestIdentity {
                    operation: crate::key::OperationId::random(),
                    issued_ms,
                },
                principal: "native-default-namespace".into(),
                identifier: identifier.clone(),
                properties: super::NamespaceProperties::default(),
            })
            .await;
        if let Some(authority) = self.load(context, &identifier).await? {
            Ok(authority)
        } else {
            outcome?;
            Err(CatalogError::Busy)
        }
    }
    pub(crate) fn from_parts(store: Arc<dyn CatalogStore>, names: Arc<dyn super::NamespaceStore>) -> Self {
        Self { store, names }
    }
    pub(super) async fn cleanup_marker(
        &self,
        key: &[u8],
        before: &[u8],
        after: &[u8],
    ) -> Result<(), CatalogError> {
        if self
            .store
            .get(key)
            .await?
            .as_ref()
            .map(|value| value.bytes.as_slice())
            != Some(before)
        {
            return Ok(());
        }
        self.store
            .compare_exchange(
                key,
                Some(before),
                after,
                crate::operation::mutation_identity(key, Some(before), after),
            )
            .await?;
        Ok(())
    }
    #[must_use]
    pub fn new<Store: super::NamespaceStore + 'static>(store: Arc<Store>) -> Self {
        Self {
            store: store.clone(),
            names: store,
        }
    }

    /// # Errors
    /// Rejects retired contexts, unavailable storage and corrupt records.
    /// Missing, unpublished or stale mappings return no namespace.
    pub async fn load(
        &self,
        context: CatalogContext,
        identifier: &NamespaceIdentifier,
    ) -> Result<Option<NamespaceAuthority>, CatalogError> {
        self.check_context(context).await?;
        let result = self.resolve(context, identifier).await?;
        self.check_context(context).await?;
        Ok(result)
    }

    /// # Errors
    /// Propagates corruption and context errors rather than treating them as absence.
    pub async fn exists(
        &self,
        context: CatalogContext,
        identifier: &NamespaceIdentifier,
    ) -> Result<bool, CatalogError> {
        Ok(self.load(context, identifier).await?.is_some())
    }

    pub(crate) async fn ensure_current(
        &self,
        context: CatalogContext,
        selected: &NamespaceAuthority,
    ) -> Result<(), CatalogError> {
        if selected.catalog != context.catalog {
            return Err(ValidationError::IdentityMismatch.into());
        }
        let key = name_key(context.catalog, selected.parent, selected.identifier.name())?;
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(CatalogError::Conflict)?;
        let StorageRecord::NamespaceMapping(mapping) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        let key = authority_key(context.catalog, selected.namespace);
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(CatalogError::Conflict)?;
        let StorageRecord::NamespaceAuthority(current) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        // Namespaces cannot move, and a live child prevents every ancestor's
        // deletion. Recheck the selected leaf and its name within this request.
        if !mapping.resolves(&current)
            || current.namespace != selected.namespace
            || current.name_epoch != selected.name_epoch
            || current.identifier != selected.identifier
            || current.parent != selected.parent
        {
            return Err(CatalogError::Conflict);
        }
        self.check_context(context).await
    }

    async fn resolve(
        &self,
        context: CatalogContext,
        identifier: &NamespaceIdentifier,
    ) -> Result<Option<NamespaceAuthority>, CatalogError> {
        let mut parent = None;
        let mut selected = None;
        for (index, name) in identifier.components().iter().enumerate() {
            let key = name_key(context.catalog, parent, name)?;
            let Some(value) = self.store.get(&key.encode()?).await? else {
                return Ok(None);
            };
            let StorageRecord::NamespaceMapping(mapping) = StorageRecord::decode(&key, &value.bytes)? else {
                return Err(ValidationError::Record.into());
            };
            if mapping.state != NamespaceMappingState::Published {
                return Ok(None);
            }
            let key = authority_key(context.catalog, mapping.namespace);
            let Some(value) = self.store.get(&key.encode()?).await? else {
                return Ok(None);
            };
            let StorageRecord::NamespaceAuthority(authority) = StorageRecord::decode(&key, &value.bytes)?
            else {
                return Err(ValidationError::Record.into());
            };
            if !mapping.resolves(&authority)
                || authority.identifier.components() != &identifier.components()[..=index]
            {
                return Ok(None);
            }
            parent = Some(authority.namespace);
            selected = Some(*authority);
        }
        Ok(selected)
    }

    pub(super) async fn check_context(&self, context: CatalogContext) -> Result<(), CatalogError> {
        crate::catalog::check_context(self.store.as_ref(), context).await
    }
}
