use std::sync::Arc;

use crate::catalog::{CatalogContext, CatalogError, CatalogStore, RootState};
use crate::error::ValidationError;
use crate::key::{IcebergKey, SystemScope};
use crate::record::StorageRecord;

use super::{authority_key, name_key, NamespaceAuthority, NamespaceIdentifier, NamespaceMappingState};

#[derive(Clone)]
pub struct NamespaceRepository {
    pub(super) store: Arc<dyn CatalogStore>,
    pub(super) names: Arc<dyn super::NamespaceStore>,
}

impl NamespaceRepository {
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
        context.validate()?;
        let key = IcebergKey::System {
            scope: SystemScope::ActiveRoot,
            suffix: Vec::new(),
        };
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(CatalogError::Uninitialized)?;
        let StorageRecord::Active(root) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if root.context != context {
            return Err(CatalogError::Conflict);
        }
        if root.state != RootState::Ready {
            return Err(CatalogError::Busy);
        }
        Ok(())
    }
}
