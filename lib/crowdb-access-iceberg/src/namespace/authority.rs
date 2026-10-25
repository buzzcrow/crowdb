use crate::error::ValidationError;
use crate::key::{CatalogId, NamespaceId, OperationId};

use super::{NamespaceIdentifier, NamespaceProperties};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamespaceLifecycle {
    Ready,
    Dropping,
    Tombstone,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceAuthority {
    pub catalog: CatalogId,
    pub namespace: NamespaceId,
    pub parent: Option<NamespaceId>,
    pub identifier: NamespaceIdentifier,
    pub name_epoch: u64,
    pub property_revision: u64,
    pub admission_fence: u64,
    pub mutation_revision: u64,
    pub lifecycle: NamespaceLifecycle,
    pub pending_operation: Option<OperationId>,
    pub creating_child: Option<NamespaceId>,
    pub rename: Option<Box<super::NamespaceRename>>,
    pub properties: NamespaceProperties,
}

impl NamespaceAuthority {
    /// # Errors
    /// Rejects zero revisions, invalid parent shape and unfenced lifecycle state.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.name_epoch == 0
            || self.property_revision == 0
            || self.admission_fence == 0
            || self.mutation_revision == 0
            || self.parent.is_some() != self.identifier.parent().is_some()
            || self.parent == Some(self.namespace)
            || (self.lifecycle != NamespaceLifecycle::Ready && self.pending_operation.is_none())
            || self.creating_child.is_some_and(|child| {
                child == self.namespace
                    || self.lifecycle != NamespaceLifecycle::Ready
                    || self.pending_operation.is_some()
                    || self.rename.is_some()
            })
        {
            return Err(ValidationError::Record);
        }
        if let Some(rename) = &self.rename {
            rename.validate()?;
            if self.lifecycle != NamespaceLifecycle::Ready
                || self.pending_operation.is_some()
                || rename.source.catalog != self.catalog
                || (rename.source.namespace != self.namespace
                    && rename.destination_namespace != self.namespace)
            {
                return Err(ValidationError::IdentityMismatch);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamespaceMappingState {
    Reserved,
    ReservedResource,
    Published,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceMapping {
    pub catalog: CatalogId,
    pub parent: Option<NamespaceId>,
    pub name: String,
    pub namespace: NamespaceId,
    pub name_epoch: u64,
    pub operation: OperationId,
    pub state: NamespaceMappingState,
}

impl NamespaceMapping {
    /// # Errors
    /// Rejects unrepresentable names, zero epochs and self-parenting mappings.
    pub fn validate(&self) -> Result<(), ValidationError> {
        super::name_key(self.catalog, self.parent, &self.name)?;
        if self.name_epoch == 0 || self.parent == Some(self.namespace) {
            return Err(ValidationError::Record);
        }
        Ok(())
    }

    #[must_use]
    pub fn resolves(&self, authority: &NamespaceAuthority) -> bool {
        self.state == NamespaceMappingState::Published
            && self.catalog == authority.catalog
            && self.parent == authority.parent
            && self.namespace == authority.namespace
            && self.name_epoch == authority.name_epoch
            && self.name == authority.identifier.name()
            && authority.lifecycle != NamespaceLifecycle::Tombstone
    }
}
