use crate::{
    catalog::CatalogContext,
    commit::TableCommitOutcome,
    error::ValidationError,
    key::{CatalogScope, IcebergKey},
    namespace::{NamespaceIdentifier, NamespaceMutation},
    operation::{PayloadReference, RequestIdentity},
    table::{TableHead, TableLifecycle, TableMapping, TableMappingState},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TableLifecyclePhase {
    Prepared,
    Reserved,
    Admitting,
    Publishing,
    Published,
    Complete,
    Aborting,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableLifecycleOperation {
    pub context: CatalogContext,
    pub identity: RequestIdentity,
    pub principal: String,
    pub revision: u64,
    pub phase: TableLifecyclePhase,
    pub input: PayloadReference,
    pub source: TableMapping,
    pub before: TableHead,
    pub candidate: TableHead,
    pub destination_namespace: Option<NamespaceIdentifier>,
    pub purge_requested: bool,
    pub admission: Option<NamespaceMutation>,
    pub outcome: Option<TableCommitOutcome>,
}

impl TableLifecycleOperation {
    #[must_use]
    pub fn key(&self) -> IcebergKey {
        IcebergKey::Catalog {
            catalog: self.context.catalog,
            scope: CatalogScope::TableLifecycleOperation,
            suffix: self.identity.operation.as_bytes().to_vec(),
        }
    }

    #[must_use]
    pub fn is_rename(&self) -> bool {
        self.destination_namespace.is_some()
    }

    /// # Errors
    /// Rejects foreign identities, metadata changes and inconsistent lifecycle phases.
    pub fn validate(&self) -> Result<(), ValidationError> {
        use TableLifecyclePhase::{Aborted, Aborting, Admitting, Complete, Prepared, Publishing, Reserved};
        self.context.validate()?;
        self.before.validate()?;
        self.candidate.validate()?;
        self.source.validate()?;
        self.reference(&self.input)?;
        if self.revision == 0
            || self.principal.is_empty()
            || self.principal.len() > 256
            || self.principal.contains('\0')
            || self.before.catalog != self.context.catalog
            || self.before.pending_operation.is_some()
            || !self.source.resolves(&self.before)
            || self.outcome.is_some() != matches!(self.phase, Complete | Aborting | Aborted)
        {
            return Err(ValidationError::Record);
        }
        let mut expected = self.before.clone();
        expected.operation_fence = expected
            .operation_fence
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        expected.pending_operation = Some(self.identity.operation);
        if self.is_rename() {
            expected.namespace = self.candidate.namespace;
            expected.name.clone_from(&self.candidate.name);
            expected.name_epoch = expected
                .name_epoch
                .checked_add(1)
                .ok_or(ValidationError::GenerationExhausted)?;
            if self.purge_requested
                || (expected.namespace == self.before.namespace && expected.name == self.before.name)
                || (matches!(
                    self.phase,
                    Admitting | Publishing | TableLifecyclePhase::Published | Complete
                ) && self.admission.is_none())
            {
                return Err(ValidationError::Record);
            }
        } else {
            expected.lifecycle = TableLifecycle::Tombstone;
            if self.admission.is_some() || matches!(self.phase, Reserved | Admitting) {
                return Err(ValidationError::Record);
            }
        }
        if expected != self.candidate || (self.phase == Prepared && self.admission.is_some()) {
            return Err(ValidationError::IdentityMismatch);
        }
        if let Some(admission) = &self.admission {
            if admission.key
                != crate::namespace::authority_key(self.context.catalog, self.candidate.namespace).encode()?
            {
                return Err(ValidationError::IdentityMismatch);
            }
            self.reference(admission.before.as_ref().ok_or(ValidationError::Record)?)?;
            self.reference(&admission.after)?;
        }
        if let Some(outcome) = &self.outcome {
            self.reference(&outcome.body)?;
            if (self.phase == Complete && outcome.status != 204)
                || (self.phase != Complete && !matches!(outcome.status, 404 | 409))
            {
                return Err(ValidationError::Record);
            }
        }
        Ok(())
    }

    pub(super) fn destination(&self, state: TableMappingState) -> TableMapping {
        TableMapping {
            catalog: self.context.catalog,
            namespace: self.candidate.namespace,
            name: self.candidate.name.clone(),
            table: self.candidate.table,
            name_epoch: self.candidate.name_epoch,
            operation: self.identity.operation,
            state,
        }
    }

    pub(super) fn next(&self, phase: TableLifecyclePhase) -> Result<Self, ValidationError> {
        let mut next = self.clone();
        next.revision = self
            .revision
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        next.phase = phase;
        Ok(next)
    }

    fn reference(&self, reference: &PayloadReference) -> Result<(), ValidationError> {
        reference.validate()?;
        if reference.catalog != self.context.catalog || reference.operation != self.identity.operation {
            return Err(ValidationError::IdentityMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TablePurgeTask {
    pub activation_epoch: u64,
    pub head: TableHead,
}

impl TablePurgeTask {
    #[must_use]
    pub fn key(&self) -> IcebergKey {
        let mut suffix = self.head.table.as_bytes().to_vec();
        suffix.extend_from_slice(&self.head.generation.to_be_bytes());
        suffix.extend_from_slice(self.head.metadata_file.as_bytes());
        IcebergKey::Catalog {
            catalog: self.head.catalog,
            scope: CatalogScope::Reclamation,
            suffix,
        }
    }

    /// # Errors
    /// Rejects tasks not bound to a tombstoned table and an activation epoch.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.head.validate()?;
        if self.activation_epoch == 0 || self.head.lifecycle != TableLifecycle::Tombstone {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}
