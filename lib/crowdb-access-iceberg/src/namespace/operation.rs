use crate::catalog::CatalogContext;
use crate::error::ValidationError;
use crate::key::{CatalogScope, IcebergKey, NamespaceId, SystemScope, MAX_KEY_BYTES};
use crate::operation::{PayloadReference, RequestIdentity};
use crate::record::MAX_RECORD_BYTES;

use super::NamespaceIdentifier;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum NamespaceAction {
    Create,
    Update,
    Drop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum NamespacePhase {
    Prepared,
    Reserved,
    Admitting,
    Admitted,
    Publishing,
    Published,
    Fencing,
    ProbingNamespaces,
    ProbingTables,
    Restoring,
    Tombstoning,
    Aborting,
    Complete,
    Aborted,
}

impl NamespacePhase {
    #[must_use]
    pub fn terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Aborted)
    }

    #[must_use]
    pub fn permits(self, next: Self, action: NamespaceAction) -> bool {
        use NamespacePhase::{
            Aborted, Aborting, Admitted, Admitting, Complete, Fencing, Prepared, ProbingNamespaces,
            ProbingTables, Published, Publishing, Reserved, Restoring, Tombstoning,
        };
        match action {
            NamespaceAction::Create => matches!(
                (self, next),
                (Prepared, Reserved | Aborting)
                    | (Reserved, Admitting | Aborting)
                    | (Admitting, Admitted | Aborting | Reserved)
                    | (Admitted, Publishing | Aborting)
                    | (Publishing, Published)
                    | (Published, Complete)
                    | (Aborting, Aborted)
            ),
            NamespaceAction::Update => matches!(
                (self, next),
                (Prepared, Publishing | Aborting | Complete)
                    | (Publishing, Published | Prepared)
                    | (Published, Complete)
                    | (Aborting, Aborted)
            ),
            NamespaceAction::Drop => matches!(
                (self, next),
                (Prepared, Fencing | Aborting | Complete)
                    | (Fencing, ProbingNamespaces | Prepared | Complete)
                    | (ProbingNamespaces, ProbingNamespaces | ProbingTables | Restoring)
                    | (ProbingTables, ProbingTables | Tombstoning | Restoring)
                    | (Tombstoning | Restoring, Complete)
                    | (Aborting, Aborted)
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceMutation {
    pub key: Vec<u8>,
    pub before: Option<PayloadReference>,
    pub after: PayloadReference,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceOutcome {
    pub status: u16,
    pub body: PayloadReference,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceOperation {
    pub context: CatalogContext,
    pub identity: RequestIdentity,
    pub principal: String,
    pub action: NamespaceAction,
    pub identifier: NamespaceIdentifier,
    pub namespace: NamespaceId,
    pub parent: Option<NamespaceId>,
    pub phase: NamespacePhase,
    pub revision: u64,
    pub input: PayloadReference,
    pub mutation: Option<NamespaceMutation>,
    pub scan_after: Vec<u8>,
    pub scan_generation: u64,
    pub outcome: Option<NamespaceOutcome>,
}

impl NamespaceOperation {
    #[must_use]
    pub fn key(&self) -> IcebergKey {
        IcebergKey::Catalog {
            catalog: self.context.catalog,
            scope: CatalogScope::NamespaceOperation,
            suffix: self.identity.operation.as_bytes().to_vec(),
        }
    }

    /// # Errors
    /// Rejects invalid phases, identities, payload domains and mutation ranges.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.context.validate()?;
        if self.principal.is_empty()
            || self.principal.len() > 256
            || self.principal.contains('\0')
            || self.revision == 0
            || self.parent.is_some() != self.identifier.parent().is_some()
            || self.parent == Some(self.namespace)
            || self.scan_after.len() > MAX_KEY_BYTES
            || (self.phase.terminal() && self.outcome.is_none())
            || (!self.phase.terminal() && self.phase != NamespacePhase::Aborting && self.outcome.is_some())
        {
            return Err(ValidationError::Record);
        }
        self.validate_phase()?;
        self.validate_reference(&self.input)?;
        if let Some(mutation) = &self.mutation {
            self.validate_reference(&mutation.after)?;
            if mutation.after.length > MAX_RECORD_BYTES {
                return Err(ValidationError::RecordTooLarge);
            }
            if let Some(before) = &mutation.before {
                self.validate_reference(before)?;
                if before.length > MAX_RECORD_BYTES {
                    return Err(ValidationError::RecordTooLarge);
                }
            }
            match IcebergKey::decode(&mutation.key)? {
                IcebergKey::Catalog {
                    catalog,
                    scope: CatalogScope::NamespaceAuthority,
                    suffix,
                } if catalog == self.context.catalog
                    && (suffix == self.namespace.as_bytes()
                        || (self.action == NamespaceAction::Create
                            && self.parent.is_some_and(|parent| suffix == parent.as_bytes()))) => {}
                key @ IcebergKey::Catalog {
                    scope: CatalogScope::NamespaceName,
                    ..
                } if key == super::name_key(self.context.catalog, self.parent, self.identifier.name())? => {}
                IcebergKey::System {
                    scope: SystemScope::ActiveRoot,
                    ..
                } if self.action == NamespaceAction::Create && self.parent.is_none() => {}
                _ => return Err(ValidationError::IdentityMismatch),
            }
        }
        if let Some(outcome) = &self.outcome {
            self.validate_reference(&outcome.body)?;
            if !matches!(
                outcome.status,
                200 | 201 | 204 | 400 | 403 | 404 | 406 | 409 | 422
            ) || (matches!(self.phase, NamespacePhase::Aborting | NamespacePhase::Aborted)
                && outcome.status < 400)
            {
                return Err(ValidationError::Record);
            }
        }
        Ok(())
    }

    pub(super) fn same_request(&self, other: &Self) -> bool {
        self.context == other.context
            && self.identity == other.identity
            && self.principal == other.principal
            && self.action == other.action
            && self.identifier == other.identifier
            && self.input == other.input
    }

    fn validate_reference(&self, reference: &PayloadReference) -> Result<(), ValidationError> {
        reference.validate()?;
        if reference.catalog != self.context.catalog || reference.operation != self.identity.operation {
            return Err(ValidationError::IdentityMismatch);
        }
        Ok(())
    }

    fn validate_phase(&self) -> Result<(), ValidationError> {
        use NamespacePhase::{
            Aborted, Aborting, Admitted, Admitting, Complete, Fencing, Prepared, ProbingNamespaces,
            ProbingTables, Published, Publishing, Reserved, Restoring, Tombstoning,
        };
        let valid = match self.action {
            NamespaceAction::Create => matches!(
                self.phase,
                Prepared
                    | Reserved
                    | Admitting
                    | Admitted
                    | Publishing
                    | Published
                    | Aborting
                    | Aborted
                    | Complete
            ),
            NamespaceAction::Update => matches!(
                self.phase,
                Prepared | Publishing | Published | Aborting | Aborted | Complete
            ),
            NamespaceAction::Drop => matches!(
                self.phase,
                Prepared
                    | Fencing
                    | ProbingNamespaces
                    | ProbingTables
                    | Restoring
                    | Tombstoning
                    | Aborting
                    | Aborted
                    | Complete
            ),
        };
        if !valid
            || (!self.scan_after.is_empty()
                && (self.action != NamespaceAction::Drop || self.scan_generation == 0))
        {
            return Err(ValidationError::Record);
        }
        if !self.scan_after.is_empty() {
            if !matches!(self.phase, ProbingNamespaces | ProbingTables) {
                return Err(ValidationError::Record);
            }
            IcebergKey::decode(&self.scan_after)?;
            let scope = if self.phase == ProbingNamespaces {
                CatalogScope::NamespaceName
            } else {
                CatalogScope::TableName
            };
            if !super::child_range(self.context.catalog, Some(self.namespace), scope)?
                .contains(&self.scan_after)
            {
                return Err(ValidationError::Key);
            }
        }
        Ok(())
    }
}
