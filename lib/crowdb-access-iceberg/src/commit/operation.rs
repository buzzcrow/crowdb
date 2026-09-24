use crate::{
    catalog::CatalogContext,
    error::ValidationError,
    key::{CatalogScope, IcebergKey},
    operation::{PayloadReference, RequestIdentity},
    table::{TableHead, TableLifecycle},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TableCommitPhase {
    Prepared,
    Validated,
    Writing,
    Publishing,
    Published,
    Complete,
    Rejected,
}

impl TableCommitPhase {
    #[must_use]
    pub fn terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Rejected)
    }

    #[must_use]
    pub fn permits(self, next: Self) -> bool {
        use TableCommitPhase::{Complete, Prepared, Published, Publishing, Rejected, Validated, Writing};
        matches!(
            (self, next),
            (Prepared, Validated | Rejected)
                | (Validated, Writing | Rejected)
                | (Writing, Publishing | Rejected)
                | (Publishing, Published | Rejected)
                | (Published, Complete)
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableCommitOutcome {
    pub status: u16,
    pub body: PayloadReference,
}

/// Durable update intent, not a file-validation proof or permission to publish.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableCommitOperation {
    pub context: CatalogContext,
    pub identity: RequestIdentity,
    pub principal: String,
    pub revision: u64,
    pub timestamp_ms: i64,
    pub phase: TableCommitPhase,
    pub input: PayloadReference,
    pub before: TableHead,
    pub candidate: Option<TableHead>,
    pub outcome: Option<TableCommitOutcome>,
}

impl TableCommitOperation {
    #[must_use]
    pub fn key(&self) -> IcebergKey {
        IcebergKey::Catalog {
            catalog: self.context.catalog,
            scope: CatalogScope::TableCommitOperation,
            suffix: self.identity.operation.as_bytes().to_vec(),
        }
    }

    /// # Errors
    /// Rejects invalid phases, foreign payloads and candidate identity or generation changes.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.context.validate()?;
        self.before.validate()?;
        self.reference(&self.input)?;
        if self.principal.is_empty()
            || self.principal.len() > 256
            || self.principal.contains('\0')
            || self.revision == 0
            || self.timestamp_ms < 0
            || self.before.catalog != self.context.catalog
            || self.before.lifecycle != TableLifecycle::Ready
            || self.before.pending_operation.is_some()
            || (self.phase.terminal() != self.outcome.is_some())
            || (self.phase == TableCommitPhase::Prepared && self.candidate.is_some())
            || (!matches!(
                self.phase,
                TableCommitPhase::Prepared | TableCommitPhase::Rejected
            ) && self.candidate.is_none())
        {
            return Err(ValidationError::Record);
        }
        if let Some(candidate) = &self.candidate {
            self.validate_candidate(candidate)?;
        }
        if let Some(outcome) = &self.outcome {
            self.reference(&outcome.body)?;
            let valid = match self.phase {
                TableCommitPhase::Complete => outcome.status == 200,
                TableCommitPhase::Rejected => matches!(outcome.status, 400 | 403 | 404 | 406 | 409 | 422),
                _ => false,
            };
            if !valid {
                return Err(ValidationError::Record);
            }
        }
        Ok(())
    }

    pub(super) fn same_request(&self, other: &Self) -> bool {
        self.context == other.context
            && self.identity == other.identity
            && self.principal == other.principal
            && self.input == other.input
            && self.before == other.before
            && self.timestamp_ms == other.timestamp_ms
    }

    fn reference(&self, reference: &PayloadReference) -> Result<(), ValidationError> {
        reference.validate()?;
        if reference.catalog != self.context.catalog || reference.operation != self.identity.operation {
            return Err(ValidationError::IdentityMismatch);
        }
        Ok(())
    }

    fn validate_candidate(&self, candidate: &TableHead) -> Result<(), ValidationError> {
        candidate.validate()?;
        let before = &self.before;
        if candidate.catalog != before.catalog
            || candidate.table != before.table
            || candidate.namespace != before.namespace
            || candidate.name != before.name
            || candidate.name_epoch != before.name_epoch
            || candidate.lifecycle != TableLifecycle::Ready
            || before.generation.checked_add(1) != Some(candidate.generation)
            || before.operation_fence.checked_add(1) != Some(candidate.operation_fence)
            || candidate.pending_operation != Some(self.identity.operation)
            || candidate.metadata_file == before.metadata_file
            || candidate.metadata_location == before.metadata_location
            || candidate.format_version < before.format_version
            || before
                .table_uuid
                .is_some_and(|uuid| candidate.table_uuid != Some(uuid))
        {
            return Err(ValidationError::IdentityMismatch);
        }
        Ok(())
    }
}
