use crate::{
    catalog::CatalogContext,
    commit::TableCommitOutcome,
    error::ValidationError,
    key::{CatalogScope, IcebergKey},
    namespace::{authority_key, NamespaceIdentifier, NamespaceMutation},
    operation::{PayloadReference, RequestIdentity},
    record::MAX_RECORD_BYTES,
    table::{TableHead, TableLifecycle, TableMapping, TableMappingState},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TableCreatePhase {
    Prepared,
    Reserved,
    FilesReady,
    Admitting,
    Admitted,
    Publishing,
    Published,
    Complete,
    Aborting,
    Aborted,
}

impl TableCreatePhase {
    pub(super) fn permits(self, next: Self) -> bool {
        use TableCreatePhase::{
            Aborted, Aborting, Admitted, Admitting, Complete, FilesReady, Prepared, Published, Publishing,
            Reserved,
        };
        matches!(
            (self, next),
            (Prepared, Reserved | Aborting)
                | (Reserved, FilesReady | Aborting)
                | (FilesReady, Admitting | Aborting)
                | (Admitting, Admitted | FilesReady | Aborting)
                | (Admitted, Publishing | Aborting)
                | (Publishing, Published)
                | (Published, Complete)
                | (Aborting, Aborted)
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableCreateOperation {
    pub context: CatalogContext,
    pub identity: RequestIdentity,
    pub principal: String,
    pub namespace: NamespaceIdentifier,
    pub revision: u64,
    pub timestamp_ms: i64,
    pub phase: TableCreatePhase,
    pub input: PayloadReference,
    pub document: PayloadReference,
    pub response: PayloadReference,
    pub candidate: TableHead,
    pub admission: Option<NamespaceMutation>,
    pub outcome: Option<TableCommitOutcome>,
}

impl TableCreateOperation {
    #[must_use]
    pub fn key(&self) -> IcebergKey {
        IcebergKey::Catalog {
            catalog: self.context.catalog,
            scope: CatalogScope::TableCreateOperation,
            suffix: self.identity.operation.as_bytes().to_vec(),
        }
    }

    #[must_use]
    pub fn mapping(&self, state: TableMappingState) -> TableMapping {
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

    /// # Errors
    /// Rejects foreign payloads, invalid initial heads and inconsistent durable phase evidence.
    pub fn validate(&self) -> Result<(), ValidationError> {
        use TableCreatePhase::{
            Aborted, Aborting, Admitted, Admitting, Complete, FilesReady, Prepared, Published, Publishing,
            Reserved,
        };
        self.context.validate()?;
        self.candidate.validate()?;
        self.namespace.encode()?;
        if self.principal.is_empty()
            || self.principal.len() > 256
            || self.principal.contains('\0')
            || self.revision == 0
            || self.timestamp_ms < 0
            || self.candidate.catalog != self.context.catalog
            || self.candidate.generation != 1
            || self.candidate.operation_fence != 1
            || self.candidate.name_epoch != 1
            || self.candidate.lifecycle != TableLifecycle::Ready
            || self.candidate.pending_operation != Some(self.identity.operation)
            || self.candidate.table_uuid.is_none()
            || self.document.digest != self.candidate.metadata_digest
        {
            return Err(ValidationError::Record);
        }
        for payload in [&self.input, &self.document, &self.response] {
            self.reference(payload)?;
        }
        if let Some(admission) = &self.admission {
            if admission.key != authority_key(self.context.catalog, self.candidate.namespace).encode()? {
                return Err(ValidationError::IdentityMismatch);
            }
            let before = admission.before.as_ref().ok_or(ValidationError::Record)?;
            for payload in [before, &admission.after] {
                self.reference(payload)?;
                if payload.length > MAX_RECORD_BYTES {
                    return Err(ValidationError::RecordTooLarge);
                }
            }
        }
        if (matches!(self.phase, Prepared | Reserved | FilesReady) && self.admission.is_some())
            || (matches!(
                self.phase,
                Admitting | Admitted | Publishing | Published | Complete
            ) && self.admission.is_none())
            || (matches!(self.phase, Complete | Aborting | Aborted) != self.outcome.is_some())
        {
            return Err(ValidationError::Record);
        }
        if let Some(outcome) = &self.outcome {
            self.reference(&outcome.body)?;
            if (self.phase == Complete && (outcome.status != 200 || outcome.body != self.response))
                || (matches!(self.phase, Aborting | Aborted)
                    && !matches!(outcome.status, 400 | 404 | 409 | 422))
            {
                return Err(ValidationError::Record);
            }
        }
        Ok(())
    }

    fn reference(&self, payload: &PayloadReference) -> Result<(), ValidationError> {
        payload.validate()?;
        if payload.catalog != self.context.catalog || payload.operation != self.identity.operation {
            return Err(ValidationError::IdentityMismatch);
        }
        Ok(())
    }

    pub(super) fn next(&self, phase: TableCreatePhase) -> Result<Self, ValidationError> {
        if !self.phase.permits(phase) {
            return Err(ValidationError::Record);
        }
        let mut next = self.clone();
        next.phase = phase;
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        Ok(next)
    }
}
