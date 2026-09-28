use super::{Error, Phase, TableCreateOperation, TableCreationRequest, TableCreator};
use crate::{
    catalog::{check_context, CatalogContext, CatalogError},
    commit::{CandidateAuxiliaryLimits, CandidateSnapshotLimits, EvaluationLimits, TableCommitOutcome},
    error::ValidationError,
    key::{OperationId, TableId},
    namespace::NamespaceIdentifier,
    operation::{RequestIdentity, MAX_PAYLOAD_BYTES},
};

mod binding;
mod proof;
pub(super) use proof::definite_validation_failure;

#[derive(Clone, Copy, Debug)]
pub struct StagedCommitLimits {
    pub evaluation: EvaluationLimits,
    pub snapshots: CandidateSnapshotLimits,
    pub auxiliary: CandidateAuxiliaryLimits,
}

#[derive(Clone, Debug)]
pub struct StagedCommitRequest {
    pub context: CatalogContext,
    pub identity: RequestIdentity,
    pub principal: String,
    pub namespace: NamespaceIdentifier,
    pub name: String,
    pub body: Vec<u8>,
    pub timestamp_ms: i64,
}

impl TableCreator {
    #[must_use]
    pub fn with_staged_limits(mut self, limits: StagedCommitLimits) -> Self {
        self.staged_limits = Some(std::sync::Arc::new(limits));
        self
    }

    /// Retains a draft and exact metadata-only response without publishing a name or head.
    /// The server supplies the expiry; it is not a nonstandard field required from the SDK.
    /// # Errors
    /// Rejects unsupported staging, invalid input, existing tables and changed retry input.
    pub async fn stage(
        &self,
        request: &TableCreationRequest,
        expires_ms: i64,
    ) -> Result<TableCommitOutcome, Error> {
        if self.staged_limits.is_none() {
            return Err(Error::Unsupported("staged commit file limits"));
        }
        if request.principal.is_empty() || request.principal.len() > 256 || request.principal.contains('\0') {
            return Err(ValidationError::Text.into());
        }
        let journal = self.journal();
        let operation =
            if let Some(existing) = journal.load(request.context, request.identity.operation).await? {
                if existing.identity != request.identity
                    || existing.principal != request.principal
                    || existing.namespace != request.namespace
                    || self.payloads().get(&existing.input).await? != request.body
                {
                    return Err(CatalogError::Conflict.into());
                }
                existing
            } else {
                journal
                    .begin(self.prepare(request, Some(expires_ms)).await?)
                    .await?
            };
        let stage = operation.stage.ok_or(CatalogError::Conflict)?;
        self.payloads().get(&stage.response).await?;
        check_context(self.store.as_ref(), request.context).await?;
        Ok(TableCommitOutcome {
            status: 200,
            body: stage.response,
        })
    }

    /// Expires only an unbound draft by phase CAS; bound publication is never deleted by time.
    /// # Errors
    /// Storage uncertainty remains retryable and cannot be interpreted as an expired publisher.
    pub async fn expire_stage(
        &self,
        context: CatalogContext,
        table: TableId,
        now_ms: i64,
    ) -> Result<bool, Error> {
        let identity = OperationId::from_bytes(table.as_bytes())?;
        let operation = self
            .journal()
            .load(context, identity)
            .await?
            .ok_or(CatalogError::Conflict)?;
        let stage = operation.stage.as_ref().ok_or(CatalogError::Conflict)?;
        if operation.candidate.table != table || now_ms < 0 {
            return Err(ValidationError::IdentityMismatch.into());
        }
        if operation.phase != Phase::Staged || stage.expires_ms > now_ms {
            return Ok(false);
        }
        let mut next = operation.next(Phase::Aborted)?;
        let body = self
            .payloads()
            .put(
                context.catalog,
                identity,
                br#"{"error":{"code":404,"type":"NoSuchTableException","message":"Staged table expired"}}"#,
            )
            .await?;
        next.outcome = Some(TableCommitOutcome { status: 404, body });
        self.journal().advance(&operation, &next).await?;
        Ok(true)
    }
}

pub(super) fn stage_response(canonical: &[u8]) -> Result<Vec<u8>, ValidationError> {
    let length = canonical
        .len()
        .checked_add(b"{\"metadata\":}".len())
        .filter(|length| *length <= MAX_PAYLOAD_BYTES)
        .ok_or(ValidationError::RecordTooLarge)?;
    let mut response = Vec::with_capacity(length);
    response.extend_from_slice(b"{\"metadata\":");
    response.extend_from_slice(canonical);
    response.push(b'}');
    Ok(response)
}
