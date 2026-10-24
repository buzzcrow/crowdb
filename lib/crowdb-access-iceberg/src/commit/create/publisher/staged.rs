use super::{limits, Error, TableCreationOutcome, TableCreationRequest, TableCreator};
use crate::{
    catalog::{check_context, CatalogContext, CatalogError},
    commit::{
        evaluate_table_creation, CandidateAuxiliaryLimits, CandidateSnapshotLimits, CreateTableRequest,
        EvaluationLimits,
    },
    error::ValidationError,
    namespace::NamespaceIdentifier,
    operation::{RequestIdentity, MAX_PAYLOAD_BYTES},
    table::TableRepository,
};

mod binding;

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

    /// Returns complete client-owned initialization material without storing a draft.
    /// # Errors
    /// Rejects invalid input, missing namespace admission, existing tables and response overflow.
    pub async fn stage(&self, request: &TableCreationRequest) -> Result<TableCreationOutcome, Error> {
        if self.staged_limits.is_none() {
            return Err(Error::Unsupported("staged commit file limits"));
        }
        super::direct::validate_principal(&request.principal)?;
        let mut decoded = CreateTableRequest::decode(&request.body, limits())?;
        if !decoded.stage_create() {
            return Err(Error::Unsupported("staged table creation"));
        }
        let layout = self.storage_layout(request.context).await?;
        let parent = self.direct_parent(request, layout).await?;
        if TableRepository::new(self.store.clone())
            .select(request.context, parent.namespace, decoded.name())
            .await?
            .is_some()
        {
            return Err(CatalogError::Conflict.into());
        }
        let created = chrono::DateTime::from_timestamp_millis(request.timestamp_ms)
            .ok_or(ValidationError::Record)?
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        if !decoded.fields["properties"].is_object() {
            decoded.fields["properties"] = serde_json::json!({});
        }
        decoded.fields["properties"]
            .as_object_mut()
            .ok_or(ValidationError::Record)?
            .entry("created-at")
            .or_insert(serde_json::Value::String(created));
        let target = super::direct::target(request, parent.namespace, decoded.name(), layout)?;
        let initial = evaluate_table_creation(&decoded, target, request.timestamp_ms, limits())?;
        let body = stage_response(initial.document.canonical())?;
        self.validate_response_size(&body)?;
        check_context(self.store.as_ref(), request.context).await?;
        Ok(TableCreationOutcome { status: 200, body })
    }
}

fn stage_response(canonical: &[u8]) -> Result<Vec<u8>, ValidationError> {
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
