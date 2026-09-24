use super::{Error, Phase, StagedCommitRequest, TableCreateOperation, TableCreator};
use crate::{
    catalog::CatalogError,
    commit::{
        evaluate_table_create_commit, CommitRequest, CommitRequestLimits, TableCommitOutcome,
        TableStageBinding, TableUpdate,
    },
    error::ValidationError,
    file::TableLocation,
    key::OperationId,
};

impl TableCreator {
    /// Binds one standard assert-create request and resumes the existing initial-head publisher.
    /// # Errors
    /// Rejects foreign/expired drafts, changed identities or bodies, and unvalidated file publication.
    pub async fn commit_staged(&self, request: &StagedCommitRequest) -> Result<TableCommitOutcome, Error> {
        let limits = self
            .staged_limits
            .as_ref()
            .ok_or(Error::Unsupported("staged commit file limits"))?;
        let decoded = CommitRequest::decode(
            &request.body,
            CommitRequestLimits {
                json: limits.evaluation.metadata,
                requirements: limits.evaluation.requirements.count,
                updates: limits.evaluation.updates,
            },
        )?;
        decoded.check_identifier(&request.namespace, &request.name)?;
        let location = decoded
            .updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::SetLocation { location } => Some(location),
                _ => None,
            })
            .ok_or(ValidationError::IdentityMismatch)?;
        let table: TableLocation = format!("{}/", location.trim_end_matches('/')).parse()?;
        if table.catalog != request.context.catalog {
            return Err(ValidationError::IdentityMismatch.into());
        }
        let identity = OperationId::from_bytes(table.table.as_bytes())?;
        let operation = self
            .journal()
            .load(request.context, identity)
            .await?
            .ok_or(CatalogError::Conflict)?;
        if operation.principal != request.principal
            || operation.namespace != request.namespace
            || operation.candidate.name != request.name
            || operation.candidate.table != table.table
        {
            return Err(CatalogError::Conflict.into());
        }
        let stage = operation.stage.as_ref().ok_or(CatalogError::Conflict)?;
        if let Some(binding) = &stage.binding {
            if binding.identity != request.identity
                || self.payloads().get(&binding.input).await? != request.body
            {
                return Err(CatalogError::Conflict.into());
            }
        } else {
            if operation.phase != Phase::Staged
                || request.timestamp_ms >= stage.expires_ms
                || request.timestamp_ms < stage.created_ms
                || request.identity.operation == identity
            {
                return Err(CatalogError::Conflict.into());
            }
            let evaluated = evaluate_table_create_commit(
                &decoded,
                operation.candidate.clone(),
                request.timestamp_ms,
                limits.evaluation,
            )?;
            if evaluated
                .document
                .fields()
                .get("partition-statistics")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|entries| !entries.is_empty())
            {
                return Err(Error::Unsupported("partition statistics selected-use validation"));
            }
            self.bind_stage(&operation, request, evaluated).await?;
        }
        self.resume(request.context, identity).await
    }

    async fn bind_stage(
        &self,
        operation: &TableCreateOperation,
        request: &StagedCommitRequest,
        evaluated: crate::commit::EvaluatedMetadata,
    ) -> Result<(), Error> {
        let response =
            crate::commit::publication::metadata_response(&evaluated.head, evaluated.document.canonical())?;
        let payloads = self.payloads();
        let identity = operation.identity.operation;
        let catalog = operation.context.catalog;
        let mut next = operation.next(Phase::Prepared)?;
        next.timestamp_ms = request.timestamp_ms;
        next.document = payloads
            .put(catalog, identity, evaluated.document.canonical())
            .await?;
        next.response = payloads.put(catalog, identity, &response).await?;
        next.candidate = evaluated.head;
        next.stage.as_mut().ok_or(ValidationError::Record)?.binding = Some(TableStageBinding {
            identity: request.identity,
            input: payloads.put(catalog, identity, &request.body).await?,
        });
        self.journal().advance(operation, &next).await?;
        Ok(())
    }
}
