use super::super::{
    http::{bad_request, service_unavailable},
    table_read::missing_table,
};
use super::{request::Target, TableWrites};
use crowdb_access_iceberg::{
    catalog::CatalogError,
    commit::{
        recover_table_commit, CommitPublicationError, CommitRequest, CreateTableRequest, StagedCommitRequest,
        TableCommitJournal, TableCommitOperation, TableCommitOutcome, TableCommitPhase, TableCreationRequest,
        TableRequirement,
    },
    operation::RetryRecord,
    wire::IcebergErrorResponse,
};

impl TableWrites {
    pub(super) async fn mutate(
        &self,
        record: &RetryRecord,
        target: Target,
        body: Vec<u8>,
        now: u64,
    ) -> Result<TableCommitOutcome, IcebergErrorResponse> {
        let timestamp_ms = i64::try_from(now).map_err(|_| service_unavailable())?;
        let Some(name) = target.name else {
            let parsed = CreateTableRequest::decode(&body, self.limits.preparation.request.json)
                .map_err(|_| bad_request())?;
            let staged = parsed.stage_create();
            let request = TableCreationRequest {
                context: record.context,
                identity: record.identity,
                principal: record.principal.clone(),
                namespace: target.namespace,
                body,
                timestamp_ms,
            };
            return if staged {
                let expiry = timestamp_ms
                    .checked_add(24 * 60 * 60 * 1000)
                    .ok_or_else(service_unavailable)?;
                self.creator.stage(&request, expiry).await
            } else {
                self.creator.create(&request).await
            }
            .map_err(creation_error);
        };
        let parsed =
            CommitRequest::decode(&body, self.limits.preparation.request).map_err(|_| bad_request())?;
        parsed
            .check_identifier(&target.namespace, &name)
            .map_err(|_| bad_request())?;
        if parsed
            .requirements
            .iter()
            .any(|requirement| matches!(requirement, TableRequirement::AssertCreate))
        {
            return self
                .creator
                .commit_staged(&StagedCommitRequest {
                    context: record.context,
                    identity: record.identity,
                    principal: record.principal.clone(),
                    namespace: target.namespace,
                    name,
                    body,
                    timestamp_ms,
                })
                .await
                .map_err(creation_error);
        }
        self.update(record, &target.namespace, &name, &body, timestamp_ms)
            .await
    }

    async fn update(
        &self,
        record: &RetryRecord,
        namespace: &crowdb_access_iceberg::namespace::NamespaceIdentifier,
        name: &str,
        body: &[u8],
        timestamp_ms: i64,
    ) -> Result<TableCommitOutcome, IcebergErrorResponse> {
        let journal = TableCommitJournal::new(self.store.clone());
        let operation = if let Some(operation) = journal
            .load(record.context, record.identity.operation)
            .await
            .map_err(|error| storage_error(&error))?
        {
            operation
        } else {
            let namespace = self
                .namespaces
                .load(record.context, namespace)
                .await
                .map_err(|error| storage_error(&error))?
                .ok_or_else(missing_table)?;
            let selected = self
                .tables
                .select(record.context, namespace.namespace, name)
                .await
                .map_err(|error| storage_error(&error))?
                .ok_or_else(missing_table)?;
            if selected.head.pending_operation.is_some() {
                return Err(service_unavailable());
            }
            let input = self
                .payloads
                .put(record.context.catalog, record.identity.operation, body)
                .await
                .map_err(|error| storage_error(&error))?;
            let initial = TableCommitOperation {
                context: record.context,
                identity: record.identity,
                principal: record.principal.clone(),
                revision: 1,
                timestamp_ms,
                phase: TableCommitPhase::Prepared,
                input,
                before: selected.head,
                candidate: None,
                outcome: None,
            };
            match journal.begin(initial).await {
                Ok(operation) => operation,
                Err(CatalogError::Conflict) => journal
                    .load(record.context, record.identity.operation)
                    .await
                    .map_err(|error| storage_error(&error))?
                    .ok_or_else(service_unavailable)?,
                Err(error) => return Err(storage_error(&error)),
            }
        };
        if operation.identity != record.identity
            || operation.principal != record.principal
            || operation.before.name != name
            || self
                .payloads
                .get(&operation.input)
                .await
                .map_err(|error| storage_error(&error))?
                != body
        {
            return Err(IcebergErrorResponse::new(
                409,
                "CommitFailedException",
                "Commit identity conflicts",
            ));
        }
        recover_table_commit(
            self.store.clone(),
            self.blocks.clone(),
            record.context,
            record.identity.operation,
            self.limits,
        )
        .await
        .map_err(|error| {
            tracing::error!(%error, "table commit remains recoverable; retry with the same request key");
            service_unavailable()
        })
    }
}

fn storage_error(error: &CatalogError) -> IcebergErrorResponse {
    tracing::error!(%error, "table mutation storage remains recoverable; retry with the same request key");
    service_unavailable()
}

fn creation_error(error: CommitPublicationError) -> IcebergErrorResponse {
    match error {
        CommitPublicationError::NamespaceMissing => {
            IcebergErrorResponse::new(404, "NoSuchNamespaceException", "Namespace does not exist")
        }
        CommitPublicationError::Unsupported(_) => super::super::table_read::unsupported(),
        CommitPublicationError::Catalog(CatalogError::Conflict) => {
            IcebergErrorResponse::new(409, "CommitFailedException", "Table creation conflicts")
        }
        CommitPublicationError::Evaluation(crowdb_access_iceberg::commit::EvaluationError::Requirement(
            crowdb_access_iceberg::commit::RequirementError::Failed(_),
        )) => IcebergErrorResponse::new(409, "CommitFailedException", "Table requirement failed"),
        CommitPublicationError::Evaluation(_)
        | CommitPublicationError::Metadata(
            crowdb_access_iceberg::table::TableMetadataError::Field(_)
            | crowdb_access_iceberg::table::TableMetadataError::Json(_)
            | crowdb_access_iceberg::table::TableMetadataError::Bounds,
        ) => bad_request(),
        error => {
            tracing::error!(%error, "table creation remains recoverable; retry with the same request key");
            service_unavailable()
        }
    }
}
