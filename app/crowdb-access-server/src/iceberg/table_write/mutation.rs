use super::super::{
    http::{bad_request, service_unavailable},
    table_read::missing_table,
};
use super::{request::Target, TableWrites};
use crowdb_access_iceberg::{
    catalog::{Capabilities, CatalogError, FormatAction},
    commit::{
        publish_direct_commit, CommitPublicationError, CommitRequest, CreateTableRequest, DirectCommitError,
        StagedCommitRequest, TableCreationOutcome, TableCreationRequest, TableRequirement,
    },
    wire::IcebergErrorResponse,
};

impl TableWrites {
    pub(super) async fn try_direct_update(
        &self,
        record: &super::TableMutation,
        capabilities: Capabilities,
        uri: &hyper::Uri,
        body: &[u8],
    ) -> Result<Option<Vec<u8>>, IcebergErrorResponse> {
        let Ok(target) = super::request::parse(uri) else {
            return Ok(None);
        };
        let Some(name) = target.name else {
            return Ok(None);
        };
        let parsed =
            CommitRequest::decode(body, self.limits.preparation.request).map_err(|_| bad_request())?;
        parsed
            .check_identifier(&target.namespace, &name)
            .map_err(|_| bad_request())?;
        if parsed
            .requirements
            .iter()
            .any(|requirement| matches!(requirement, TableRequirement::AssertCreate))
        {
            return Ok(None);
        }
        let namespace = self
            .namespaces
            .load(record.context, &target.namespace)
            .await
            .map_err(|error| storage_error(&error))?
            .ok_or_else(missing_table)?;
        let selected = self
            .tables
            .select(record.context, namespace.namespace, &name)
            .await
            .map_err(|error| storage_error(&error))?
            .ok_or_else(missing_table)?;
        let mut version = selected.head.format_version;
        let mut upgrades = parsed.upgrade_targets().peekable();
        if upgrades.peek().is_none() && !capabilities.supports(version, FormatAction::Write) {
            return Err(super::super::table_read::unsupported());
        }
        for target in upgrades {
            if !capabilities.supports_upgrade(version, target) {
                return Err(super::super::table_read::unsupported());
            }
            version = target;
        }
        let mut binding = <sha2::Sha256 as sha2::Digest>::new();
        sha2::Digest::update(&mut binding, (record.principal.len() as u64).to_be_bytes());
        sha2::Digest::update(&mut binding, record.principal.as_bytes());
        sha2::Digest::update(&mut binding, record.digest);
        let response = publish_direct_commit(
            self.store.clone(),
            self.blocks.clone(),
            record.context,
            selected.head,
            &parsed,
            record.identity,
            sha2::Digest::finalize(binding).into(),
            super::now_ms()?,
            self.limits,
        )
        .await
        .map_err(direct_error)?;
        Ok(Some(response))
    }

    pub(super) async fn mutate(
        &self,
        record: &super::TableMutation,
        capabilities: Capabilities,
        target: Target,
        body: Vec<u8>,
        now: u64,
    ) -> Result<TableCreationOutcome, IcebergErrorResponse> {
        let timestamp_ms = i64::try_from(now).map_err(|_| service_unavailable())?;
        let Some(name) = target.name else {
            let parsed = CreateTableRequest::decode(&body, self.limits.preparation.request.json)
                .map_err(|_| bad_request())?;
            if !capabilities.supports(
                parsed
                    .format_version(self.limits.preparation.request.json)
                    .map_err(|_| bad_request())?,
                FormatAction::Create,
            ) {
                return Err(super::super::table_read::unsupported());
            }
            let staged = parsed.stage_create();
            let request = TableCreationRequest {
                context: record.context,
                identity: record.identity,
                principal: record.principal.clone(),
                namespace: target.namespace,
                body,
                timestamp_ms,
            };
            if staged {
                return self.creator.stage(&request).await.map_err(creation_error);
            }
            return self.creator.create(&request).await.map_err(creation_error);
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
            if !capabilities.supports(parsed.create_version(), FormatAction::Create) {
                return Err(super::super::table_read::unsupported());
            }
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
        Err(bad_request())
    }
}

fn direct_error(error: DirectCommitError) -> IcebergErrorResponse {
    match error {
        DirectCommitError::Conflict => {
            IcebergErrorResponse::new(409, "CommitFailedException", "Table generation changed")
        }
        DirectCommitError::Uncertain => IcebergErrorResponse::new(
            503,
            "ServiceUnavailableException",
            "Table commit outcome is uncertain; reload the table",
        ),
        DirectCommitError::Evaluation(crowdb_access_iceberg::commit::EvaluationError::Requirement(
            crowdb_access_iceberg::commit::RequirementError::Failed(_),
        )) => IcebergErrorResponse::new(409, "CommitFailedException", "Table requirement failed"),
        DirectCommitError::Evaluation(_) | DirectCommitError::Metadata(_) => bad_request(),
        error => {
            tracing::error!(%error, "direct table commit failed");
            service_unavailable()
        }
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
        CommitPublicationError::TableExists => {
            IcebergErrorResponse::new(409, "AlreadyExistsException", "Table already exists")
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
            tracing::error!(%error, "table creation failed; retry complete input or reload the table");
            service_unavailable()
        }
    }
}
