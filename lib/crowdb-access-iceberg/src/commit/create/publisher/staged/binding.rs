use super::{Error, StagedCommitRequest, TableCreationOutcome, TableCreationRequest, TableCreator};
use crate::{
    catalog::{check_context, CatalogStorageLayout},
    commit::{evaluate_table_create_commit, CommitRequest, CommitRequestLimits, TableUpdate},
    error::ValidationError,
    file::TableLocation,
};
use sha2::{Digest, Sha256};

impl TableCreator {
    /// Publishes the complete standard assert-create input without consulting a draft.
    /// # Errors
    /// Missing initialization, foreign storage identity and uncertain resource writes stop this request.
    pub async fn commit_staged(&self, request: &StagedCommitRequest) -> Result<TableCreationOutcome, Error> {
        super::super::direct::validate_principal(&request.principal)?;
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
        let uuid = decoded
            .updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::AssignUuid { uuid } => Some(uuid),
                _ => None,
            })
            .ok_or(ValidationError::IdentityMismatch)?;
        let uuid = uuid::Uuid::parse_str(uuid).map_err(|_| ValidationError::IdentityMismatch)?;
        let creation = TableCreationRequest {
            context: request.context,
            identity: request.identity,
            principal: request.principal.clone(),
            namespace: request.namespace.clone(),
            body: Vec::new(),
            timestamp_ms: request.timestamp_ms,
        };
        let layout = self.storage_layout(request.context).await?;
        let parent = self.direct_parent(&creation, layout).await?;
        if table.catalog != request.context.catalog
            || table.storage_namespace != (layout == CatalogStorageLayout::Native).then_some(parent.namespace)
        {
            return Err(ValidationError::IdentityMismatch.into());
        }
        let mut target = super::super::direct::target(&creation, parent.namespace, &request.name, layout)?;
        target.table = table.table;
        target.metadata_location =
            table.file(&format!("metadata/1-{}.metadata.json", target.metadata_file))?;
        target.table_uuid = Some(uuid);
        let evaluated =
            evaluate_table_create_commit(&decoded, target, request.timestamp_ms, limits.evaluation)?;
        let body =
            crate::commit::publication::metadata_response(&evaluated.head, evaluated.document.canonical())?;
        self.validate_response_size(&body)?;
        crate::commit::publication::write_metadata_file(
            self.store.clone(),
            self.blocks.clone().ok_or(crate::catalog::CatalogError::Busy)?,
            request.context,
            &evaluated.document,
        )
        .await?;
        let reservation = reservation(request, parent.namespace, table)?;
        self.publish_direct_creation(request.context, &parent, evaluated.head, reservation)
            .await?;
        check_context(self.store.as_ref(), request.context).await?;
        Ok(TableCreationOutcome { status: 200, body })
    }
}

fn reservation(
    request: &StagedCommitRequest,
    namespace: crate::key::NamespaceId,
    table: TableLocation,
) -> Result<crate::key::OperationId, ValidationError> {
    let mut digest = Sha256::new();
    digest.update(b"CROWDB\0ICEBERG\0ASSERT-CREATE-RESERVATION\0v1");
    for field in [
        request.context.catalog.as_bytes().as_slice(),
        &request.context.activation_epoch.to_be_bytes(),
        namespace.as_bytes().as_slice(),
        table.table.as_bytes().as_slice(),
        request.name.as_bytes(),
        request.principal.as_bytes(),
        request.body.as_slice(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    crate::key::OperationId::from_bytes(&digest.finalize()[..16])
}
