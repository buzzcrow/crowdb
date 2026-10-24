use super::{limits, Error, TableCreationOutcome, TableCreationRequest, TableCreator};
use crate::{
    catalog::{check_context, CatalogStorageLayout},
    commit::{evaluate_table_creation, CreateTableRequest},
    error::ValidationError,
    file::TableLocation,
    key::{FileId, NamespaceId, TableId},
    namespace::{NamespaceAuthority, NamespaceLifecycle, NamespaceRepository},
    table::{TableHead, TableLifecycle},
};

mod resources;

impl TableCreator {
    /// Publishes complete request input without retaining a process or response record.
    /// # Errors
    /// Unknown writes stop the request; clients retry complete input or reload the table name.
    pub async fn create(&self, request: &TableCreationRequest) -> Result<TableCreationOutcome, Error> {
        validate_principal(&request.principal)?;
        let decoded = CreateTableRequest::decode(&request.body, limits())?;
        if decoded.stage_create() {
            return Err(Error::Unsupported("staged table creation"));
        }
        let layout = self.storage_layout(request.context).await?;
        let parent = self.direct_parent(request, layout).await?;
        let target = target(request, parent.namespace, decoded.name(), layout)?;
        let initial = evaluate_table_creation(&decoded, target, request.timestamp_ms, limits())?;
        let response =
            crate::commit::publication::metadata_response(&initial.head, initial.document.canonical())?;
        self.validate_response_size(&response)?;
        crate::commit::publication::write_metadata_file(
            self.store.clone(),
            self.blocks.clone().ok_or(crate::catalog::CatalogError::Busy)?,
            request.context,
            &initial.document,
        )
        .await?;
        self.publish_direct_creation(request.context, &parent, initial.head)
            .await?;
        check_context(self.store.as_ref(), request.context).await?;
        Ok(TableCreationOutcome {
            status: 200,
            body: response,
        })
    }

    async fn direct_parent(
        &self,
        request: &TableCreationRequest,
        layout: CatalogStorageLayout,
    ) -> Result<NamespaceAuthority, Error> {
        let repository = NamespaceRepository::from_parts(self.store.clone(), self.names.clone());
        let parent =
            if layout == CatalogStorageLayout::Native && request.namespace.components() == ["default"] {
                repository
                    .ensure_default(request.context, request.identity.issued_ms)
                    .await?
            } else {
                repository
                    .load(request.context, &request.namespace)
                    .await?
                    .ok_or(Error::NamespaceMissing)?
            };
        if parent.lifecycle != NamespaceLifecycle::Ready {
            return Err(Error::NamespaceMissing);
        }
        Ok(parent)
    }
}

fn validate_principal(principal: &str) -> Result<(), ValidationError> {
    if principal.is_empty() || principal.len() > 256 || principal.contains('\0') {
        return Err(ValidationError::Text);
    }
    Ok(())
}

fn target(
    request: &TableCreationRequest,
    namespace: NamespaceId,
    name: &str,
    layout: CatalogStorageLayout,
) -> Result<TableHead, ValidationError> {
    let table = TableLocation {
        storage_namespace: (layout == CatalogStorageLayout::Native).then_some(namespace),
        catalog: request.context.catalog,
        table: TableId::random(),
    };
    let file = FileId::random();
    Ok(TableHead {
        catalog: table.catalog,
        table: table.table,
        namespace,
        name: name.into(),
        name_epoch: 1,
        lifecycle: TableLifecycle::Ready,
        generation: 1,
        metadata_file: file,
        metadata_location: table.file(&format!("metadata/1-{file}.metadata.json"))?,
        metadata_digest: [0; 32],
        commit_binding: None,
        format_version: 2,
        table_uuid: Some(uuid::Uuid::new_v4()),
        operation_fence: 1,
        pending_operation: None,
    })
}
