use super::{
    evaluate_table_creation, limits, CatalogError, CreateTableRequest, Error, FileId, NamespaceRepository,
    Phase, TableCreateOperation, TableCreationRequest, TableCreator, TableHead, TableId, TableLifecycle,
    TableLocation, ValidationError,
};

impl TableCreator {
    async fn storage_layout(
        &self,
        context: crate::catalog::CatalogContext,
    ) -> Result<crate::catalog::CatalogStorageLayout, Error> {
        let authority_key = crate::key::IcebergKey::Catalog {
            catalog: context.catalog,
            scope: crate::key::CatalogScope::Authority,
            suffix: Vec::new(),
        };
        let value = self
            .store
            .get(&authority_key.encode()?)
            .await
            .map_err(CatalogError::from)?
            .ok_or(CatalogError::Uninitialized)?;
        let crate::record::StorageRecord::Authority(authority) =
            crate::record::StorageRecord::decode(&authority_key, &value.bytes)?
        else {
            return Err(ValidationError::Record.into());
        };
        Ok(authority.storage_layout)
    }

    pub(super) async fn prepare(
        &self,
        request: &TableCreationRequest,
        expires_ms: Option<i64>,
    ) -> Result<TableCreateOperation, Error> {
        let mut decoded = CreateTableRequest::decode(&request.body, limits())?;
        if decoded.stage_create() != expires_ms.is_some() {
            return Err(Error::Unsupported("staged table creation"));
        }
        let layout = self.storage_layout(request.context).await?;
        let repository = NamespaceRepository::from_parts(self.store.clone(), self.names.clone());
        let namespace = if layout == crate::catalog::CatalogStorageLayout::Native
            && request.namespace.components() == ["default"]
        {
            repository
                .ensure_default(request.context, request.identity.issued_ms)
                .await?
        } else {
            repository
                .load(request.context, &request.namespace)
                .await?
                .ok_or(Error::NamespaceMissing)?
        };
        if let Some(expiry) = expires_ms {
            if request.timestamp_ms < 0 || expiry <= request.timestamp_ms {
                return Err(ValidationError::Record.into());
            }
            if crate::table::TableRepository::new(self.store.clone())
                .select(request.context, namespace.namespace, decoded.name())
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
        }
        let target = target(
            request,
            namespace.namespace,
            decoded.name(),
            expires_ms.is_some(),
            layout,
        )?;
        let initial = evaluate_table_creation(&decoded, target, request.timestamp_ms, limits())?;
        let response = if expires_ms.is_some() {
            super::staged::stage_response(initial.document.canonical())?
        } else {
            crate::commit::publication::metadata_response(&initial.head, initial.document.canonical())?
        };
        self.validate_response_size(&response)?;
        let payloads = self.payloads();
        let input = payloads
            .put(request.context.catalog, request.identity.operation, &request.body)
            .await?;
        let document = payloads
            .put(
                request.context.catalog,
                request.identity.operation,
                initial.document.canonical(),
            )
            .await?;
        let response = payloads
            .put(request.context.catalog, request.identity.operation, &response)
            .await?;
        Ok(TableCreateOperation {
            context: request.context,
            identity: request.identity,
            principal: request.principal.clone(),
            namespace: request.namespace.clone(),
            revision: 1,
            timestamp_ms: request.timestamp_ms,
            phase: if expires_ms.is_some() {
                Phase::Staged
            } else {
                Phase::Prepared
            },
            input,
            document,
            response: response.clone(),
            candidate: initial.head,
            admission: None,
            outcome: None,
            stage: expires_ms.map(|expires_ms| crate::commit::TableCreateStage {
                created_ms: request.timestamp_ms,
                expires_ms,
                response,
                binding: None,
            }),
        })
    }
}

fn target(
    request: &TableCreationRequest,
    namespace: crate::key::NamespaceId,
    name: &str,
    staged: bool,
    layout: crate::catalog::CatalogStorageLayout,
) -> Result<TableHead, ValidationError> {
    let table = TableLocation {
        storage_namespace: match layout {
            crate::catalog::CatalogStorageLayout::Legacy => None,
            crate::catalog::CatalogStorageLayout::Native => Some(namespace),
        },
        catalog: request.context.catalog,
        table: if staged {
            TableId::from_bytes(request.identity.operation.as_bytes())?
        } else {
            TableId::random()
        },
    };
    Ok(TableHead {
        catalog: request.context.catalog,
        table: table.table,
        namespace,
        name: name.into(),
        name_epoch: 1,
        lifecycle: TableLifecycle::Ready,
        generation: 1,
        metadata_file: FileId::from_bytes(request.identity.operation.as_bytes())?,
        metadata_location: table.file(&format!(
            "metadata/1-{}.metadata.json",
            request.identity.operation
        ))?,
        metadata_digest: [0; 32],
        commit_binding: None,
        format_version: 2,
        table_uuid: Some(uuid::Uuid::new_v4()),
        operation_fence: 1,
        pending_operation: Some(request.identity.operation),
    })
}
