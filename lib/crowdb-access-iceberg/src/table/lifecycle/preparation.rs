use super::{Phase, TableLifecycleAction, TableLifecycleOperation, TableLifecycleRequest, TableLifecycles};
use crate::{
    catalog::CatalogError,
    commit::TableCommitOutcome,
    error::ValidationError,
    key::NameSuffix,
    record::StorageRecord,
    table::{name_key, TableLifecycle, TableRepository},
};

pub(super) fn input(request: &TableLifecycleRequest) -> Result<Vec<u8>, CatalogError> {
    request.context.validate()?;
    if request.principal.is_empty() || request.principal.len() > 256 || request.principal.contains('\0') {
        return Err(ValidationError::Text.into());
    }
    NameSuffix {
        parent: None,
        name: &request.name,
    }
    .encode()?;
    let action = match &request.action {
        TableLifecycleAction::Drop { purge_requested } => serde_json::json!({"drop": purge_requested}),
        TableLifecycleAction::Rename { namespace, name } => {
            NameSuffix { parent: None, name }.encode()?;
            serde_json::json!({"rename": {"namespace": namespace.components(), "name": name}})
        }
    };
    serde_json::to_vec(&serde_json::json!({"namespace":request.namespace.components(), "name":request.name, "action":action}))
        .map_err(|_| ValidationError::Record.into())
}

impl TableLifecycles {
    pub(super) async fn prepare(
        &self,
        request: &TableLifecycleRequest,
        input: &[u8],
    ) -> Result<Option<TableCommitOutcome>, CatalogError> {
        let Some(namespace) = self.namespaces.load(request.context, &request.namespace).await? else {
            return self
                .initial_outcome(request, input, 404, "NoSuchTableException")
                .await;
        };
        let Some(selected) = TableRepository::new(self.store.clone())
            .select(request.context, namespace.namespace, &request.name)
            .await?
        else {
            return self
                .initial_outcome(request, input, 404, "NoSuchTableException")
                .await;
        };
        let before = selected.head;
        if before.pending_operation.is_some() {
            return Err(CatalogError::Busy);
        }
        let key = name_key(before.catalog, before.namespace, &before.name)?;
        let value = self.store.get(&key.encode()?).await?.ok_or(CatalogError::Busy)?;
        let StorageRecord::TableMapping(source) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if !source.resolves(&before) {
            return Err(CatalogError::Busy);
        }
        let mut candidate = before.clone();
        candidate.operation_fence = candidate
            .operation_fence
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        candidate.pending_operation = Some(request.identity.operation);
        let (destination_namespace, purge_requested) = match &request.action {
            TableLifecycleAction::Drop { purge_requested } => {
                candidate.lifecycle = TableLifecycle::Tombstone;
                (None, *purge_requested)
            }
            TableLifecycleAction::Rename { namespace, name } => {
                if namespace == &request.namespace && name == &request.name {
                    return self.initial_outcome(request, input, 204, "").await;
                }
                let Some(target) = self.namespaces.load(request.context, namespace).await? else {
                    return self
                        .initial_outcome(request, input, 404, "NoSuchNamespaceException")
                        .await;
                };
                candidate.namespace = target.namespace;
                candidate.name.clone_from(name);
                candidate.name_epoch = candidate
                    .name_epoch
                    .checked_add(1)
                    .ok_or(ValidationError::GenerationExhausted)?;
                (Some(namespace.clone()), false)
            }
        };
        let operation = TableLifecycleOperation {
            context: request.context,
            identity: request.identity,
            principal: request.principal.clone(),
            revision: 1,
            phase: Phase::Prepared,
            input: self
                .payloads
                .put(request.context.catalog, request.identity.operation, input)
                .await?,
            source,
            before,
            candidate,
            destination_namespace,
            purge_requested,
            admission: None,
            outcome: None,
        };
        self.begin(request, input, operation).await?;
        Ok(None)
    }

    async fn initial_outcome(
        &self,
        request: &TableLifecycleRequest,
        input: &[u8],
        status: u16,
        kind: &str,
    ) -> Result<Option<TableCommitOutcome>, CatalogError> {
        if let Some(operation) = self.load(request.context, request.identity.operation).await? {
            self.match_request(request, input, &operation).await?;
            return Ok(None);
        }
        let body = if status == 204 {
            Vec::new()
        } else {
            serde_json::to_vec(&serde_json::json!({"error":{"code":status,"type":kind,"message":"Table lifecycle target is unavailable"}}))
                .map_err(|_| ValidationError::Record)?
        };
        let body = self
            .payloads
            .put(request.context.catalog, request.identity.operation, &body)
            .await?;
        crate::catalog::check_context(self.store.as_ref(), request.context).await?;
        Ok(Some(TableCommitOutcome { status, body }))
    }
}
