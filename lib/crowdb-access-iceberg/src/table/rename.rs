use std::sync::Arc;

use crate::{
    catalog::{check_context, CasOutcome, CatalogContext, CatalogError, CatalogStore},
    error::ValidationError,
    namespace::{
        NamespaceAuthority, NamespaceLifecycle, NamespaceRename, NamespaceRepository, NamespaceStore,
    },
    operation::mutation_identity,
    record::StorageRecord,
};

use super::{
    name_key, storage_head_key, TableHead, TableLifecycleAction, TableLifecycleRequest, TableMapping,
    TableMappingState, TableRepository,
};

mod admission;
mod recovery;
mod reservation;

pub(crate) struct TableRenames {
    store: Arc<dyn CatalogStore>,
    names: Arc<dyn NamespaceStore>,
    namespaces: NamespaceRepository,
}

impl TableRenames {
    pub(crate) fn from_parts(store: Arc<dyn CatalogStore>, names: Arc<dyn NamespaceStore>) -> Self {
        Self {
            namespaces: NamespaceRepository::from_parts(store.clone(), names.clone()),
            store,
            names,
        }
    }

    pub(crate) async fn execute(&self, request: &TableLifecycleRequest) -> Result<u16, CatalogError> {
        let TableLifecycleAction::Rename { namespace, name } = &request.action else {
            return Err(ValidationError::Record.into());
        };
        let Some(source) = self.parent(request.context, &request.namespace).await? else {
            return Ok(404);
        };
        let target = if namespace == &request.namespace {
            source.clone()
        } else {
            let Some(target) = self.parent(request.context, namespace).await? else {
                return Ok(404);
            };
            target
        };
        let Some(selected) = TableRepository::new(self.store.clone())
            .select(request.context, source.namespace, &request.name)
            .await?
        else {
            return Ok(404);
        };
        if selected.head.pending_operation.is_some() {
            return Err(CatalogError::Busy);
        }
        if namespace == &request.namespace && name == &request.name {
            return Ok(204);
        }
        let key = name_key(request.context.catalog, source.namespace, &request.name)?;
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(CatalogError::Conflict)?;
        let StorageRecord::TableMapping(mapping) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if !mapping.resolves(&selected.head) {
            return Err(CatalogError::Conflict);
        }
        let rename = NamespaceRename {
            source: mapping,
            destination_namespace: target.namespace,
            destination_name: name.clone(),
            // A retried HTTP request must not reuse a canceled reservation's bytes.
            operation: crate::key::OperationId::random(),
            head_generation: selected.head.generation,
            head_fence: selected.head.operation_fence,
        };
        rename.validate()?;
        let mut parents = vec![source, target];
        parents.sort_unstable_by_key(|parent| *parent.namespace.as_bytes());
        parents.dedup_by_key(|parent| parent.namespace);
        if let Err(error) = self.admit(&rename, parents).await {
            if matches!(error, CatalogError::Busy | CatalogError::Conflict) {
                self.recover(request.context, &rename).await?;
            }
            return Err(error);
        }
        if let Err(error) = self.reserve(request.context, &rename).await {
            if matches!(error, CatalogError::Busy | CatalogError::Conflict) {
                self.recover(request.context, &rename).await?;
            }
            return Err(error);
        }
        self.publish(request.context, &rename, selected.head).await
    }

    async fn parent(
        &self,
        context: CatalogContext,
        name: &crate::namespace::NamespaceIdentifier,
    ) -> Result<Option<NamespaceAuthority>, CatalogError> {
        let parent = self.namespaces.load(context, name).await?;
        if let Some(rename) = parent.as_ref().and_then(|parent| parent.rename.as_ref()) {
            self.recover(context, rename).await?;
            return Err(CatalogError::Busy);
        }
        if parent.as_ref().is_some_and(|parent| {
            parent.lifecycle != NamespaceLifecycle::Ready || parent.pending_operation.is_some()
        }) {
            return Err(CatalogError::Busy);
        }
        Ok(parent)
    }

    async fn publish(
        &self,
        context: CatalogContext,
        rename: &NamespaceRename,
        before: TableHead,
    ) -> Result<u16, CatalogError> {
        check_context(self.store.as_ref(), context).await?;
        let mut candidate = before.clone();
        candidate.namespace = rename.destination_namespace;
        candidate.name.clone_from(&rename.destination_name);
        candidate.name_epoch += 1;
        candidate.operation_fence += 1;
        let key = storage_head_key(before.storage_location());
        let encoded = key.encode()?;
        let expected = StorageRecord::TableHead(Box::new(before)).encode()?;
        let bytes = StorageRecord::TableHead(Box::new(candidate.clone())).encode()?;
        let result = self
            .store
            .compare_exchange(
                &encoded,
                Some(&expected),
                &bytes,
                mutation_identity(&encoded, Some(&expected), &bytes),
            )
            .await?;
        let (head, status) = match result {
            CasOutcome::Applied(_) => (candidate, 204),
            CasOutcome::Conflict(Some(value)) => {
                let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes)? else {
                    return Err(ValidationError::Record.into());
                };
                let status = if value.bytes == bytes { 204 } else { 409 };
                (*head, status)
            }
            CasOutcome::Conflict(None) => return Err(CatalogError::Busy),
        };
        self.settle(context, rename, head).await?;
        check_context(self.store.as_ref(), context).await?;
        Ok(status)
    }
}
