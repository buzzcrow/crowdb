use super::{Phase, TableLifecycleOperation, TableLifecycles, TablePurgeTask};
use crate::{
    catalog::{check_context, CasOutcome, CatalogError},
    commit::TableCommitOutcome,
    error::ValidationError,
    operation::mutation_identity,
    record::StorageRecord,
    table::{head_key, name_key, TableMapping, TableMappingState},
};

impl TableLifecycles {
    pub(super) async fn publish(&self, operation: &TableLifecycleOperation) -> Result<(), CatalogError> {
        self.current(operation).await?;
        if operation.is_rename() {
            self.check_admission(operation).await?;
        }
        let key = head_key(operation.context.catalog, operation.before.table);
        let before = StorageRecord::TableHead(Box::new(operation.before.clone())).encode()?;
        let after = StorageRecord::TableHead(Box::new(operation.candidate.clone())).encode()?;
        let encoded = key.encode()?;
        let result = self
            .store
            .compare_exchange(
                &encoded,
                Some(&before),
                &after,
                mutation_identity(&encoded, Some(&before), &after),
            )
            .await?;
        match result {
            CasOutcome::Applied(_) => (),
            CasOutcome::Conflict(Some(value)) if value.bytes == after => (),
            CasOutcome::Conflict(Some(value)) => {
                let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes)? else {
                    return Err(ValidationError::Record.into());
                };
                if head.operation_fence <= operation.before.operation_fence
                    || head.pending_operation == Some(operation.identity.operation)
                {
                    return Err(CatalogError::Busy);
                }
                return self.abort(operation, 409, "CommitFailedException").await;
            }
            CasOutcome::Conflict(None) => return Err(CatalogError::Busy),
        }
        self.advance(operation, &operation.next(Phase::Published)?).await
    }

    pub(super) async fn complete(&self, operation: &TableLifecycleOperation) -> Result<(), CatalogError> {
        self.current(operation).await?;
        let head_key = head_key(operation.context.catalog, operation.before.table);
        let value = self
            .store
            .get(&head_key.encode()?)
            .await?
            .ok_or(CatalogError::Busy)?;
        if value.bytes != StorageRecord::TableHead(Box::new(operation.candidate.clone())).encode()? {
            return Err(CatalogError::Busy);
        }
        if operation.is_rename() {
            self.publish_name(operation).await?;
        } else if operation.purge_requested {
            let task = TablePurgeTask {
                activation_epoch: operation.context.activation_epoch,
                head: operation.candidate.clone(),
            };
            let key = task.key().encode()?;
            let bytes = StorageRecord::TablePurgeTask(Box::new(task)).encode()?;
            let result = self
                .store
                .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
                .await?;
            if !matches!(result, CasOutcome::Applied(_))
                && !matches!(result, CasOutcome::Conflict(Some(value)) if value.bytes == bytes)
            {
                return Err(CatalogError::Busy);
            }
        }
        self.remove_mapping(&operation.source).await?;
        let mut next = operation.next(Phase::Complete)?;
        next.outcome = Some(TableCommitOutcome {
            status: 204,
            body: self
                .payloads
                .put(operation.context.catalog, operation.identity.operation, &[])
                .await?,
        });
        self.advance(operation, &next).await
    }

    async fn publish_name(&self, operation: &TableLifecycleOperation) -> Result<(), CatalogError> {
        let mapping = operation.destination(TableMappingState::Reserved);
        let key = name_key(mapping.catalog, mapping.namespace, &mapping.name)?.encode()?;
        let before = StorageRecord::TableMapping(mapping).encode()?;
        let after =
            StorageRecord::TableMapping(operation.destination(TableMappingState::Published)).encode()?;
        let result = self
            .names
            .compare_exchange(
                &key,
                Some(&before),
                &after,
                mutation_identity(&key, Some(&before), &after),
            )
            .await?;
        if !matches!(result, CasOutcome::Applied(_))
            && !matches!(result, CasOutcome::Conflict(Some(value)) if value.bytes == after)
        {
            return Err(CatalogError::Busy);
        }
        Ok(())
    }

    pub(super) async fn cleanup(&self, operation: &TableLifecycleOperation) -> Result<(), CatalogError> {
        self.current(operation).await?;
        if matches!(operation.phase, Phase::Aborting | Phase::Aborted) {
            if operation.is_rename() {
                self.remove_mapping(&operation.destination(TableMappingState::Reserved))
                    .await?;
            }
        } else if operation.phase == Phase::Complete {
            self.remove_mapping(&operation.source).await?;
            if operation.is_rename() {
                let key = head_key(operation.context.catalog, operation.before.table).encode()?;
                let before = StorageRecord::TableHead(Box::new(operation.candidate.clone())).encode()?;
                let mut settled = operation.candidate.clone();
                settled.pending_operation = None;
                let after = StorageRecord::TableHead(Box::new(settled)).encode()?;
                self.store
                    .compare_exchange(
                        &key,
                        Some(&before),
                        &after,
                        mutation_identity(&key, Some(&before), &after),
                    )
                    .await?;
            }
        } else {
            return Err(ValidationError::Record.into());
        }
        self.release_admission(operation).await?;
        check_context(self.store.as_ref(), operation.context).await
    }

    async fn remove_mapping(&self, mapping: &TableMapping) -> Result<(), CatalogError> {
        let key = name_key(mapping.catalog, mapping.namespace, &mapping.name)?.encode()?;
        let bytes = StorageRecord::TableMapping(mapping.clone()).encode()?;
        self.names
            .delete_mapping(&key, &bytes, mutation_identity(&key, Some(&bytes), &[]))
            .await?;
        Ok(())
    }

    pub(super) async fn abort(
        &self,
        operation: &TableLifecycleOperation,
        status: u16,
        kind: &str,
    ) -> Result<(), CatalogError> {
        let bytes = serde_json::to_vec(&serde_json::json!({"error":{"code":status,"type":kind,"message":"Table lifecycle conflicts with current authority"}}))
            .map_err(|_| ValidationError::Record)?;
        let mut next = operation.next(Phase::Aborting)?;
        next.outcome = Some(TableCommitOutcome {
            status,
            body: self
                .payloads
                .put(operation.context.catalog, operation.identity.operation, &bytes)
                .await?,
        });
        self.advance(operation, &next).await
    }
}
