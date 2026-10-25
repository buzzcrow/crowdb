// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{TableLifecycleAction, TableLifecycleRequest, TableLifecycles, TablePurgeTask};
use crate::{
    catalog::{check_context, CasOutcome, CatalogError},
    error::ValidationError,
    key::OperationId,
    operation::mutation_identity,
    record::StorageRecord,
    table::{name_key, storage_head_key, TableHead, TableLifecycle, TableRepository},
};

impl TableLifecycles {
    pub(super) async fn drop_current(&self, request: &TableLifecycleRequest) -> Result<u16, CatalogError> {
        let TableLifecycleAction::Drop { purge_requested } = request.action else {
            return Err(ValidationError::Record.into());
        };
        let Some(parent) = self.namespaces.load(request.context, &request.namespace).await? else {
            return Ok(404);
        };
        let Some(selected) = TableRepository::new(self.store.clone())
            .select(request.context, parent.namespace, &request.name)
            .await?
        else {
            return Ok(404);
        };
        let head = selected.head;
        if head.pending_operation.is_some() {
            return Err(CatalogError::Busy);
        }
        let name = name_key(head.catalog, head.namespace, &head.name)?;
        let Some(source) = self.names.get(&name.encode()?).await? else {
            return Err(CatalogError::Conflict);
        };
        let StorageRecord::TableMapping(mapping) = StorageRecord::decode(&name, &source.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if !mapping.resolves(&head) {
            return Err(CatalogError::Conflict);
        }
        let mut candidate = head.clone();
        candidate.lifecycle = TableLifecycle::Tombstone;
        candidate.operation_fence = candidate
            .operation_fence
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        candidate.pending_operation = Some(OperationId::random());
        check_context(self.store.as_ref(), request.context).await?;
        if purge_requested {
            candidate = self.prepare_purge(request, &head, candidate).await?;
        }
        let key = storage_head_key(head.storage_location()).encode()?;
        let before = StorageRecord::TableHead(Box::new(head)).encode()?;
        let bytes = StorageRecord::TableHead(Box::new(candidate)).encode()?;
        match self
            .store
            .compare_exchange(
                &key,
                Some(&before),
                &bytes,
                mutation_identity(&key, Some(&before), &bytes),
            )
            .await?
        {
            CasOutcome::Applied(_) => {}
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => {}
            CasOutcome::Conflict(_) => return Ok(409),
        }
        let key = name.encode()?;
        self.names
            .delete_mapping(
                &key,
                &source.bytes,
                mutation_identity(&key, Some(&source.bytes), &[]),
            )
            .await?;
        check_context(self.store.as_ref(), request.context).await?;
        Ok(204)
    }

    async fn prepare_purge(
        &self,
        request: &TableLifecycleRequest,
        before: &TableHead,
        candidate: TableHead,
    ) -> Result<TableHead, CatalogError> {
        let marker = TablePurgeTask {
            activation_epoch: request.context.activation_epoch,
            head: candidate,
            dropped_ms: 0,
        };
        let key = marker.key();
        let encoded = key.encode()?;
        let bytes = StorageRecord::TablePurgeTask(Box::new(marker.clone())).encode()?;
        let mut expected: Option<Vec<u8>> = None;
        for _ in 0..8 {
            match self
                .store
                .compare_exchange(
                    &encoded,
                    expected.as_deref(),
                    &bytes,
                    mutation_identity(&encoded, expected.as_deref(), &bytes),
                )
                .await?
            {
                CasOutcome::Applied(_) => return Ok(marker.head),
                CasOutcome::Conflict(Some(value)) => {
                    let StorageRecord::TablePurgeTask(existing) = StorageRecord::decode(&key, &value.bytes)?
                    else {
                        return Err(ValidationError::Record.into());
                    };
                    if existing.activation_epoch != request.context.activation_epoch {
                        return Err(CatalogError::Conflict);
                    }
                    let predecessor = existing.predecessor()?;
                    if predecessor == *before && existing.dropped_ms == 0 {
                        return Ok(existing.head);
                    }
                    if existing.dropped_ms != 0
                        || (before.generation <= predecessor.generation
                            && before.operation_fence <= predecessor.operation_fence)
                    {
                        return Err(CatalogError::Conflict);
                    }
                    expected = Some(value.bytes);
                }
                CasOutcome::Conflict(None) => return Err(CatalogError::Conflict),
            }
        }
        Err(CatalogError::Busy)
    }
}
