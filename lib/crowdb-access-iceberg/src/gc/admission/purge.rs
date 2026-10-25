// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    check_context, mutation_identity, CasOutcome, CatalogContext, CatalogError, GcLimits, GcRepository,
    GcTask, GcTaskKind, OperationId, StorageRecord, TablePurgeTask, ValidationError,
};
use crate::{catalog::StoredValue, table::storage_head_key};
use tokio::time::Instant;

/// Resource discovery can cancel an unpublished deletion without creating a worker.
pub enum PurgeAdmission {
    Ready(Box<GcTask>),
    Cancelled,
    Deferred,
}

impl GcRepository {
    /// Installs a worker only for the exact completed table deletion.
    /// `now_ms` is the caller's clock sampled immediately before this call.
    /// # Errors
    /// Rejects stale contexts, corrupt identities and unknown storage outcomes.
    pub async fn discover_purge(
        &self,
        context: CatalogContext,
        marker: &TablePurgeTask,
        now_ms: u64,
        limits: GcLimits,
    ) -> Result<PurgeAdmission, CatalogError> {
        let started = Instant::now();
        marker.validate()?;
        if marker.head.catalog != context.catalog || marker.activation_epoch != context.activation_epoch {
            return Err(ValidationError::IdentityMismatch.into());
        }
        check_context(self.store.as_ref(), context).await?;
        let key = marker.key();
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(PurgeAdmission::Cancelled);
        };
        if StorageRecord::decode(&key, &value.bytes)?
            != StorageRecord::TablePurgeTask(Box::new(marker.clone()))
        {
            return Ok(PurgeAdmission::Deferred);
        }
        let identity = OperationId::from_bytes(marker.head.table.as_bytes())?;
        if let Some(existing) = self.task(context.catalog, identity).await? {
            if existing.context != context
                || existing.kind != GcTaskKind::PurgeTable
                || existing.head.as_ref() != Some(&marker.head)
            {
                return Err(CatalogError::Conflict);
            }
            return Ok(PurgeAdmission::Ready(Box::new(existing)));
        }
        self.resolve_purge_head(context, marker, now_ms, started, limits)
            .await
    }

    async fn resolve_purge_head(
        &self,
        context: CatalogContext,
        marker: &TablePurgeTask,
        now_ms: u64,
        started: Instant,
        limits: GcLimits,
    ) -> Result<PurgeAdmission, CatalogError> {
        let key = storage_head_key(marker.head.storage_location());
        let encoded = key.encode()?;
        let mut value = self.store.get(&encoded).await?;
        for _ in 0..8 {
            let record = value
                .as_ref()
                .map(|value| StorageRecord::decode(&key, &value.bytes))
                .transpose()?;
            let next = match record {
                Some(StorageRecord::TableHead(head)) if *head == marker.head => {
                    let task = GcTask::plan(
                        context,
                        OperationId::from_bytes(marker.head.table.as_bytes())?,
                        Some(marker.head.clone()),
                        if marker.dropped_ms == 0 {
                            let elapsed = u64::try_from(started.elapsed().as_nanos().div_ceil(1_000_000))
                                .map_err(|_| ValidationError::Deadline)?;
                            now_ms.checked_add(elapsed).ok_or(ValidationError::Deadline)?
                        } else {
                            marker.dropped_ms
                        },
                        limits,
                    )?;
                    return self
                        .exchange_admission_task(None, &task)
                        .await
                        .map(|task| PurgeAdmission::Ready(Box::new(task)));
                }
                Some(StorageRecord::RetiredTable(_)) => return self.cancel_purge_marker(marker).await,
                Some(StorageRecord::TableHead(head)) => {
                    let predecessor = marker.predecessor()?;
                    if *head == predecessor {
                        let mut fenced = *head;
                        fenced.operation_fence = fenced
                            .operation_fence
                            .checked_add(1)
                            .ok_or(ValidationError::GenerationExhausted)?;
                        StorageRecord::TableHead(Box::new(fenced))
                    } else if head.generation > predecessor.generation
                        || head.operation_fence > predecessor.operation_fence
                    {
                        return self.cancel_purge_marker(marker).await;
                    } else {
                        return Err(CatalogError::Conflict);
                    }
                }
                None => StorageRecord::RetiredTable(marker.head.storage_location()),
                _ => return Err(ValidationError::Record.into()),
            };
            let bytes = next.encode()?;
            match self
                .store
                .compare_exchange(
                    &encoded,
                    value.as_ref().map(|value| value.bytes.as_slice()),
                    &bytes,
                    mutation_identity(
                        &encoded,
                        value.as_ref().map(|value| value.bytes.as_slice()),
                        &bytes,
                    ),
                )
                .await?
            {
                CasOutcome::Applied(revision) => value = Some(StoredValue { bytes, revision }),
                CasOutcome::Conflict(observed) => value = observed,
            }
        }
        Ok(PurgeAdmission::Deferred)
    }

    async fn cancel_purge_marker(&self, marker: &TablePurgeTask) -> Result<PurgeAdmission, CatalogError> {
        let key = marker.key().encode()?;
        let bytes = StorageRecord::TablePurgeTask(Box::new(marker.clone())).encode()?;
        match self
            .store
            .delete_gc_record(&key, &bytes, mutation_identity(&key, Some(&bytes), &[]))
            .await?
        {
            CasOutcome::Applied(_) | CasOutcome::Conflict(None) => Ok(PurgeAdmission::Cancelled),
            CasOutcome::Conflict(_) => Ok(PurgeAdmission::Deferred),
        }
    }
}
