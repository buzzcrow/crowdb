// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::DatasetClient;
use crate::{OperationContext, Result};
use crowdb_access_dataset::{
    DatasetAuthority, DatasetId, DatasetIdentity, DatasetRecord, DatasetUploads, SnapshotId, SnapshotRecord,
};
use std::{future::Future, sync::Arc};

/// Existing authority services scoped by an explicitly supplied trusted identity.
/// Complete resources and mutable pointers retain the authority's typed CAS rules.
pub struct DatasetControl {
    pub authority: Arc<DatasetAuthority>,
    pub uploads: Arc<DatasetUploads>,
    pub identity: DatasetIdentity,
}

impl DatasetClient {
    /// Runs one caller-selected control operation under shared request admission.
    /// Retain resource IDs, complete inputs and explicit bases for uncertain outcomes.
    /// This method never retries the callback, rebases a condition or switches to HTTP.
    /// # Errors
    /// Returns admission, domain failures or interrupted mutation uncertainty.
    pub async fn control<T, F, R>(&self, context: OperationContext, operation: F) -> Result<T>
    where
        F: FnOnce(DatasetControl) -> R,
        R: Future<Output = Result<T>>,
    {
        let id = context.request_id.get();
        let request = self.runtime.resources.admit(context)?;
        let scope = DatasetControl {
            authority: Arc::clone(&self.authority),
            uploads: Arc::clone(&self.uploads),
            identity: self.identity.clone(),
        };
        request
            .run_mutation(async { operation(scope).await })
            .await
            .map_err(|error| match error {
                crate::Error::Dataset {
                    kind: super::FailureKind::Storage,
                    ..
                } => crate::Error::UncertainMutation {
                    request_id: id,
                    reason: crate::error::MutationInterruption::Transport,
                },
                other => other,
            })
    }

    /// # Errors
    /// Returns duplicate identity, admission or storage failure.
    pub async fn create(&self, context: OperationContext) -> Result<DatasetId> {
        self.control(context, |scope| async move {
            Ok(scope.authority.create_dataset(scope.identity).await?)
        })
        .await
    }

    /// Marks an empty Dataset deleted; cleanup follows the twenty-minute grace period.
    /// Retain the Dataset ID for safe retries, including after name reuse.
    /// # Errors
    /// Rejects nonempty, missing or replaced Datasets, admission and storage failures.
    pub async fn delete(
        &self,
        context: OperationContext,
        id: DatasetId,
    ) -> Result<crowdb_access_dataset::DatasetDeletion> {
        self.control(context, |scope| async move {
            Ok(scope
                .authority
                .delete_dataset(
                    &scope.identity,
                    id,
                    &crowdb_access_dataset::ReadCancellation::new(),
                )
                .await?)
        })
        .await
    }

    /// # Errors
    /// Returns missing identity, admission or storage failure.
    pub async fn open(&self, context: OperationContext) -> Result<DatasetRecord> {
        let request = self.runtime.resources.admit(context)?;
        request
            .run(async { Ok(self.authority.get_dataset(&self.identity).await?) })
            .await
    }

    /// # Errors
    /// Returns missing snapshot, admission or storage failure.
    pub async fn snapshot(&self, context: OperationContext, id: SnapshotId) -> Result<SnapshotRecord> {
        let request = self.runtime.resources.admit(context)?;
        request
            .run(async { Ok(self.authority.get_snapshot(&self.identity, id).await?) })
            .await
    }
}
