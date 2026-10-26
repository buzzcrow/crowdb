// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::DatasetClient;
use crate::{OperationContext, Result};
use crowdb_access_dataset::{DatasetReads, ReadBinding, ReadSpec};
impl DatasetClient {
    /// Binds immutable versions into a client-owned continuation without writes.
    /// # Errors
    /// Returns invalid queries, unavailable metadata or cancellation.
    pub async fn bind_read(&self, context: OperationContext, spec: ReadSpec) -> Result<ReadBinding> {
        self.control(context, |scope| async move {
            Ok(DatasetReads::new(scope.authority)
                .bind(&scope.identity, spec)
                .await?)
        })
        .await
    }
}
