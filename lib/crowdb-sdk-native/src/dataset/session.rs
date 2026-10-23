// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{now, DatasetClient};
use crate::{OperationContext, Result};
use crowdb_access_dataset::{
    DatasetSessions, OperationId, ReadCancellation, ReadPreparation, ReadSession, SessionSpecV2,
};

impl DatasetClient {
    /// Advances one bounded preparation window; alias and Split binding are durable.
    /// # Errors
    /// Returns divergent preparation, selection/worker limits or storage failure.
    pub async fn prepare_read(
        &self,
        context: OperationContext,
        spec: &SessionSpecV2,
        page: u64,
    ) -> Result<ReadPreparation> {
        self.control(context, |scope| async move {
            Ok(DatasetSessions::new(scope.authority)
                .prepare_split_read(&scope.identity, spec, page, now(), &ReadCancellation::new())
                .await?)
        })
        .await
    }

    /// Resumes the acknowledged cursor. Buffer release never changes this cursor.
    /// # Errors
    /// Returns closed/missing session, cancellation or storage failure.
    pub async fn resume_read(&self, context: OperationContext, id: OperationId) -> Result<ReadSession> {
        self.control(context, |scope| async move {
            Ok(DatasetSessions::new(scope.authority)
                .resume(&scope.identity, id, now())
                .await?)
        })
        .await
    }

    /// # Errors
    /// Rejects forged/divergent cursors, expired sessions or storage failure.
    pub async fn acknowledge(
        &self,
        context: OperationContext,
        id: OperationId,
        expected: &str,
        next: &str,
    ) -> Result<ReadSession> {
        self.control(context, |scope| async move {
            Ok(DatasetSessions::new(scope.authority)
                .acknowledge(&scope.identity, id, expected, next, now())
                .await?)
        })
        .await
    }

    /// Closes only this read and its active storage protection, without ack.
    /// # Errors
    /// Returns missing preparation, admission or storage failure.
    pub async fn close_read(&self, context: OperationContext, id: OperationId) -> Result<ReadSession> {
        self.control(context, |scope| async move {
            Ok(DatasetSessions::new(scope.authority)
                .cancel_split_read(&scope.identity, id)
                .await?)
        })
        .await
    }
}
