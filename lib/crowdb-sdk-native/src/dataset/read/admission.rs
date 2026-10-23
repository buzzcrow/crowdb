// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::DatasetClient;
use crate::{runtime::RequestLease, Result};
use crowdb_access_dataset::{DatasetAuthority, DatasetIdentity, DatasetSessions, OperationId, SnapshotId};
use std::sync::Arc;

pub(super) struct ReadAdmission {
    authority: Arc<DatasetAuthority>,
    identity: DatasetIdentity,
    session: Option<OperationId>,
}
impl ReadAdmission {
    pub(super) async fn admit(
        client: &DatasetClient,
        request: &RequestLease,
        snapshot: SnapshotId,
        session: Option<OperationId>,
    ) -> Result<Self> {
        request
            .run(async {
                Ok(client
                    .authority
                    .check_snapshot_readable(&client.identity, snapshot)
                    .await?)
            })
            .await?;
        Ok(Self {
            authority: Arc::clone(&client.authority),
            identity: client.identity.clone(),
            session,
        })
    }
    pub(super) async fn check(&self) -> Result<()> {
        if let Some(session) = self.session {
            DatasetSessions::new(Arc::clone(&self.authority))
                .check_open(&self.identity, session)
                .await?;
        }
        Ok(())
    }
}
