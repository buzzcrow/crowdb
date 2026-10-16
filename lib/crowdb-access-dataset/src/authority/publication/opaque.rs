// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{decode, encode, DatasetAuthority};
use crate::{
    key, AuthorityError, CasOutcome, DatasetIdentity, OperationId, PublicationState, SnapshotId,
    SnapshotPublication, SnapshotRecord,
};

impl DatasetAuthority {
    pub(super) async fn prepare_opaque_snapshot(
        &self,
        identity: &DatasetIdentity,
        parent: Option<SnapshotId>,
        manifest: Vec<u8>,
        operation: OperationId,
    ) -> Result<SnapshotId, AuthorityError> {
        let operation_key = key::operation(identity, operation)?;
        if let Some(value) = self.store.get(&operation_key).await? {
            let snapshot = SnapshotId::from_bytes(&value.bytes)?;
            if let Some(value) = self.store.get(&key::snapshot(identity, snapshot)?).await? {
                let record: SnapshotRecord = decode(&value.bytes)?;
                if record.publication.parent != parent || record.publication.manifest != manifest {
                    return Err(AuthorityError::AlreadyExists);
                }
                return Ok(snapshot);
            }
            let publication = SnapshotPublication {
                snapshot,
                parent,
                operation,
                manifest,
                state: PublicationState::Prepared,
            };
            publication.validate()?;
            let bytes = encode(&SnapshotRecord { publication })?;
            match self
                .store
                .compare_exchange(&key::snapshot(identity, snapshot)?, None, &bytes)
                .await?
            {
                CasOutcome::Applied(_) => return Ok(snapshot),
                CasOutcome::Conflict(_) => return Err(AuthorityError::AlreadyExists),
            }
        }
        if self.current_latest(identity).await? != parent {
            return Err(AuthorityError::ParentConflict);
        }
        let snapshot = SnapshotId::random();
        match self
            .store
            .compare_exchange(&operation_key, None, snapshot.as_bytes())
            .await?
        {
            CasOutcome::Applied(_) => {}
            CasOutcome::Conflict(Some(value)) => return Ok(SnapshotId::from_bytes(&value.bytes)?),
            CasOutcome::Conflict(None) => return Err(AuthorityError::Corrupt),
        }
        let publication = SnapshotPublication {
            snapshot,
            parent,
            operation,
            manifest,
            state: PublicationState::Prepared,
        };
        publication.validate()?;
        let bytes = encode(&SnapshotRecord { publication })?;
        match self
            .store
            .compare_exchange(&key::snapshot(identity, snapshot)?, None, &bytes)
            .await?
        {
            CasOutcome::Applied(_) => Ok(snapshot),
            CasOutcome::Conflict(_) => Err(AuthorityError::AlreadyExists),
        }
    }
}
