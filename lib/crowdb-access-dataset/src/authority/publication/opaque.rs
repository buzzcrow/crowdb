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
        let resolved = self.publication_identity(identity, operation).await?;
        let snapshot = resolved.snapshot;
        if let Some(value) = self.store.get(&key::snapshot(identity, snapshot)?).await? {
            if value.bytes == super::retirement::RETIRED {
                return Err(crate::DatasetError::InvalidPublicationState.into());
            }
            let record: SnapshotRecord = decode(&value.bytes)?;
            if record.publication.operation != operation
                || record.publication.parent != parent
                || record.publication.manifest != manifest
            {
                return Err(AuthorityError::AlreadyExists);
            }
            return Ok(snapshot);
        }
        if resolved.retired {
            return Err(crate::DatasetError::InvalidPublicationState.into());
        }
        if !resolved.legacy && self.current_latest(identity).await? != parent {
            return Err(AuthorityError::ParentConflict);
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
