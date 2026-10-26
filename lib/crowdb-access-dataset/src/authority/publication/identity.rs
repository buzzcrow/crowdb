// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::retirement;
use crate::{key, AuthorityError, DatasetAuthority, DatasetIdentity, OperationId, SnapshotId};

pub(super) struct PublicationIdentity {
    pub snapshot: SnapshotId,
    pub retired: bool,
    pub legacy: bool,
}
impl PublicationIdentity {
    pub(super) fn direct(operation: OperationId) -> Result<Self, AuthorityError> {
        Ok(Self {
            snapshot: SnapshotId::from_bytes(operation.as_bytes())?,
            retired: false,
            legacy: false,
        })
    }
}
impl DatasetAuthority {
    pub(super) async fn publication_identity(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<PublicationIdentity, AuthorityError> {
        let mut result = PublicationIdentity::direct(operation)?;
        if let Some(stored) = self.store.get(&key::operation(identity, operation)?).await? {
            if stored.bytes == retirement::RETIRED {
                result.retired = true;
            } else {
                result.snapshot = SnapshotId::from_bytes(&stored.bytes)?;
                result.legacy = true;
            }
        }
        Ok(result)
    }
}
