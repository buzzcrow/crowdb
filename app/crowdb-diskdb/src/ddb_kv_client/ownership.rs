// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Data-group fencing for `DiskGroup` management handovers.

use crowdb_kv_client::{BatchOp, Error, GetOutcome, ReadMode, Result};
use serde::{Deserialize, Serialize};

use super::{Bind, DdbKvClient};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnershipFence {
    pub rack_id: u64,
    pub node_id: u64,
    pub disk_group_id: u64,
    pub instance_id: u64,
    pub generation: u64,
}

impl OwnershipFence {
    fn key(&self) -> Vec<u8> {
        format!(
            "/diskdb/ownership-fence/{}/{}/{}",
            self.rack_id, self.node_id, self.disk_group_id
        )
        .into_bytes()
    }
}

fn fenced() -> Error {
    Error::Server("DiskGroup ownership changed; stale DiskDB writes are fenced".into())
}

impl DdbKvClient {
    pub fn ownership_fence(&self) -> Option<std::sync::Arc<OwnershipFence>> {
        self.fence.clone()
    }
    #[must_use]
    pub fn with_ownership_fencing(mut self) -> Self {
        self.require_fence = true;
        self
    }

    /// Capture authority from the exact in-memory group used by this operation.
    #[must_use]
    pub fn for_group(&self, group: &crate::model::disk_group::DdbDiskGroup) -> Self {
        let mut scoped = self.clone();
        scoped.fence = group.ownership_fence();
        scoped
    }

    /// Fence the previous manager before reconstructing any allocation bitmap.
    /// The generation comes from a confirmed Group 0 owner record revision.
    pub async fn claim_ownership(&self, bind: Bind, fence: &OwnershipFence) -> Result<()> {
        let key = fence.key();
        let bytes = serde_json::to_vec(fence).map_err(|e| Error::Server(e.to_string()))?;
        let revision = match self
            .kv
            .get(bind.0, bind.1, &key, ReadMode::Linearizable, None)
            .await?
        {
            GetOutcome::NotFound => 0,
            GetOutcome::Found { value, revision } => {
                let current: OwnershipFence =
                    serde_json::from_slice(&value).map_err(|e| Error::Server(e.to_string()))?;
                if current == *fence {
                    return Ok(());
                }
                if current.generation >= fence.generation {
                    return Err(fenced());
                }
                revision
            }
        };
        self.kv
            .put_cas(bind.0, bind.1, &key, &bytes, revision)
            .await
            .map(|_| ())
    }

    pub(super) async fn write_owned(
        &self,
        bind: Bind,
        ops: &[BatchOp],
        record_condition: Option<(&[u8], u64)>,
    ) -> Result<u64> {
        let Some(fence) = &self.fence else {
            if self.require_fence {
                return Err(fenced());
            }
            return if let Some((key, revision)) = record_condition {
                self.kv
                    .batch_write_cas(bind.0, bind.1, ops, key, revision)
                    .await
                    .map(|v| v.revision)
            } else {
                self.kv.batch_write(bind.0, bind.1, ops).await.map(|v| v.revision)
            };
        };
        let key = fence.key();
        let expected = serde_json::to_vec(&**fence).map_err(|error| Error::Server(error.to_string()))?;
        self.kv
            .batch_write_owned(bind.0, bind.1, ops, &key, &expected, record_condition)
            .await
            .map(|write| write.revision)
    }
}
