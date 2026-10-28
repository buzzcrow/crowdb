// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A new process advances the same logical owner's generation before media use.
use super::{mutation, HardwareClient};
use crate::{BatchOp, Error, Result, ServiceRegistryClient};
use crowdb_protocol::{
    common::OwnerMapValue,
    key::{OwnerMapKey, RackKey, TextKey},
};

impl HardwareClient {
    /// Advance the owner generation once for the current process session. Call
    /// before reading member disks and claiming the data-group ownership fence.
    ///
    /// # Errors
    /// Rejects replaced sessions/owners, lower epochs and unavailable authority.
    pub async fn claim_owner_process_epoch(
        &self,
        key: &OwnerMapKey,
        instance: u64,
        epoch: u64,
    ) -> Result<u64> {
        let service = ServiceRegistryClient::from_shared(self.kv.clone());
        if epoch == 0 || service.diskdb_session_epoch(instance).await? != epoch {
            return Err(Error::Server("DiskDB process session was replaced".into()));
        }
        let path = key.to_path();
        let rack_path = RackKey { rack_id: key.rack_id }.to_path();
        for _ in 0..10 {
            let rack = mutation::read(&self.kv, &rack_path).await?;
            let (bytes, generation) = mutation::read(&self.kv, &path)
                .await?
                .ok_or_else(|| Error::Server("DiskGroup owner disappeared".into()))?;
            let mut owner: OwnerMapValue = mutation::decode(&path, &bytes)?;
            if owner.instance_id != instance || owner.process_epoch > epoch {
                return Err(Error::Server("DiskGroup owner or process was replaced".into()));
            }
            if owner.process_epoch == epoch {
                return Ok(generation);
            }
            owner.process_epoch = epoch;
            let mut operations = vec![mutation::put(&path, &owner)?];
            let (condition, revision) = if let Some((bytes, revision)) = rack {
                operations.push(BatchOp::Put {
                    key: rack_path.as_bytes().to_vec().into(),
                    value: bytes,
                });
                (rack_path.as_str(), revision)
            } else {
                (path.as_str(), generation)
            };
            match self
                .kv
                .batch_write_cas(0, 0, &operations, condition.as_bytes(), revision)
                .await
            {
                Ok(_) | Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::OutcomeUnknown)
    }
}
