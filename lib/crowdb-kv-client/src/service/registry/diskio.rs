// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Pin physical `DiskIO` registration and fence it with disk membership changes.

use super::ServiceRegistryClient;
use crate::{BatchOp, Error, GetOutcome, ReadMode, Result};
use crowdb_protocol::{
    common::InstanceValue,
    key::{InstanceKey, RackKey, TextKey},
};

fn location(value: &InstanceValue) -> Option<(u64, u64)> {
    let extra = value.extra.as_ref()?.diskdb.as_ref()?;
    Some((extra.rack_id?, extra.node_id?))
}

impl ServiceRegistryClient {
    async fn instance_record(&self, path: &str) -> Result<Option<(InstanceValue, u64)>> {
        match self
            .kv
            .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
            .await?
        {
            GetOutcome::NotFound => Ok(None),
            GetOutcome::Found { value, revision } => Ok(Some((
                serde_json::from_slice(&value).map_err(|error| Error::Server(error.to_string()))?,
                revision,
            ))),
        }
    }

    pub(super) async fn put_diskio_instance(
        &self,
        path: &str,
        value: &InstanceValue,
        payload: &[u8],
    ) -> Result<()> {
        for _ in 0..10 {
            let existing = self.instance_record(path).await?;
            let previous = existing.as_ref().and_then(|(value, _)| location(value));
            let current = location(value);
            if previous.is_some() && previous != current {
                return Err(Error::Server(
                    "DiskIO physical identity cannot be replaced or cleared".into(),
                ));
            }
            let revision = existing.map_or(0, |(_, revision)| revision);
            // Pin the initial physical identity with instance CAS before sharing
            // the Rack fence. A simultaneous different-node claimant must lose.
            if previous.is_none() {
                match self.kv.put_cas(0, 0, path.as_bytes(), payload, revision).await {
                    Ok(_) if current.is_none() => return Ok(()),
                    Ok(_) | Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => continue,
                    Err(error) => return Err(error),
                }
            }
            let (rack_id, _) =
                current.ok_or_else(|| Error::Server("DiskIO physical identity is unknown".into()))?;
            let rack_path = RackKey { rack_id }.to_path();
            let (rack, rack_revision) = match self
                .kv
                .get(0, 0, rack_path.as_bytes(), ReadMode::Linearizable, None)
                .await?
            {
                GetOutcome::Found { value, revision } => (value, revision),
                GetOutcome::NotFound => {
                    return self
                        .kv
                        .put_cas(0, 0, path.as_bytes(), payload, revision)
                        .await
                        .map(|_| ())
                }
            };
            let operations = [
                BatchOp::Put {
                    key: path.as_bytes().to_vec().into(),
                    value: payload.to_vec().into(),
                },
                BatchOp::Put {
                    key: rack_path.as_bytes().to_vec().into(),
                    value: rack,
                },
            ];
            match self
                .kv
                .batch_write_cas(0, 0, &operations, rack_path.as_bytes(), rack_revision)
                .await
            {
                Ok(_) => return Ok(()),
                Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::OutcomeUnknown)
    }

    pub(super) async fn unregister_diskio(&self, instance_id: u64) -> Result<()> {
        let path = InstanceKey {
            service: "diskio".into(),
            instance_id,
        }
        .to_path();
        let Some((value, revision)) = self.instance_record(&path).await? else {
            return Ok(());
        };
        if location(&value).is_some() {
            return Err(Error::Server(
                "located DiskIO registration is retained for exclusion confirmation".into(),
            ));
        }
        self.kv
            .batch_write_cas(
                0,
                0,
                &[BatchOp::Delete {
                    key: path.as_bytes().to_vec().into(),
                }],
                path.as_bytes(),
                revision,
            )
            .await
            .map(|_| ())
    }
}
