// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A fixed per-boot service session; an old boot cannot renew after replacement.
use super::{now_ms, ServiceRegistryClient};
use crate::{BatchOp, Error, GetOutcome, ReadMode, Result};
use crowdb_protocol::{
    common::{DiskGroupUsageSummary, DiskdbExtra, InstanceValue, ServiceExtra},
    key::InstanceKey,
};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Session {
    version: u32,
    instance_id: u64,
    boot_id: String,
    epoch: u64,
}
fn key(instance: u64) -> String {
    format!("/diskdb/session/{instance}")
}
fn replaced() -> Error {
    Error::Server("DiskDB process session was replaced; the old process is fenced".into())
}
impl ServiceRegistryClient {
    async fn diskdb_session(&self, instance: u64) -> Result<Option<(Session, u64)>> {
        match self
            .kv
            .get(0, 0, key(instance).as_bytes(), ReadMode::Linearizable, None)
            .await?
        {
            GetOutcome::NotFound => Ok(None),
            GetOutcome::Found { value, revision } => {
                let record: Session =
                    serde_json::from_slice(&value).map_err(|error| Error::Server(error.to_string()))?;
                if record.version != 1
                    || record.instance_id != instance
                    || record.boot_id.is_empty()
                    || record.boot_id.len() > 128
                    || record.epoch > revision
                {
                    return Err(Error::Server("invalid DiskDB process session".into()));
                }
                Ok(Some((record, revision)))
            }
        }
    }
    /// Capture the previous boot identity once, independently of heartbeat revisions.
    /// # Errors
    /// Rejects unsupported or malformed sessions and unavailable authority.
    pub async fn diskdb_session_predecessor(&self, instance: u64) -> Result<(Option<String>, u64)> {
        Ok(self
            .diskdb_session(instance)
            .await?
            .map_or((None, 0), |(record, _)| (Some(record.boot_id), record.epoch)))
    }
    /// Observe the stable accepted process epoch; zero means no accepted session.
    /// # Errors
    /// Rejects malformed sessions and unavailable authority.
    pub async fn diskdb_session_epoch(&self, instance: u64) -> Result<u64> {
        Ok(self
            .diskdb_session(instance)
            .await?
            .map_or(0, |(record, _)| record.epoch))
    }
    /// Claim a boot from one fixed predecessor identity. Heartbeats can advance
    /// its record revision, while a different boot cannot be silently replaced.
    /// # Errors
    /// Rejects changed predecessors, invalid identity and uncertain authority.
    pub async fn claim_diskdb_session(
        &self,
        instance: u64,
        boot: &str,
        previous_boot: Option<&str>,
        previous_epoch: u64,
    ) -> Result<u64> {
        if boot.is_empty() || boot.len() > 128 {
            return Err(Error::Server("invalid DiskDB boot identity".into()));
        }
        for _ in 0..10 {
            let current = self.diskdb_session(instance).await?;
            let revision = current.as_ref().map_or(0, |(_, revision)| *revision);
            let epoch = if let Some((record, _)) = &current {
                if record.boot_id == boot {
                    if record.epoch != 0 {
                        return Ok(record.epoch);
                    }
                    revision
                } else {
                    if Some(record.boot_id.as_str()) != previous_boot || record.epoch != previous_epoch {
                        return Err(replaced());
                    }
                    0
                }
            } else {
                if previous_boot.is_some() || previous_epoch != 0 {
                    return Err(replaced());
                }
                0
            };
            let payload = serde_json::to_vec(&Session {
                version: 1,
                instance_id: instance,
                boot_id: boot.into(),
                epoch,
            })
            .map_err(|error| Error::Server(error.to_string()))?;
            match self
                .kv
                .put_cas(0, 0, key(instance).as_bytes(), &payload, revision)
                .await
            {
                Ok(_) | Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::OutcomeUnknown)
    }
    /// Publish liveness only from the current boot, without rewriting its epoch.
    /// # Errors
    /// Rejects old sessions and propagates failed conditional publication.
    pub async fn heartbeat_diskdb_session(
        &self,
        instance: u64,
        endpoint: &str,
        groups: &[u64],
        usage: &[DiskGroupUsageSummary],
        epoch: u64,
    ) -> Result<()> {
        let value = InstanceValue {
            instance_id: instance,
            rpc_endpoint: endpoint.into(),
            last_heartbeat_ms: now_ms(),
            extra: Some(ServiceExtra {
                diskdb: Some(DiskdbExtra {
                    diskio_scope: None,
                    rack_id: None,
                    node_id: None,
                    owned_dg_ids: groups.to_vec(),
                    group_usages: usage.to_vec(),
                }),
                kv_server: None,
                chunk_kv: None,
            }),
        };
        let payload = serde_json::to_vec(&value).map_err(|error| Error::Server(error.to_string()))?;
        let operation = BatchOp::Put {
            key: InstanceKey {
                service: "diskdb".into(),
                instance_id: instance,
            }
            .to_path()
            .into_bytes()
            .into(),
            value: payload.into(),
        };
        for _ in 0..10 {
            let (record, revision) = self.diskdb_session(instance).await?.ok_or_else(replaced)?;
            if epoch == 0 || record.epoch != epoch {
                return Err(replaced());
            }
            let operations = [
                operation.clone(),
                BatchOp::Put {
                    key: key(instance).into_bytes().into(),
                    value: serde_json::to_vec(&record)
                        .map_err(|error| Error::Server(error.to_string()))?
                        .into(),
                },
            ];
            match self
                .kv
                .batch_write_cas(0, 0, &operations, key(instance).as_bytes(), revision)
                .await
            {
                Ok(_) => return Ok(()),
                Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::OutcomeUnknown)
    }
}
