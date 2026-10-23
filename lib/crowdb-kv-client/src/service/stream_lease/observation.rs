// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Observation publication fenced by the same process lease as serving grants.

use super::{invalid, Result, StreamProcessLeaseClient, StreamProcessLeaseError};
use crate::{BatchOp, Error};
use crowdb_protocol::{
    chunk_stream::{StreamProcessIdentity, StreamPurpose},
    common::{ChunkKvExtra, InstanceValue, ServiceExtra},
    key::{InstanceKey, StreamProcessLeaseKey, TextKey},
};

impl StreamProcessLeaseClient {
    /// Publish one WAL owner's observation only while its exact boot is current.
    /// # Errors
    /// Rejects expired/replaced boots, malformed observations and unavailable CAS.
    pub async fn publish_chunk_kv_observation(
        &self,
        identity: StreamProcessIdentity,
        endpoint: &str,
        observation: &ChunkKvExtra,
        now_ms: u64,
    ) -> Result<()> {
        if identity.purpose != StreamPurpose::Wal || observation.process_epoch != identity.process_epoch {
            return Err(invalid("observation differs from its WAL process"));
        }
        let value = InstanceValue {
            instance_id: identity.instance_id,
            rpc_endpoint: endpoint.into(),
            last_heartbeat_ms: now_ms,
            extra: Some(ServiceExtra {
                diskdb: None,
                kv_server: None,
                chunk_kv: Some(observation.clone()),
            }),
        };
        let observation_op = BatchOp::Put {
            key: InstanceKey {
                service: "chunk-kv".into(),
                instance_id: identity.instance_id,
            }
            .to_path()
            .into_bytes()
            .into(),
            value: serde_json::to_vec(&value)
                .map_err(|error| invalid(&error.to_string()))?
                .into(),
        };
        self.mutate_observation(identity, observation_op, now_ms).await
    }

    /// Withdraw discovery without deleting the persistent process/expiry fence.
    /// # Errors
    /// A replaced boot cannot withdraw its successor's observation.
    pub async fn withdraw_chunk_kv_observation(
        &self,
        identity: StreamProcessIdentity,
        now_ms: u64,
    ) -> Result<()> {
        if identity.purpose != StreamPurpose::Wal {
            return Err(invalid("not a WAL process"));
        }
        let op = BatchOp::Delete {
            key: InstanceKey {
                service: "chunk-kv".into(),
                instance_id: identity.instance_id,
            }
            .to_path()
            .into_bytes()
            .into(),
        };
        self.mutate_observation(identity, op, now_ms).await
    }

    async fn mutate_observation(
        &self,
        identity: StreamProcessIdentity,
        observation_op: BatchOp,
        now_ms: u64,
    ) -> Result<()> {
        let lease_path = StreamProcessLeaseKey {
            purpose: identity.purpose,
            instance_id: identity.instance_id,
        }
        .to_path();
        for _ in 0..8 {
            let Some((lease, revision)) = self.read(identity.purpose, identity.instance_id).await? else {
                return Err(StreamProcessLeaseError::Replaced);
            };
            if lease.identity != identity {
                return Err(StreamProcessLeaseError::Replaced);
            }
            if now_ms >= lease.expires_at_ms {
                return Err(StreamProcessLeaseError::Expired);
            }
            let ops = [
                observation_op.clone(),
                BatchOp::Put {
                    key: lease_path.clone().into_bytes().into(),
                    value: serde_json::to_vec(&lease)
                        .map_err(|error| invalid(&error.to_string()))?
                        .into(),
                },
            ];
            match self
                .kv
                .batch_write_cas(0, 0, &ops, lease_path.as_bytes(), revision)
                .await
            {
                Ok(_) => return Ok(()),
                Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(StreamProcessLeaseError::OutcomeUnknown)
    }
}
