// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_paxos_kv_server::group0_control_plane::Group0ControlPlane;
use crowdb_protocol::{
    chunk_stream::{StreamProcessIdentity, StreamProcessLease, StreamPurpose},
    key::{StreamProcessLeaseKey, TextKey},
};

pub struct TestProcessLease;

impl TestProcessLease {
    pub fn idle_instance(instance_id: u64, endpoint: &str) -> crowdb_protocol::common::InstanceValue {
        use crowdb_protocol::common::{ChunkKvExtra, InstanceValue, ServiceExtra};
        InstanceValue {
            instance_id,
            rpc_endpoint: endpoint.into(),
            last_heartbeat_ms: u64::MAX,
            extra: Some(ServiceExtra {
                chunk_kv: Some(ChunkKvExtra {
                    process_epoch: 1,
                    ..ChunkKvExtra::default()
                }),
                ..ServiceExtra::default()
            }),
        }
    }

    pub async fn seed(control: &Group0ControlPlane, instance_id: u64) {
        let lease = StreamProcessLease {
            identity: StreamProcessIdentity {
                purpose: StreamPurpose::Wal,
                instance_id,
                process_epoch: 1,
                boot_nonce: [1; 16],
            },
            expires_at_ms: u64::MAX,
            prior_exclusion_deadline_ms: 0,
            grant_sequence: 0,
            grant_expires_at_ms: 0,
        };
        control
            .compare_and_put(
                Bytes::from(
                    StreamProcessLeaseKey {
                        purpose: StreamPurpose::Wal,
                        instance_id,
                    }
                    .to_path(),
                ),
                Bytes::from(serde_json::to_vec(&lease).unwrap()),
                0,
            )
            .await
            .unwrap();
    }
}
