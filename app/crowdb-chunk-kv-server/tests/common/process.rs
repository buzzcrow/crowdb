// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::{StreamProcessAuthority, StreamProcessLeaseTiming};
use crowdb_protocol::chunk_stream::{StreamProcessIdentity, StreamProcessLease, StreamPurpose};
use std::{sync::Arc, time::Instant};

pub struct TestProcess;

impl TestProcess {
    pub fn authority(instance_id: u64) -> Arc<StreamProcessAuthority> {
        Arc::new(
            StreamProcessAuthority::new(
                StreamProcessLease {
                    identity: StreamProcessIdentity {
                        purpose: StreamPurpose::Wal,
                        instance_id,
                        process_epoch: 1,
                        boot_nonce: [1; 16],
                    },
                    expires_at_ms: 3_601_000,
                    prior_exclusion_deadline_ms: 0,
                    grant_sequence: 0,
                    grant_expires_at_ms: 0,
                },
                StreamProcessLeaseTiming::default(),
                1_000,
                Instant::now(),
            )
            .unwrap(),
        )
    }
}
