// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{StreamProcessIdentity, StreamProcessLease, StreamPurpose};

pub struct TestStreamLeaseRecord;

impl TestStreamLeaseRecord {
    pub fn at(expires_at_ms: u64) -> StreamProcessLease {
        StreamProcessLease {
            identity: StreamProcessIdentity {
                purpose: StreamPurpose::Wal,
                instance_id: 7,
                process_epoch: 1,
                boot_nonce: [1; 16],
            },
            expires_at_ms,
            prior_exclusion_deadline_ms: 0,
            grant_sequence: 0,
            grant_expires_at_ms: 0,
        }
    }
}
