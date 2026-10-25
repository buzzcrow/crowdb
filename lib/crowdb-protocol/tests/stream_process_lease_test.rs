// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{
    StreamName, StreamProcessIdentity, StreamProcessLease, StreamPurpose, StreamWriterOwner,
};
use crowdb_protocol::key::{StreamProcessLeaseKey, StreamWriterOwnerKey, TextKey};

#[test]
fn owner_keys_and_records_keep_purpose_instance_and_process_identity() {
    for purpose in [StreamPurpose::Wal, StreamPurpose::Stream] {
        let key = StreamProcessLeaseKey {
            purpose,
            instance_id: 7,
        };
        assert_eq!(StreamProcessLeaseKey::from_path(&key.to_path()).unwrap(), key);
        let record = StreamProcessLease {
            identity: StreamProcessIdentity {
                purpose,
                instance_id: 7,
                process_epoch: 1,
                boot_nonce: [1; 16],
            },
            expires_at_ms: 2_000,
            prior_exclusion_deadline_ms: 0,
            grant_sequence: 0,
            grant_expires_at_ms: 0,
        };
        record.validate().unwrap();
        let value = serde_json::to_value(&record).unwrap();
        assert_eq!(
            serde_json::from_value::<StreamProcessLease>(value.clone()).unwrap(),
            record
        );
        let mut successor = record.clone();
        successor.identity.process_epoch = 2;
        assert!(successor.validate().is_err());
        successor.prior_exclusion_deadline_ms = record.expires_at_ms;
        successor.validate().unwrap();
        let mut unknown = value;
        unknown
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), 1.into());
        assert!(serde_json::from_value::<StreamProcessLease>(unknown).is_err());
    }
}

#[test]
fn business_stream_processes_cannot_claim_wal_grant_fencing_fields() {
    let mut record = StreamProcessLease {
        identity: StreamProcessIdentity {
            purpose: StreamPurpose::Stream,
            instance_id: 7,
            process_epoch: 1,
            boot_nonce: [1; 16],
        },
        expires_at_ms: 2_000,
        prior_exclusion_deadline_ms: 0,
        grant_sequence: 1,
        grant_expires_at_ms: 3_000,
    };
    assert!(record.validate().is_err());
    record.identity.purpose = StreamPurpose::Wal;
    record.validate().unwrap();
    record.grant_expires_at_ms = 0;
    assert!(record.validate().is_err());
}

#[test]
fn writer_assignment_requires_a_complete_unique_identity() {
    let record = StreamWriterOwner {
        stream_name: StreamName { high: 12, low: 34 },
        writer_epoch: 3,
        process: StreamProcessIdentity {
            purpose: StreamPurpose::Stream,
            instance_id: 7,
            process_epoch: 1,
            boot_nonce: [1; 16],
        },
        assignment_nonce: [2; 16],
        released: false,
    };
    record.validate().unwrap();
    let key = StreamWriterOwnerKey {
        stream_name: record.stream_name,
    };
    assert_eq!(StreamWriterOwnerKey::from_path(&key.to_path()).unwrap(), key);
    assert_eq!(
        serde_json::from_slice::<StreamWriterOwner>(&serde_json::to_vec(&record).unwrap()).unwrap(),
        record
    );
    for field in 0..5 {
        let mut invalid = record.clone();
        match field {
            0 => invalid.writer_epoch = 0,
            1 => invalid.process.instance_id = 0,
            2 => invalid.process.process_epoch = 0,
            3 => invalid.process.boot_nonce = [0; 16],
            _ => invalid.assignment_nonce = [0; 16],
        }
        assert!(invalid.validate().is_err());
    }
}
