// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{
    inspection::{FieldMetadata, FieldStorageKind},
    FieldDefinition, FieldLocator, SnapshotId,
};
use crowdb_chunk_kv_client::{ClientError, ComposedItemError};
use crowdb_protocol::chunk_kv::{BatchMutationResult, ChunkKvResponse, OperationResult, RpcFailure};
use serde_json::{json, Value};

pub const MAX_ROWS: usize = 16;
pub const MAX_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub enum TestProfile {
    Metadata,
    LargeValue,
}

impl TestProfile {
    pub fn name(self) -> &'static str {
        match self {
            Self::Metadata => "schema_membership_field_metadata",
            Self::LargeValue => "inline_64k_payload",
        }
    }
    pub fn rows(self) -> usize {
        match self {
            Self::Metadata => 256,
            Self::LargeValue => 16,
        }
    }
}

pub struct TestRows {
    pub profile: TestProfile,
    prefix: Vec<u8>,
    snapshot: SnapshotId,
}

pub struct TestRow {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl TestRow {
    pub fn size(&self) -> usize {
        self.key.len() + self.value.len()
    }
}

impl TestRows {
    pub fn new(profile: TestProfile, run: &str, trial: usize, mode: char) -> Self {
        Self {
            profile,
            prefix: format!("measurement/write-batch/{run}/{trial:04}/{mode}/").into_bytes(),
            snapshot: SnapshotId::from_bytes(&[7; 16]).unwrap(),
        }
    }

    pub fn key(&self, index: usize) -> Vec<u8> {
        [self.prefix.as_slice(), format!("{index:08}").as_bytes()].concat()
    }

    pub fn row(&self, index: usize) -> TestRow {
        // Values use the public Dataset row encodings, but the isolated keys do
        // not publish a Dataset resource, inspection root or user-visible head.
        let value = match (self.profile, index % 3) {
            (TestProfile::Metadata, 0) => serde_json::to_vec(&FieldDefinition {
                name: "body".into(),
                required: true,
            })
            .unwrap(),
            (TestProfile::Metadata, 1) => {
                serde_json::to_vec(&format!("sample-{index:08}").into_bytes()).unwrap()
            }
            (TestProfile::Metadata, _) => serde_json::to_vec(&FieldMetadata {
                name: "body".into(),
                storage: FieldStorageKind::Inline,
                length: 64,
                md5: Some([7; 16]),
                source_snapshot: self.snapshot,
            })
            .unwrap(),
            (TestProfile::LargeValue, _) => serde_json::to_vec(&FieldLocator::Inline {
                value: vec![255; 64 * 1024],
                md5: [7; 16],
            })
            .unwrap(),
        };
        assert!(value.len() <= 1024 * 1024);
        let row = TestRow {
            key: self.key(index),
            value,
        };
        assert!(row.size() <= MAX_BYTES);
        row
    }
}

#[derive(Default)]
pub struct TestOutcomes {
    pub applied: usize,
    pub same_value: usize,
    pub conflicts: usize,
    pub server_errors: usize,
    pub client_errors: usize,
    pub malformed: usize,
    pub not_attempted: usize,
    pub logical_bytes: usize,
    pub peak_rows: usize,
    pub peak_bytes: usize,
    pub errors: Vec<String>,
}

impl TestOutcomes {
    pub fn clean(&self) -> bool {
        self.same_value + self.conflicts + self.server_errors + self.client_errors + self.malformed == 0
    }

    pub fn admitted(&mut self, rows: usize, bytes: usize) {
        assert!(rows <= MAX_ROWS && bytes <= MAX_BYTES);
        self.peak_rows = self.peak_rows.max(rows);
        self.peak_bytes = self.peak_bytes.max(bytes);
    }

    pub fn record(&mut self, result: Result<OperationResult, TestRowError>, row: &TestRow) {
        self.logical_bytes += row.size();
        match result {
            Ok(OperationResult::Mutation {
                applied: true,
                revision: Some(revision),
                ..
            }) if revision > 0 => self.applied += 1,
            Ok(OperationResult::Mutation {
                applied: false,
                observed,
                ..
            }) => {
                if observed
                    .as_ref()
                    .is_some_and(|observed| observed.key == row.key && observed.value == row.value)
                {
                    self.same_value += 1;
                } else {
                    self.conflicts += 1;
                }
                self.errors
                    .push("fresh-key create was not applied; no confirmation GET performed".into());
            }
            Ok(other) => {
                self.malformed += 1;
                self.errors.push(format!("unexpected mutation shape: {other:?}"));
            }
            Err(TestRowError::Server(error)) => {
                self.server_errors += 1;
                self.errors.push(format!("typed server failure: {error:?}"));
            }
            Err(TestRowError::Client(error)) => {
                self.client_errors += 1;
                self.errors
                    .push(format!("client failure; mutation may have applied: {error:?}"));
            }
            Err(TestRowError::Malformed(error)) => {
                self.malformed += 1;
                self.errors.push(error);
            }
        }
    }

    pub fn json(&self) -> Value {
        json!({"applied":self.applied, "same_value_conflicts":self.same_value, "conflicts":self.conflicts,
            "typed_server_errors":self.server_errors, "client_errors_outcome_uncertain":self.client_errors,
            "malformed":self.malformed, "not_attempted":self.not_attempted,
            "logical_input_bytes":self.logical_bytes, "peak_pending_rows":self.peak_rows,
            "peak_pending_logical_bytes":self.peak_bytes, "original_errors":self.errors})
    }
}

pub enum TestRowError {
    Server(RpcFailure),
    Client(ClientError),
    Malformed(String),
}

pub fn point_result(
    result: crowdb_chunk_kv_client::Result<ChunkKvResponse>,
) -> Result<OperationResult, TestRowError> {
    result
        .map_err(TestRowError::Client)?
        .result
        .map_err(TestRowError::Server)
}

pub fn batch_result(
    result: Result<BatchMutationResult, ComposedItemError>,
) -> Result<OperationResult, TestRowError> {
    match result {
        Ok(value) => value.result.map_err(TestRowError::Server),
        Err(ComposedItemError::Client(error)) => Err(TestRowError::Client(error)),
        Err(ComposedItemError::Server(error)) => Err(TestRowError::Server(error)),
    }
}
