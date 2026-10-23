// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv_client::BatchOp;
use crowdb_protocol::chunk_stream::{encode_stream_root_pin, StreamRootPin};
use crowdb_protocol::key::{BinaryKey, StreamRootPinKey};

use super::DirectoryRootPinSnapshot;
use crate::directory::metadata::{kv_error, DirectoryMetadataStore};
use crate::{Result, StreamError};

impl DirectoryMetadataStore {
    pub(super) async fn write_directory_pin(
        &self,
        control: BatchOp,
        revision: u64,
        before: &StreamRootPin,
        after: Option<&StreamRootPin>,
    ) -> Result<Option<DirectoryRootPinSnapshot>> {
        if revision == 0 {
            return Err(StreamError::ReadUnavailable(
                "root retention control is missing".into(),
            ));
        }
        let pin_key = StreamRootPinKey {
            stream_name: before.head.stream_name,
            pin_id: before.id,
        }
        .to_bytes();
        let pin_op = match after {
            Some(pin) => BatchOp::Put {
                key: Bytes::from(pin_key),
                value: Bytes::from(
                    encode_stream_root_pin(pin)
                        .map_err(|error| StreamError::Corruption(error.to_string()))?,
                ),
            },
            None => BatchOp::Delete {
                key: Bytes::from(pin_key),
            },
        };
        let BatchOp::Put { key, .. } = &control else {
            unreachable!()
        };
        let result = self
            .records
            .batch_write_cas(&[control.clone(), pin_op], key, revision)
            .await;
        match result {
            Ok(outcome) => Ok(after.map(|pin| DirectoryRootPinSnapshot {
                pin: pin.clone(),
                revision: outcome.revision,
                store_id: self.store_id,
            })),
            Err(crowdb_kv_client::Error::CasBusy) => Err(StreamError::Backpressure),
            Err(
                error @ (crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::OutcomeUnknown),
            ) => {
                let observed = self
                    .load_directory_pin(before.head.stream_name, before.id)
                    .await?;
                match (after, observed) {
                    (Some(expected), Some(actual)) if actual.pin() == expected => Ok(Some(actual)),
                    (None, None) => Ok(None),
                    _ if matches!(error, crowdb_kv_client::Error::OutcomeUnknown) => {
                        Err(StreamError::WriteStalled)
                    }
                    _ => Err(StreamError::Backpressure),
                }
            }
            Err(error) => Err(kv_error(error)),
        }
    }
}
