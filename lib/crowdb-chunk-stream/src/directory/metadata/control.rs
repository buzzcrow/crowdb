// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv_client::BatchOp;
use crowdb_protocol::chunk_stream::{StreamDirectoryControl, StreamDirectoryHead};
use crowdb_protocol::key::{BinaryKey, StreamDirectoryControlKey};

use super::{kv_error, DirectoryMetadataStore, GetOutcome};
use crate::{Result, StreamError};

impl DirectoryMetadataStore {
    pub(super) fn directory_control_op(head: &StreamDirectoryHead) -> Result<BatchOp> {
        Self::directory_control_scope_op(StreamDirectoryControl {
            stream_name: head.stream_name,
            purpose: head.purpose,
        })
    }

    pub(super) fn directory_control_scope_op(scope: StreamDirectoryControl) -> Result<BatchOp> {
        let key = StreamDirectoryControlKey {
            stream_name: scope.stream_name,
        }
        .to_bytes();
        let value = scope
            .encode()
            .map_err(|error| StreamError::Corruption(error.to_string()))?;
        Ok(BatchOp::Put {
            key: Bytes::from(key),
            value: Bytes::from(value),
        })
    }

    pub(super) async fn protect_directory_candidate(
        &self,
        head: &StreamDirectoryHead,
        guard_key: &[u8],
        bytes: &[u8],
    ) -> Result<()> {
        self.protect_directory_records(
            StreamDirectoryControl {
                stream_name: head.stream_name,
                purpose: head.purpose,
            },
            guard_key,
            bytes,
        )
        .await
    }

    pub(super) async fn protect_directory_records(
        &self,
        scope: StreamDirectoryControl,
        guard_key: &[u8],
        bytes: &[u8],
    ) -> Result<()> {
        let (control, revision) = self.load_directory_control(scope).await?;
        let BatchOp::Put { key, .. } = &control else {
            unreachable!()
        };
        if self.directory_guard_matches(guard_key, bytes).await? {
            if revision == 0 {
                // Another publisher may have created control and guard together
                // between these two reads. A fresh observation must confirm it.
                return Err(StreamError::Backpressure);
            }
            return Ok(());
        }
        let ops = [
            control.clone(),
            BatchOp::Put {
                key: Bytes::copy_from_slice(guard_key),
                value: Bytes::copy_from_slice(bytes),
            },
        ];
        match self.records.batch_write_cas(&ops, key, revision).await {
            Ok(_) => Ok(()),
            Err(crowdb_kv_client::Error::CasBusy) => Err(StreamError::Backpressure),
            Err(
                error @ (crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::OutcomeUnknown),
            ) => {
                if self.directory_guard_matches(guard_key, bytes).await? {
                    return Ok(());
                }
                if matches!(error, crowdb_kv_client::Error::OutcomeUnknown) {
                    Err(StreamError::WriteStalled)
                } else {
                    Err(StreamError::Backpressure)
                }
            }
            Err(error) => Err(kv_error(error)),
        }
    }

    pub(super) async fn load_directory_control(
        &self,
        scope: StreamDirectoryControl,
    ) -> Result<(BatchOp, u64)> {
        let control = Self::directory_control_scope_op(scope)?;
        let BatchOp::Put { key, value } = &control else {
            unreachable!()
        };
        let revision = match self.records.get(key).await.map_err(kv_error)? {
            GetOutcome::Found {
                value: observed,
                revision,
            } if observed == *value => revision,
            GetOutcome::Found { .. } => {
                return Err(StreamError::Corruption(
                    "directory control scope is invalid".into(),
                ))
            }
            GetOutcome::NotFound => 0,
        };
        Ok((control, revision))
    }

    async fn directory_guard_matches(&self, key: &[u8], bytes: &[u8]) -> Result<bool> {
        match self.records.get(key).await.map_err(kv_error)? {
            GetOutcome::Found { value, .. } if value.as_ref() == bytes => Ok(true),
            GetOutcome::Found { .. } => Err(StreamError::Corruption(
                "publication guard has conflicting bytes".into(),
            )),
            GetOutcome::NotFound => Ok(false),
        }
    }
}
