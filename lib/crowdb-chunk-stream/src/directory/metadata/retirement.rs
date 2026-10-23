// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded retirement of published root-index entries, independently of pins.

use bytes::Bytes;
use crowdb_kv_client::BatchOp;
use crowdb_protocol::chunk_stream::StreamDirectoryControl;
use crowdb_protocol::key::{BinaryKey, StreamDirectoryRootKey};

use super::{kv_error, DirectoryHeadSnapshot, DirectoryMetadataStore};
use crate::{Result, StreamError, MAX_DIRECTORY_SCAN_KEYS};

impl DirectoryMetadataStore {
    /// Retires at most `max_roots` oldest root-index records below the caller's
    /// retention horizon. Pins retain their own exact descriptors. This never
    /// deletes directory pages, chunk spans or physical data.
    ///
    /// # Errors
    /// Rejects an invalid horizon/budget, stale head, malformed root, missing
    /// control or concurrent retention change. Retry with a fresh head snapshot.
    pub async fn retire_directory_roots_before(
        &self,
        expected: &DirectoryHeadSnapshot,
        retained_generation: u64,
        max_roots: usize,
    ) -> Result<usize> {
        if expected.store_id != self.store_id
            || expected.head.metadata_group_id != self.group_id
            || retained_generation == 0
            || retained_generation > expected.head.generation
            || max_roots == 0
            || max_roots > MAX_DIRECTORY_SCAN_KEYS
        {
            return Err(StreamError::InvalidRequest(
                "root retirement binding, horizon or budget is invalid".into(),
            ));
        }
        let name = expected.head.stream_name;
        let (control, revision) = self
            .load_directory_control(StreamDirectoryControl {
                stream_name: name,
                purpose: expected.head.purpose,
            })
            .await?;
        if revision == 0 {
            return Err(StreamError::ReadUnavailable(
                "root retention control is missing".into(),
            ));
        }
        let current = self.load_directory_head(name).await?;
        if !current.is_some_and(|head| head.revision == expected.revision && head.head == expected.head) {
            return Err(StreamError::Backpressure);
        }
        let candidates = self.scan_directory_roots(name, None, max_roots).await?;
        let mut keys = Vec::with_capacity(candidates.entries.len());
        for identity in candidates.entries {
            if identity.generation >= retained_generation {
                continue;
            }
            let Some(root) = self.load_directory_root(name, identity).await? else {
                return Err(StreamError::Backpressure);
            };
            if root.purpose != expected.head.purpose || root.writer_epoch > expected.head.writer_epoch {
                return Err(StreamError::Corruption("retired root scope is invalid".into()));
            }
            keys.push(
                StreamDirectoryRootKey {
                    stream_name: name,
                    identity,
                }
                .to_bytes(),
            );
        }
        self.delete_retention_records(control, revision, &keys).await?;
        Ok(keys.len())
    }

    pub(super) async fn delete_retention_records(
        &self,
        control: BatchOp,
        revision: u64,
        keys: &[Vec<u8>],
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut ops = Vec::with_capacity(keys.len() + 1);
        ops.push(control);
        ops.extend(keys.iter().map(|key| BatchOp::Delete {
            key: Bytes::copy_from_slice(key),
        }));
        let BatchOp::Put { key, .. } = &ops[0] else {
            unreachable!()
        };
        match self.records.batch_write_cas(&ops, key, revision).await {
            Ok(_) => Ok(()),
            Err(crowdb_kv_client::Error::CasBusy | crowdb_kv_client::Error::CasFailed { .. }) => {
                Err(StreamError::Backpressure)
            }
            Err(crowdb_kv_client::Error::OutcomeUnknown) => {
                for key in keys {
                    if !matches!(
                        self.records.get(key).await.map_err(kv_error)?,
                        super::GetOutcome::NotFound
                    ) {
                        return Err(StreamError::WriteStalled);
                    }
                }
                Ok(())
            }
            Err(error) => Err(kv_error(error)),
        }
    }
}
