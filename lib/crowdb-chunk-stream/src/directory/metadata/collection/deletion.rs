// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv_client::BatchOp;
use crowdb_protocol::key::{BinaryKey, StreamDirectoryPageKey};

use super::DirectoryPageCollection;
use crate::directory::metadata::{kv_error, GetOutcome};
use crate::{DirectoryMetadataStore, Result, StreamError};

impl DirectoryMetadataStore {
    pub(super) async fn delete_unreachable_directory_page(
        &self,
        control: BatchOp,
        cursor: &DirectoryPageCollection,
    ) -> Result<()> {
        let page_key = StreamDirectoryPageKey {
            stream_name: cursor.scope.stream_name,
            page_id: cursor.target.id,
        }
        .to_bytes();
        let ops = [
            control,
            BatchOp::Delete {
                key: Bytes::copy_from_slice(&page_key),
            },
        ];
        let BatchOp::Put { key, .. } = &ops[0] else {
            unreachable!()
        };
        let result = match self
            .records
            .batch_write_cas(&ops, key, cursor.control_revision)
            .await
        {
            Ok(_) => Ok(()),
            Err(crowdb_kv_client::Error::CasBusy | crowdb_kv_client::Error::CasFailed { .. }) => {
                Err(StreamError::Backpressure)
            }
            Err(crowdb_kv_client::Error::OutcomeUnknown) => {
                match self.records.get(&page_key).await.map_err(kv_error)? {
                    GetOutcome::NotFound => Ok(()),
                    GetOutcome::Found { .. } => Err(StreamError::WriteStalled),
                }
            }
            Err(error) => Err(kv_error(error)),
        };
        if result.is_ok() {
            self.directory_pages
                .invalidate(cursor.scope.stream_name, cursor.target.id);
        }
        result
    }
}
