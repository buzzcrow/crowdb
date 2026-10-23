// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv_client::BatchOp;
use crowdb_protocol::chunk_stream::{
    decode_stream_sealed_chunk_span, encode_stream_sealed_chunk_span, StreamName, StreamPurpose,
    StreamSealedChunkSpan,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::key::{BinaryKey, StreamSealedChunkSpanKey};

use super::{kv_error, DirectoryKeyScan, DirectoryMetadataStore, GetOutcome};
use crate::{Result, StreamError};

impl DirectoryMetadataStore {
    /// # Errors
    /// Rejects wrong-key, corrupt or unavailable exact sealed-chunk bounds.
    pub async fn load_directory_span(
        &self,
        name: StreamName,
        chunk_id: ChunkId,
    ) -> Result<Option<StreamSealedChunkSpan>> {
        let key = StreamSealedChunkSpanKey {
            stream_name: name,
            chunk_id,
        }
        .to_bytes();
        match self.records.get(&key).await.map_err(kv_error)? {
            GetOutcome::NotFound => Ok(None),
            GetOutcome::Found { value, .. } => decode_stream_sealed_chunk_span(&value, name, chunk_id)
                .map(Some)
                .map_err(|error| StreamError::Corruption(error.to_string())),
        }
    }

    /// Enumerates exact sealed chunk identities without loading span bodies.
    /// # Errors
    /// Rejects invalid scan bounds, malformed keys or unavailable storage.
    pub async fn scan_directory_spans(
        &self,
        name: StreamName,
        after: Option<ChunkId>,
        limit: usize,
    ) -> Result<DirectoryKeyScan<ChunkId>> {
        let prefix = StreamSealedChunkSpanKey::stream_prefix(name);
        let after = after.map(|chunk_id| {
            StreamSealedChunkSpanKey {
                stream_name: name,
                chunk_id,
            }
            .to_bytes()
        });
        self.scan_directory_ids::<StreamSealedChunkSpanKey, _>(&prefix, after.as_deref(), limit, |key| {
            if key.stream_name != name
                || !(StreamPurpose::Wal.matches(key.chunk_id) || StreamPurpose::Stream.matches(key.chunk_id))
            {
                return Err(StreamError::Corruption(
                    "sealed chunk span key scope is invalid".into(),
                ));
            }
            Ok(key.chunk_id)
        })
        .await
    }

    pub(super) fn sealed_span_op(span: &StreamSealedChunkSpan) -> Result<BatchOp> {
        Ok(BatchOp::Put {
            key: Bytes::from(
                StreamSealedChunkSpanKey {
                    stream_name: span.stream_name,
                    chunk_id: span.chunk_id,
                }
                .to_bytes(),
            ),
            value: Bytes::from(
                encode_stream_sealed_chunk_span(span)
                    .map_err(|error| StreamError::Corruption(error.to_string()))?,
            ),
        })
    }

    pub(super) async fn validate_sealed_span_record(&self, span: &StreamSealedChunkSpan) -> Result<()> {
        let BatchOp::Put { key, value } = Self::sealed_span_op(span)? else {
            unreachable!()
        };
        match self.records.get(&key).await.map_err(kv_error)? {
            GetOutcome::NotFound => Ok(()),
            GetOutcome::Found { value: observed, .. } if observed == value => Ok(()),
            GetOutcome::Found { .. } => Err(StreamError::Corruption(
                "immutable sealed chunk span contains conflicting bytes".into(),
            )),
        }
    }
}
