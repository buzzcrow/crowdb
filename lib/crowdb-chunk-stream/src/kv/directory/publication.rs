// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv_client::BatchOp;
use crowdb_protocol::chunk_stream::{
    encode_stream_directory_head, encode_stream_publication_guard, stream_publication_id,
    StreamDirectoryHead, StreamPublicationGuard,
};
use crowdb_protocol::key::{
    BinaryKey, StreamDirectoryPageKey, StreamDirectoryRootKey, StreamManifestHeadKey,
    StreamPublicationGuardKey,
};
use futures::{stream, StreamExt, TryStreamExt};

use super::{kv_error, DirectoryHeadSnapshot, KvStreamMetadataStore};
use crate::{DirectoryMutation, Result, StreamError};

impl KvStreamMetadataStore {
    /// Store a protected path mutation, retain its immutable root, then atomically
    /// replace the head and remove its guard. No ordinary append calls this API.
    ///
    /// # Errors
    /// Rejects invalid candidates, conflicting immutable bytes, stale revision/epoch
    /// and unresolved outcomes. A failed publication leaves its durable guard intact.
    pub async fn publish_directory(
        &self,
        expected: Option<&DirectoryHeadSnapshot>,
        head: StreamDirectoryHead,
        mutation: &DirectoryMutation,
    ) -> Result<DirectoryHeadSnapshot> {
        if head.metadata_group_id != self.group_id
            || expected.is_some_and(|snapshot| snapshot.store_id != self.store_id)
        {
            return Err(StreamError::InvalidRequest(
                "directory metadata group differs from store binding".into(),
            ));
        }
        mutation
            .validate_head(self, &head, expected.map(|snapshot| &snapshot.head))
            .await?;
        let guard = StreamPublicationGuard {
            head: head.clone(),
            base: expected.map(|snapshot| snapshot.head.clone()),
            pages: mutation.pages().iter().map(|page| page.reference().id).collect(),
        };
        let guard_bytes = encode_stream_publication_guard(&guard).map_err(codec_error)?;
        let guard_key = StreamPublicationGuardKey {
            stream_name: head.stream_name,
            publication_id: stream_publication_id(&head).map_err(codec_error)?,
        }
        .to_bytes();
        self.protect_directory_candidate(&head, &guard_key, &guard_bytes)
            .await?;
        stream::iter(mutation.pages())
            .map(|page| async move {
                let key = StreamDirectoryPageKey {
                    stream_name: head.stream_name,
                    page_id: page.reference().id,
                }
                .to_bytes();
                self.put_directory_immutable(&key, page.bytes()).await
            })
            .buffer_unordered(8)
            .try_collect::<Vec<_>>()
            .await?;
        let bytes = encode_stream_directory_head(&head).map_err(codec_error)?;
        let root_key = StreamDirectoryRootKey {
            stream_name: head.stream_name,
            identity: head.identity(),
        }
        .to_bytes();
        self.put_directory_immutable(&root_key, &bytes).await?;
        self.replace_directory_head(expected, head, bytes, guard_key)
            .await
    }

    async fn replace_directory_head(
        &self,
        expected: Option<&DirectoryHeadSnapshot>,
        head: StreamDirectoryHead,
        bytes: Vec<u8>,
        guard_key: Vec<u8>,
    ) -> Result<DirectoryHeadSnapshot> {
        let key = StreamManifestHeadKey {
            stream_name: head.stream_name,
        }
        .to_bytes();
        let ops = [
            Self::directory_control_op(&head)?,
            BatchOp::Put {
                key: Bytes::copy_from_slice(&key),
                value: Bytes::from(bytes),
            },
            BatchOp::Delete {
                key: Bytes::from(guard_key),
            },
        ];
        match self
            .kv
            .batch_write_cas(
                self.store_id,
                self.group_id,
                &ops,
                &key,
                expected.map_or(0, |snapshot| snapshot.revision),
            )
            .await
        {
            Ok(outcome) => Ok(DirectoryHeadSnapshot {
                head,
                revision: outcome.revision,
                store_id: self.store_id,
            }),
            Err(crowdb_kv_client::Error::CasBusy) => Err(StreamError::Backpressure),
            Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::OutcomeUnknown) => {
                match self.load_directory_head(head.stream_name).await? {
                    Some(observed) if observed.head == head => Ok(observed),
                    Some(observed) if observed.head.writer_epoch > head.writer_epoch => {
                        Err(StreamError::StaleWriter)
                    }
                    _ => Err(StreamError::WriteStalled),
                }
            }
            Err(error) => Err(kv_error(error)),
        }
    }
}

fn codec_error(error: impl std::fmt::Display) -> StreamError {
    StreamError::Corruption(format!("directory publication codec failed: {error}"))
}
