// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv_client::BatchOp;
use crowdb_protocol::chunk_stream::{
    encode_stream_directory_head, encode_stream_publication_guard, stream_publication_id,
    StreamDirectoryHead, StreamPublicationGuard,
};
use crowdb_protocol::key::{
    BinaryKey, StreamDirectoryRootKey, StreamManifestHeadKey, StreamPublicationGuardKey,
};

use super::{kv_error, DirectoryHeadSnapshot, DirectoryMetadataStore, GetOutcome};
use crate::{DirectoryMutation, Result, StreamError};

impl DirectoryMetadataStore {
    /// Store protected immutable pages, then atomically retain their published
    /// root, replace the head and remove its guard. Ordinary append does not call this.
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
        if expected.is_none() {
            if let Some(current) = self.load_directory_head(head.stream_name).await? {
                if current.head == head {
                    return Ok(current);
                }
                return Err(if current.head.writer_epoch > head.writer_epoch {
                    StreamError::StaleWriter
                } else {
                    StreamError::WriteStalled
                });
            }
        }
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
        self.store_directory_pages(mutation).await?;
        let bytes = encode_stream_directory_head(&head).map_err(codec_error)?;
        let root_key = StreamDirectoryRootKey {
            stream_name: head.stream_name,
            identity: head.identity(),
        }
        .to_bytes();
        self.validate_directory_root_record(&root_key, &bytes).await?;
        self.replace_directory_head(expected, head, bytes, guard_key)
            .await
    }

    async fn validate_directory_root_record(&self, key: &[u8], bytes: &[u8]) -> Result<()> {
        match self.records.get(key).await.map_err(kv_error)? {
            GetOutcome::NotFound => Ok(()),
            GetOutcome::Found { value, .. } if value.as_ref() == bytes => Ok(()),
            GetOutcome::Found { .. } => Err(StreamError::Corruption(
                "immutable directory root contains conflicting bytes".into(),
            )),
        }
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
        let root_key = StreamDirectoryRootKey {
            stream_name: head.stream_name,
            identity: head.identity(),
        }
        .to_bytes();
        let value = Bytes::from(bytes);
        let ops = [
            Self::directory_control_op(&head)?,
            BatchOp::Put {
                key: Bytes::from(root_key),
                value: value.clone(),
            },
            BatchOp::Put {
                key: Bytes::copy_from_slice(&key),
                value,
            },
            BatchOp::Delete {
                key: Bytes::from(guard_key),
            },
        ];
        match self
            .records
            .batch_write_cas(&ops, &key, expected.map_or(0, |snapshot| snapshot.revision))
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
