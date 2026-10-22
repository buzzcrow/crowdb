// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable extent-page validation and create-if-absent publication.

use super::{
    decode, encode, kv_error, GetOutcome, KvStreamMetadataStore, ReadMode, Result, StreamError,
    StreamExtentPage,
};
use crate::perf::{perf, PhaseTimer};
use crowdb_protocol::key::{BinaryKey, StreamExtentPageKey};

impl KvStreamMetadataStore {
    pub(super) async fn put_immutable_page(&self, page: &StreamExtentPage) -> Result<()> {
        let _timer = PhaseTimer::start(&perf().page_cas);
        if page.chunk_ids.is_empty()
            || page.logical_offsets.len() != page.chunk_ids.len() + 1
            || page.physical_offsets.len() != page.chunk_ids.len()
            || page
                .logical_offsets
                .windows(2)
                .any(|window| window[0] >= window[1])
        {
            return Err(StreamError::Corruption(
                "refusing to publish malformed extent page".into(),
            ));
        }
        let key = StreamExtentPageKey {
            stream_name: page.stream_name,
            writer_epoch: page.writer_epoch,
            generation: page.generation,
            page_index: page.page_index,
        }
        .to_bytes();
        let value = encode(page)?;
        match self
            .kv
            .put_cas(self.store_id, self.group_id, &key, &value, 0)
            .await
        {
            Ok(_) => Ok(()),
            Err(
                error @ (crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::OutcomeUnknown),
            ) => {
                match self
                    .kv
                    .get(self.store_id, self.group_id, &key, ReadMode::Linearizable, None)
                    .await
                    .map_err(kv_error)?
                {
                    GetOutcome::Found { value, .. } => {
                        let observed: StreamExtentPage = decode(&value)?;
                        if observed == *page {
                            Ok(())
                        } else {
                            tracing::warn!(
                                stream_high = page.stream_name.high,
                                stream_low = page.stream_name.low,
                                writer_epoch = page.writer_epoch,
                                generation = page.generation,
                                page_index = page.page_index,
                                candidate = ?page,
                                observed = ?observed,
                                "immutable extent-page key has conflicting contents"
                            );
                            Err(StreamError::Corruption(
                                "immutable extent-page key contains another value".into(),
                            ))
                        }
                    }
                    GetOutcome::NotFound => {
                        tracing::warn!(
                            stream_high = page.stream_name.high,
                            stream_low = page.stream_name.low,
                            writer_epoch = page.writer_epoch,
                            generation = page.generation,
                            page_index = page.page_index,
                            %error,
                            "immutable extent-page CAS was unresolved and its key is absent"
                        );
                        Err(StreamError::WriteStalled)
                    }
                }
            }
            Err(crowdb_kv_client::Error::CasBusy) => Err(StreamError::Backpressure),
            Err(error) => Err(kv_error(error)),
        }
    }
}
