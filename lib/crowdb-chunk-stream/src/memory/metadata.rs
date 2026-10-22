// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Test metadata shares the conditional record map with indexed storage.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_protocol::chunk_stream::{
    decode_stream_manifest_record, encode_stream_manifest_record, StreamExtentPage, StreamManifest,
    StreamName,
};
use crowdb_protocol::key::{BinaryKey, StreamExtentPageKey, StreamManifestHeadKey};
use std::sync::{atomic::Ordering, Arc};

use super::{
    directory_records::{MemoryDirectoryRecords, MemoryRecord},
    MemoryState, MemoryStreamStore,
};
use crate::{DirectoryMetadataStore, Result, StreamError, StreamMetadataStore};

#[async_trait]
impl StreamMetadataStore for MemoryStreamStore {
    fn directory_metadata(&self, group_id: u64) -> Result<DirectoryMetadataStore> {
        DirectoryMetadataStore::new(
            Arc::new(MemoryDirectoryRecords::new(
                Arc::clone(&self.state),
                group_id,
                Arc::clone(&self.fail_next_publish),
                Arc::clone(&self.metadata_publishes),
            )),
            0,
            group_id,
        )
    }

    async fn load_current(&self, name: StreamName) -> Result<Option<StreamManifest>> {
        let state = self.state.lock().await;
        let Some(group_id) = state.metadata_heads.get(&name) else {
            return Ok(None);
        };
        let key = StreamManifestHeadKey { stream_name: name }.to_bytes();
        match state.metadata_records.get(&(*group_id, key)) {
            Some(record) => decode_stream_manifest_record(&record.value)
                .map(|record| Some(record.value))
                .map_err(|error| StreamError::Corruption(error.to_string())),
            None => Ok(None),
        }
    }

    async fn load_extent_page(
        &self,
        name: StreamName,
        epoch: u64,
        generation: u64,
        index: u64,
    ) -> Result<Option<StreamExtentPage>> {
        self.extent_page_loads.fetch_add(1, Ordering::AcqRel);
        Ok(self
            .state
            .lock()
            .await
            .pages
            .get(&(name, epoch, generation, index))
            .cloned())
    }

    async fn publish(
        &self,
        expected: Option<(u64, u64)>,
        manifest: StreamManifest,
        pages: Vec<StreamExtentPage>,
    ) -> Result<()> {
        if self.fail_next_publish.swap(false, Ordering::AcqRel) {
            return Err(StreamError::Internal(
                "injected metadata publication failure".into(),
            ));
        }
        let mut state = self.state.lock().await;
        let head_key = StreamManifestHeadKey {
            stream_name: manifest.stream_name,
        }
        .to_bytes();
        let current = match state
            .metadata_records
            .get(&(manifest.metadata_group_id, head_key))
        {
            Some(record) => Some(
                decode_stream_manifest_record(&record.value)
                    .map_err(|_| StreamError::StaleWriter)?
                    .value,
            ),
            None => None,
        };
        if current
            .as_ref()
            .is_some_and(|current| current.purpose != manifest.purpose)
        {
            return Err(StreamError::Corruption("stream purpose is immutable".into()));
        }
        if current
            .as_ref()
            .map(|current| (current.writer_epoch, current.generation))
            != expected
        {
            return Err(StreamError::StaleWriter);
        }
        publish_metadata_records(&mut state, &manifest, &pages)?;
        for page in pages {
            state.pages.insert(
                (
                    page.stream_name,
                    page.writer_epoch,
                    page.generation,
                    page.page_index,
                ),
                page,
            );
        }
        state.manifests.insert(manifest.stream_name, manifest);
        self.metadata_publishes.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    async fn reclaim_extent_pages_before(
        &self,
        name: StreamName,
        retained_generation: u64,
        max_pages: usize,
    ) -> Result<u64> {
        if retained_generation == 0 || max_pages == 0 {
            return Err(StreamError::InvalidRequest(
                "metadata reclaim watermark and page bound must be nonzero".into(),
            ));
        }
        let mut state = self.state.lock().await;
        let keys: Vec<_> = state
            .pages
            .keys()
            .filter(|(stream, _, generation, _)| *stream == name && *generation < retained_generation)
            .take(max_pages)
            .copied()
            .collect();
        let group = state.metadata_heads.get(&name).copied();
        for key in &keys {
            state.pages.remove(key);
            if let Some(group) = group {
                let bytes = StreamExtentPageKey {
                    stream_name: key.0,
                    writer_epoch: key.1,
                    generation: key.2,
                    page_index: key.3,
                }
                .to_bytes();
                state.metadata_records.remove(&(group, bytes));
            }
        }
        Ok(keys.len() as u64)
    }
}

fn publish_metadata_records(
    state: &mut MemoryState,
    manifest: &StreamManifest,
    pages: &[StreamExtentPage],
) -> Result<()> {
    let revision = state
        .metadata_revision
        .checked_add(1)
        .ok_or(StreamError::WriteStalled)?;
    let head_key = StreamManifestHeadKey {
        stream_name: manifest.stream_name,
    }
    .to_bytes();
    let head = encode_stream_manifest_record(manifest)
        .map_err(|error| StreamError::Corruption(error.to_string()))?;
    let encoded_pages: Vec<_> = pages
        .iter()
        .map(|page| {
            let key = StreamExtentPageKey {
                stream_name: page.stream_name,
                writer_epoch: page.writer_epoch,
                generation: page.generation,
                page_index: page.page_index,
            }
            .to_bytes();
            bincode::serialize(page)
                .map(|bytes| (key, Bytes::from(bytes)))
                .map_err(|error| StreamError::Corruption(error.to_string()))
        })
        .collect::<Result<_>>()?;
    state.metadata_revision = revision;
    state
        .metadata_heads
        .insert(manifest.stream_name, manifest.metadata_group_id);
    for (key, value) in encoded_pages {
        state.metadata_records.insert(
            (manifest.metadata_group_id, key),
            MemoryRecord { value, revision },
        );
    }
    state.metadata_records.insert(
        (manifest.metadata_group_id, head_key),
        MemoryRecord {
            value: Bytes::from(head),
            revision,
        },
    );
    Ok(())
}
