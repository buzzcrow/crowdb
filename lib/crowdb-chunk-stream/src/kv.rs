// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Production registry and metadata adapters backed by CROWDB KV.

use std::sync::Arc;

use async_trait::async_trait;
use crowdb_kv_client::{CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_protocol::chunk_stream::{StreamBinding, StreamExtentPage, StreamManifest, StreamName};
use crowdb_protocol::key::{
    BinaryKey, StreamBindingKey, StreamExtentPageKey, StreamManifestHeadKey, TextKey,
};

use crate::{Result, StreamError, StreamMetadataStore, StreamRegistry};

/// Group-0 readable stream binding registry.
pub struct KvStreamRegistry {
    kv: Arc<CrowdbKvClient>,
}

impl KvStreamRegistry {
    #[must_use]
    pub fn new(kv: Arc<CrowdbKvClient>) -> Self {
        Self { kv }
    }
}

#[async_trait]
impl StreamRegistry for KvStreamRegistry {
    async fn load(&self, stream_name: StreamName) -> Result<Option<StreamBinding>> {
        let key = StreamBindingKey { stream_name }.to_path();
        match self
            .kv
            .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
            .await
            .map_err(kv_error)?
        {
            GetOutcome::Found { value, .. } => {
                let binding: StreamBinding = serde_json::from_slice(&value).map_err(|error| {
                    StreamError::Corruption(format!("stream binding decode failed: {error}"))
                })?;
                if binding.stream_name != stream_name || binding.metadata_group_id == 0 {
                    return Err(StreamError::Corruption(
                        "stream binding identity or metadata group is invalid".into(),
                    ));
                }
                Ok(Some(binding))
            }
            GetOutcome::NotFound => Ok(None),
        }
    }

    async fn create(&self, binding: StreamBinding) -> Result<()> {
        if binding.metadata_group_id == 0 || binding.binding_generation == 0 {
            return Err(StreamError::InvalidRequest(
                "stream binding requires a nonzero metadata group and generation".into(),
            ));
        }
        let key = StreamBindingKey {
            stream_name: binding.stream_name,
        }
        .to_path();
        let value = serde_json::to_vec(&binding)
            .map_err(|error| StreamError::Internal(format!("stream binding encode failed: {error}")))?;
        match self.kv.put_cas(0, 0, key.as_bytes(), &value, 0).await {
            Ok(_) => Ok(()),
            Err(crowdb_kv_client::Error::CasFailed { .. }) => Err(StreamError::InvalidRequest(
                "stream binding already exists".into(),
            )),
            Err(crowdb_kv_client::Error::CasBusy) => Err(StreamError::Backpressure),
            Err(crowdb_kv_client::Error::OutcomeUnknown) => match self.load(binding.stream_name).await? {
                Some(observed) if observed == binding => Ok(()),
                _ => Err(StreamError::WriteStalled),
            },
            Err(error) => Err(kv_error(error)),
        }
    }
}

/// One nonzero KV-group metadata store. Extent pages are immutable fresh keys;
/// the manifest is a stable revision-CAS head.
pub struct KvStreamMetadataStore {
    kv: Arc<CrowdbKvClient>,
    store_id: u64,
    group_id: u64,
}

impl KvStreamMetadataStore {
    /// Creates an adapter for one nonzero metadata group.
    ///
    /// # Errors
    ///
    /// Returns an error when group zero is selected for stream metadata.
    pub fn new(kv: Arc<CrowdbKvClient>, store_id: u64, group_id: u64) -> Result<Self> {
        if group_id == 0 {
            return Err(StreamError::InvalidRequest(
                "stream metadata must use a nonzero KV group".into(),
            ));
        }
        Ok(Self {
            kv,
            store_id,
            group_id,
        })
    }

    async fn get_head(&self, stream_name: StreamName) -> Result<Option<(StreamManifest, u64)>> {
        let key = StreamManifestHeadKey { stream_name }.to_bytes();
        match self
            .kv
            .get(self.store_id, self.group_id, &key, ReadMode::Linearizable, None)
            .await
            .map_err(kv_error)?
        {
            GetOutcome::Found { value, revision } => {
                let manifest: StreamManifest = decode(&value)?;
                if manifest.stream_name != stream_name || manifest.metadata_group_id != self.group_id {
                    return Err(StreamError::Corruption(
                        "stream manifest identity or metadata group is invalid".into(),
                    ));
                }
                Ok(Some((manifest, revision)))
            }
            GetOutcome::NotFound => Ok(None),
        }
    }

    async fn put_immutable_page(&self, page: &StreamExtentPage) -> Result<()> {
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
            Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::OutcomeUnknown) => {
                match self
                    .kv
                    .get(self.store_id, self.group_id, &key, ReadMode::Linearizable, None)
                    .await
                    .map_err(kv_error)?
                {
                    GetOutcome::Found { value, .. } if decode::<StreamExtentPage>(&value)? == *page => Ok(()),
                    _ => Err(StreamError::Corruption(
                        "immutable extent-page key contains another value".into(),
                    )),
                }
            }
            Err(crowdb_kv_client::Error::CasBusy) => Err(StreamError::Backpressure),
            Err(error) => Err(kv_error(error)),
        }
    }
}

#[async_trait]
impl StreamMetadataStore for KvStreamMetadataStore {
    async fn load_current(&self, stream_name: StreamName) -> Result<Option<StreamManifest>> {
        Ok(self.get_head(stream_name).await?.map(|(manifest, _)| manifest))
    }

    async fn load_extent_page(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
        generation: u64,
        page_index: u64,
    ) -> Result<Option<StreamExtentPage>> {
        let key = StreamExtentPageKey {
            stream_name,
            writer_epoch,
            generation,
            page_index,
        }
        .to_bytes();
        match self
            .kv
            .get(self.store_id, self.group_id, &key, ReadMode::Linearizable, None)
            .await
            .map_err(kv_error)?
        {
            GetOutcome::Found { value, .. } => {
                let page: StreamExtentPage = decode(&value)?;
                if page.stream_name != stream_name
                    || page.writer_epoch != writer_epoch
                    || page.generation != generation
                    || page.page_index != page_index
                {
                    return Err(StreamError::Corruption(
                        "stream extent-page identity is invalid".into(),
                    ));
                }
                Ok(Some(page))
            }
            GetOutcome::NotFound => Ok(None),
        }
    }

    async fn publish(
        &self,
        expected: Option<(u64, u64)>,
        manifest: StreamManifest,
        extent_pages: Vec<StreamExtentPage>,
    ) -> Result<()> {
        if manifest.metadata_group_id != self.group_id {
            return Err(StreamError::InvalidRequest(
                "manifest metadata group differs from store binding".into(),
            ));
        }
        if extent_pages.iter().any(|page| {
            page.stream_name != manifest.stream_name
                || page.writer_epoch != manifest.writer_epoch
                || page.generation != manifest.generation
        }) {
            return Err(StreamError::Corruption(
                "extent page identity differs from candidate manifest".into(),
            ));
        }
        for page in &extent_pages {
            self.put_immutable_page(page).await?;
        }
        let observed = self.get_head(manifest.stream_name).await?;
        if observed
            .as_ref()
            .map(|(head, _)| (head.writer_epoch, head.generation))
            != expected
        {
            if observed.as_ref().is_some_and(|(head, _)| head == &manifest) {
                return Ok(());
            }
            if observed
                .as_ref()
                .is_some_and(|(head, _)| head.writer_epoch > manifest.writer_epoch)
            {
                return Err(StreamError::StaleWriter);
            }
            return Err(StreamError::Internal(
                "stream manifest generation conflict".into(),
            ));
        }
        let expected_revision = observed.map_or(0, |(_, revision)| revision);
        let key = StreamManifestHeadKey {
            stream_name: manifest.stream_name,
        }
        .to_bytes();
        let value = encode(&manifest)?;
        match self
            .kv
            .put_cas(self.store_id, self.group_id, &key, &value, expected_revision)
            .await
        {
            Ok(_) => Ok(()),
            Err(crowdb_kv_client::Error::CasBusy) => Err(StreamError::Backpressure),
            Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::OutcomeUnknown) => {
                match self.get_head(manifest.stream_name).await? {
                    Some((current, _)) if current == manifest => Ok(()),
                    Some((current, _)) if current.writer_epoch > manifest.writer_epoch => {
                        Err(StreamError::StaleWriter)
                    }
                    _ => Err(StreamError::WriteStalled),
                }
            }
            Err(error) => Err(kv_error(error)),
        }
    }

    async fn reclaim_extent_pages_before(
        &self,
        stream_name: StreamName,
        retained_generation: u64,
        max_pages: usize,
    ) -> Result<u64> {
        if retained_generation == 0 || max_pages == 0 {
            return Err(StreamError::InvalidRequest(
                "metadata reclaim watermark and page bound must be nonzero".into(),
            ));
        }
        let prefix = StreamExtentPageKey::stream_prefix(stream_name);
        let limit = u32::try_from(max_pages).unwrap_or(u32::MAX);
        let candidates = self
            .kv
            .scan(
                self.store_id,
                self.group_id,
                &prefix,
                &[],
                &[],
                limit,
                ReadMode::Linearizable,
                None,
                true,
                None,
            )
            .await
            .map_err(kv_error)?;
        let mut reclaimed = 0_u64;
        for (key, _) in candidates.items {
            let page_key = StreamExtentPageKey::from_bytes(&key).map_err(|error| {
                StreamError::Corruption(format!("stream extent-page key decode failed: {error}"))
            })?;
            if page_key.stream_name == stream_name && page_key.generation < retained_generation {
                self.kv
                    .delete(self.store_id, self.group_id, &key, None)
                    .await
                    .map_err(kv_error)?;
                reclaimed += 1;
            }
        }
        Ok(reclaimed)
    }
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    bincode::serialize(value)
        .map_err(|error| StreamError::Internal(format!("stream metadata encode failed: {error}")))
}

fn decode<T: serde::de::DeserializeOwned>(value: &[u8]) -> Result<T> {
    bincode::deserialize(value)
        .map_err(|error| StreamError::Corruption(format!("stream metadata decode failed: {error}")))
}

#[allow(clippy::needless_pass_by_value)]
fn kv_error(error: crowdb_kv_client::Error) -> StreamError {
    StreamError::Internal(format!("stream KV operation failed: {error}"))
}
