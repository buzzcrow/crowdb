// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{sync::Arc, time::Duration};

use crowdb_chunk_kv_client::MultiScanOutcome;

use crate::{
    bucket::BucketError,
    metadata::{
        BucketId, ChunkKvMetadataStore, MetadataKey, MetadataRecordError, MultipartRepository,
        MultipartRepositoryError, TenantId,
    },
};

const UPLOAD_ITEMS: usize = 20;

struct BucketPosition {
    id: BucketId,
    after: Option<Vec<u8>>,
}

/// Discardable bucket and upload positions; all cleanup obligations stay in metadata.
pub struct MultipartExpirySweep {
    repository: MultipartRepository,
    bucket_start: Option<Vec<u8>>,
    current: Option<BucketPosition>,
    scanning_uploads: bool,
}

#[derive(Debug)]
pub struct MultipartExpiryStep {
    pub expired: usize,
    pub more: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum MultipartExpiryError {
    #[error(transparent)]
    Bucket(#[from] BucketError),
    #[error(transparent)]
    Upload(#[from] MultipartRepositoryError),
    #[error("multipart expiry step exceeded its local work budget")]
    WorkLimit,
    #[error("oversized bucket discovery row retained for later rounds")]
    OversizedBucketRow,
}

impl MultipartExpirySweep {
    #[must_use]
    pub fn new(store: Arc<ChunkKvMetadataStore>, tenant: TenantId) -> Self {
        Self {
            repository: MultipartRepository::new(store, tenant),
            bucket_start: None,
            current: None,
            scanning_uploads: false,
        }
    }

    /// Processes one tenant raw-key hint or one bounded upload-index page.
    ///
    /// # Errors
    /// Retains local raw-key progress across corrupt rows and uncertain mutations.
    /// A failed upload scan drops its bucket hint so other buckets can advance.
    pub async fn step(&mut self, now_ms: u64) -> Result<MultipartExpiryStep, MultipartExpiryError> {
        if self.scanning_uploads {
            self.current = None;
            self.scanning_uploads = false;
        }
        if let Ok(result) = tokio::time::timeout(Duration::from_secs(2), self.advance(now_ms)).await {
            result
        } else {
            if self.scanning_uploads {
                self.current = None;
                self.scanning_uploads = false;
            }
            Err(MultipartExpiryError::WorkLimit)
        }
    }

    async fn advance(&mut self, now_ms: u64) -> Result<MultipartExpiryStep, MultipartExpiryError> {
        if self.current.is_some() {
            return self.advance_uploads(now_ms).await;
        }
        let tenant = &self.repository.tenant;
        let start = self
            .bucket_start
            .clone()
            .unwrap_or_else(|| MetadataKey::tenant_prefix(tenant));
        let end = MetadataKey::tenant_end(tenant);
        if start >= end {
            return Ok(self.finish_round());
        }
        let page = self
            .repository
            .store
            .scan_one_observed(start, end, super::MAX_EXPIRY_SCAN_BYTES)
            .await
            .map_err(BucketError::from)?;
        let page = match page {
            MultiScanOutcome::Page(page) => page,
            MultiScanOutcome::OversizedRow { key } => {
                self.observe_hint(&key)?;
                return Err(MultipartExpiryError::OversizedBucketRow);
            }
        };
        let Some(item) = page.items.into_iter().next() else {
            return Ok(self.finish_round());
        };
        self.observe_hint(&item.key)?;
        Ok(MultipartExpiryStep {
            expired: 0,
            more: true,
        })
    }

    fn finish_round(&mut self) -> MultipartExpiryStep {
        self.bucket_start = None;
        MultipartExpiryStep {
            expired: 0,
            more: false,
        }
    }

    fn observe_hint(&mut self, key: &[u8]) -> Result<(), MultipartExpiryError> {
        let tenant = &self.repository.tenant;
        let prefix = MetadataKey::tenant_prefix(tenant);
        let suffix = key
            .strip_prefix(prefix.as_slice())
            .ok_or(BucketError::Record(MetadataRecordError::Invalid))?;
        let Some(bytes) = suffix.get(..16) else {
            self.bucket_start = Some(after_key(Some(key), Vec::new));
            return Ok(());
        };
        let mut id = [0; 16];
        id.copy_from_slice(bytes);
        let id = BucketId::new(id);
        // This is an inclusive next seek, already past every raw row for this ID.
        // Failed or cancelled upload scans may discard the hint without hot-looping
        // on its retained session, part and generation rows.
        self.bucket_start = Some(MetadataKey::bucket_end(tenant, id));
        self.current = Some(BucketPosition { id, after: None });
        Ok(())
    }

    async fn advance_uploads(&mut self, now_ms: u64) -> Result<MultipartExpiryStep, MultipartExpiryError> {
        let current = self.current.as_ref().expect("an upload step has a bucket");
        let bucket = current.id;
        let tenant = &self.repository.tenant;
        let start = after_key(current.after.as_deref(), || {
            MetadataKey::multipart_session_prefix(tenant, bucket)
        });
        self.scanning_uploads = true;
        let result = self
            .repository
            .store
            .scan_page(
                start,
                MetadataKey::multipart_session_end(tenant, bucket),
                UPLOAD_ITEMS,
                super::MAX_EXPIRY_SCAN_BYTES,
                None,
            )
            .await;
        self.scanning_uploads = false;
        let page = match result {
            Ok(page) => page,
            Err(error) => {
                self.current = None;
                return Err(MultipartRepositoryError::from(error).into());
            }
        };
        let mut expired = 0;
        for index in page.items {
            self.current.as_mut().expect("the bucket remains selected").after = Some(index.key.clone());
            expired += usize::from(self.repository.expire_index(bucket, index, now_ms).await?);
        }
        if page.continuation.is_none() {
            self.current = None;
        }
        Ok(MultipartExpiryStep { expired, more: true })
    }
}

fn after_key(after: Option<&[u8]>, start: impl FnOnce() -> Vec<u8>) -> Vec<u8> {
    after.map_or_else(start, |after| {
        let mut next = after.to_vec();
        next.push(0);
        next
    })
}
