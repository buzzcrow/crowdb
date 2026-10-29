// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded bucket-name discovery with tenant and prefix-bound continuations.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use super::BucketError;
use crate::metadata::{
    BucketNameRecord, ChunkKvMetadataStore, MetadataKey, MetadataStoreError, ObjectRecord, TenantId,
};

const MAX_CURSOR_BYTES: usize = 64 * 1024;
const CURSOR_CONTEXT: &[u8] = b"crowdb-list-buckets-v1\0";

pub struct BucketListRequest<'a> {
    pub prefix: &'a [u8],
    pub continuation: Option<&'a str>,
    pub max_buckets: usize,
    pub max_scan_items: usize,
    pub max_scan_bytes: usize,
    pub now: u64,
    pub ttl: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub struct BucketListPage {
    pub buckets: Vec<BucketNameRecord>,
    pub continuation: Option<String>,
}

pub struct BucketListSigner(Vec<u8>);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    tenant: Vec<u8>,
    prefix: Vec<u8>,
    after: Vec<u8>,
    expires: u64,
}

impl BucketListSigner {
    /// # Errors
    /// Rejects an empty continuation signing key.
    pub fn new(key: Vec<u8>) -> Result<Self, BucketError> {
        if key.is_empty() {
            return Err(BucketError::InvalidListRequest);
        }
        Ok(Self(key))
    }

    /// Lists one bounded metadata window, excluding tombstones and legacy object rows.
    /// # Errors
    /// Rejects invalid scope, expired cursors, malformed records and storage failures.
    pub async fn list(
        &self,
        store: &ChunkKvMetadataStore,
        tenant: &TenantId,
        request: &BucketListRequest<'_>,
    ) -> Result<BucketListPage, BucketError> {
        if !(1..=10_000).contains(&request.max_buckets)
            || request.max_scan_items == 0
            || request.max_scan_bytes == 0
        {
            return Err(BucketError::InvalidListRequest);
        }
        let (mut start, end) = MetadataKey::bucket_name_bounds(tenant, request.prefix)?;
        if let Some(token) = request.continuation {
            let cursor = self.decode(token, tenant, request)?;
            if cursor.after < start || cursor.after >= end {
                return Err(BucketError::InvalidListRequest);
            }
            start = cursor.after;
            start.push(0);
        }
        let page = store
            .scan_page(start, end, request.max_scan_items, request.max_scan_bytes, None)
            .await?;
        if page.items.is_empty() && page.continuation.is_some() {
            return Err(MetadataStoreError::Result("bucket listing made no progress").into());
        }
        let count = page.items.len();
        let mut buckets = Vec::new();
        let mut continuation = None;
        for (index, value) in page.items.into_iter().enumerate() {
            match BucketNameRecord::decode(&value.value) {
                Ok(record) => {
                    if record.tenant != *tenant
                        || MetadataKey::bucket_name(tenant, &record.name)? != value.key
                        || !record.name.starts_with(request.prefix)
                    {
                        return Err(crate::metadata::MetadataRecordError::Invalid.into());
                    }
                    if !record.tombstone {
                        buckets.push(record);
                    }
                }
                Err(_) if ObjectRecord::decode(&value.value).is_ok() => {}
                Err(error) => return Err(error.into()),
            }
            if buckets.len() == request.max_buckets || index + 1 == count {
                if index + 1 < count || page.continuation.is_some() {
                    continuation = Some(self.encode(&Cursor {
                        tenant: tenant.as_bytes().to_vec(),
                        prefix: request.prefix.to_vec(),
                        after: value.key,
                        expires: request.now.saturating_add(request.ttl),
                    })?);
                }
                break;
            }
        }
        Ok(BucketListPage {
            buckets,
            continuation,
        })
    }

    fn mac(&self, bytes: &[u8]) -> Result<Hmac<Sha256>, BucketError> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).map_err(|_| BucketError::InvalidListRequest)?;
        mac.update(CURSOR_CONTEXT);
        mac.update(bytes);
        Ok(mac)
    }

    fn encode(&self, cursor: &Cursor) -> Result<String, BucketError> {
        let mut bytes = serde_json::to_vec(cursor).map_err(|_| BucketError::InvalidListRequest)?;
        if bytes.len() > MAX_CURSOR_BYTES - 32 {
            return Err(BucketError::InvalidListRequest);
        }
        let signature = self.mac(&bytes)?.finalize().into_bytes();
        bytes.extend_from_slice(&signature);
        Ok(URL_SAFE_NO_PAD.encode(bytes))
    }

    fn decode(
        &self,
        token: &str,
        tenant: &TenantId,
        request: &BucketListRequest<'_>,
    ) -> Result<Cursor, BucketError> {
        if token.len() > 2 * MAX_CURSOR_BYTES {
            return Err(BucketError::InvalidListRequest);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| BucketError::InvalidListRequest)?;
        if bytes.len() <= 32 || bytes.len() > MAX_CURSOR_BYTES {
            return Err(BucketError::InvalidListRequest);
        }
        let (payload, signature) = bytes.split_at(bytes.len() - 32);
        self.mac(payload)?
            .verify_slice(signature)
            .map_err(|_| BucketError::InvalidListRequest)?;
        let cursor: Cursor = serde_json::from_slice(payload).map_err(|_| BucketError::InvalidListRequest)?;
        if cursor.tenant != tenant.as_bytes()
            || cursor.prefix != request.prefix
            || cursor.expires < request.now
        {
            return Err(BucketError::InvalidListRequest);
        }
        Ok(cursor)
    }
}
