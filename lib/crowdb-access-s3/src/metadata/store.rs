// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv_client::{ChunkKvClient, ClientError, MultiScanPage, MultiScanRequest};
use crowdb_protocol::chunk_kv::{
    ChunkKvResponse, OperationResult, RpcCompareCondition, RpcFailure, RpcValue, ScanDirection,
};

use super::{BucketId, MetadataKey, MetadataKeyError, TenantId};

const DELETE_SCAN_MAX_ITEMS: usize = 1;
const DELETE_SCAN_MAX_BYTES: usize = 4096;

/// Routed Chunk-KV boundary for durable S3 metadata.
///
/// Object publication deliberately uses [`Self::put`] rather than CAS. Bucket
/// deletion alone uses [`Self::replace_bucket_mapping`] after its slow scan.
pub struct ChunkKvMetadataStore {
    client: Arc<ChunkKvClient>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PutIfAbsentOutcome {
    Inserted { revision: u64 },
    Existing(RpcValue),
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataStoreError {
    #[error("S3 metadata key is invalid: {0}")]
    Key(#[from] MetadataKeyError),
    #[error("Chunk-KV client failed: {0}")]
    Client(#[from] ClientError),
    #[error("Chunk-KV rejected S3 metadata: {0}")]
    Failure(String),
    #[error("Chunk-KV returned {0} for an S3 metadata operation")]
    Result(&'static str),
    #[error("Chunk-KV mutation did not provide an applied revision")]
    MissingRevision,
}

impl ChunkKvMetadataStore {
    #[must_use]
    pub fn new(client: Arc<ChunkKvClient>) -> Self {
        Self { client }
    }

    /// Reads one validated S3 metadata value through its routed partition.
    ///
    /// # Errors
    ///
    /// Returns a client, server, or response-shape failure.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<RpcValue>, MetadataStoreError> {
        value_result(self.client.get(key, None).await?)
    }

    /// Writes one complete object record directly.
    ///
    /// This is the ordinary small-object PUT metadata path: no preliminary
    /// read, CAS, bucket lease, or deletion permit is introduced here.
    ///
    /// # Errors
    ///
    /// Returns a client, server, or response-shape failure.
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<u64, MetadataStoreError> {
        mutation_revision(self.client.put(key, value).await?)
    }

    /// Removes one object-key value after the caller has selected its bucket.
    /// DELETE is deliberately an unconditional point mutation, matching PUT's
    /// overwrite model and avoiding an extra read/CAS on the normal path.
    ///
    /// # Errors
    ///
    /// Returns a client, server, or response-shape failure.
    pub async fn delete(&self, key: Vec<u8>) -> Result<u64, MetadataStoreError> {
        mutation_revision(self.client.delete(key).await?)
    }

    /// Removes a key while treating an already-absent value as success.
    ///
    /// # Errors
    ///
    /// Returns transport, server, or unexpected response-shape failures.
    pub async fn delete_idempotent(&self, key: Vec<u8>) -> Result<(), MetadataStoreError> {
        let _ = mutation_result(self.client.delete(key).await?)?;
        Ok(())
    }

    /// Creates a bucket mapping while preserving the existing mapping on retry.
    ///
    /// # Errors
    ///
    /// Returns a client, server, or response-shape failure.
    pub async fn put_if_absent(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<PutIfAbsentOutcome, MetadataStoreError> {
        match mutation_result(self.client.put_if_absent(key, value).await?)? {
            (true, Some(revision), _) => Ok(PutIfAbsentOutcome::Inserted { revision }),
            (false, _, Some(observed)) => Ok(PutIfAbsentOutcome::Existing(observed)),
            (true, None, _) => Err(MetadataStoreError::MissingRevision),
            (false, _, None) => Err(MetadataStoreError::Result("an empty conditional mutation")),
        }
    }

    /// Uses the bucket-name mapping's observed bytes as the deletion slow-path fence.
    ///
    /// # Errors
    ///
    /// Returns a client, server, or response-shape failure.
    pub async fn replace_bucket_mapping(
        &self,
        key: Vec<u8>,
        expected: Vec<u8>,
        tombstone: Vec<u8>,
    ) -> Result<bool, MetadataStoreError> {
        self.compare_exchange(key, expected, tombstone).await
    }

    /// Conditionally replaces a metadata value when conflict semantics require it.
    ///
    /// Object PUT calls this only for an explicitly declared predecessor; the
    /// ordinary small-object path uses [`Self::put`] instead.
    ///
    /// # Errors
    ///
    /// Returns a client, server, or response-shape failure.
    pub async fn compare_exchange(
        &self,
        key: Vec<u8>,
        expected: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<bool, MetadataStoreError> {
        let (applied, _, _) = mutation_result(
            self.client
                .compare_exchange(key, RpcCompareCondition::Value(expected), value)
                .await?,
        )?;
        Ok(applied)
    }

    /// Checks whether a bucket currently contains any visible object.
    ///
    /// This intentionally scans only one visibility entry. A complete deletion
    /// workflow scans first, then calls [`Self::replace_bucket_mapping`].
    ///
    /// # Errors
    ///
    /// Returns a key, client, server, or response-shape failure.
    pub async fn has_visible_object(
        &self,
        tenant: &TenantId,
        bucket: BucketId,
    ) -> Result<bool, MetadataStoreError> {
        let page = self
            .client
            .scan(MultiScanRequest {
                start: Some(MetadataKey::object_prefix(tenant, bucket)),
                end: Some(MetadataKey::object_end(tenant, bucket)),
                direction: ScanDirection::Forward,
                max_items: DELETE_SCAN_MAX_ITEMS,
                max_bytes: DELETE_SCAN_MAX_BYTES,
                continuation: None,
            })
            .await?;
        scan_has_item(page)
    }

    /// Scans one bounded S3 metadata interval through routed Chunk-KV partitions.
    ///
    /// # Errors
    ///
    /// Returns a client or terminal server failure.
    pub async fn scan(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        max_items: usize,
        max_bytes: usize,
    ) -> Result<Vec<RpcValue>, MetadataStoreError> {
        let page = self
            .client
            .scan(MultiScanRequest {
                start: Some(start),
                end: Some(end),
                direction: ScanDirection::Forward,
                max_items,
                max_bytes,
                continuation: None,
            })
            .await?;
        if let Some(terminal_failure) = page.terminal_failure {
            return Err(failure(&terminal_failure));
        }
        Ok(page.items)
    }
}

fn value_result(response: ChunkKvResponse) -> Result<Option<RpcValue>, MetadataStoreError> {
    match response.result.map_err(|error| failure(&error))? {
        OperationResult::Value(value) => Ok(value),
        _ => Err(MetadataStoreError::Result("a non-value result")),
    }
}

fn mutation_revision(response: ChunkKvResponse) -> Result<u64, MetadataStoreError> {
    let (applied, revision, _) = mutation_result(response)?;
    if !applied {
        return Err(MetadataStoreError::Result("an unapplied direct mutation"));
    }
    revision.ok_or(MetadataStoreError::MissingRevision)
}

fn mutation_result(
    response: ChunkKvResponse,
) -> Result<(bool, Option<u64>, Option<RpcValue>), MetadataStoreError> {
    match response.result.map_err(|error| failure(&error))? {
        OperationResult::Mutation {
            applied,
            revision,
            observed,
        } => Ok((applied, revision, observed)),
        _ => Err(MetadataStoreError::Result("a non-mutation result")),
    }
}

fn scan_has_item(page: MultiScanPage) -> Result<bool, MetadataStoreError> {
    if let Some(terminal_failure) = page.terminal_failure {
        return Err(failure(&terminal_failure));
    }
    Ok(!page.items.is_empty())
}

fn failure(failure: &RpcFailure) -> MetadataStoreError {
    MetadataStoreError::Failure(format!("{:?}: {}", failure.code, failure.message))
}
