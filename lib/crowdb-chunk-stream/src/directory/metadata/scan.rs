// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded key enumeration for retention and diagnostics.

use crowdb_protocol::chunk_stream::{
    StreamDirectoryPageId, StreamName, StreamPublicationId, StreamRootIdentity,
};
use crowdb_protocol::key::{
    BinaryKey, StreamDirectoryPageKey, StreamDirectoryRootKey, StreamPublicationGuardKey,
};

use super::{kv_error, DirectoryMetadataStore};
use crate::{Result, StreamError};

pub const MAX_DIRECTORY_SCAN_KEYS: usize = 128;

/// One bounded lexicographic page. Resume after `next` in the same namespace.
/// Enumeration alone is not a reachability proof: cleanup must capture retention
/// control before scanning roots/protections and fence deletion on that revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryKeyScan<T> {
    pub entries: Vec<T>,
    pub next: Option<T>,
}

impl DirectoryMetadataStore {
    /// Enumerates root identities without reading their bodies.
    /// # Errors
    /// Rejects invalid bounds, malformed keys and unavailable scans.
    pub async fn scan_directory_roots(
        &self,
        name: StreamName,
        after: Option<StreamRootIdentity>,
        limit: usize,
    ) -> Result<DirectoryKeyScan<StreamRootIdentity>> {
        let prefix = StreamDirectoryRootKey::stream_prefix(name);
        let after = after.map(|identity| {
            StreamDirectoryRootKey {
                stream_name: name,
                identity,
            }
            .to_bytes()
        });
        self.scan_directory_ids::<StreamDirectoryRootKey, _>(&prefix, after.as_deref(), limit, |key| {
            if key.stream_name != name || key.identity.writer_epoch == 0 || key.identity.generation == 0 {
                return Err(invalid_key());
            }
            Ok(key.identity)
        })
        .await
    }

    /// Enumerates independently addressed pages without materializing page data.
    /// # Errors
    /// Rejects invalid bounds, malformed keys and unavailable scans.
    pub async fn scan_directory_pages(
        &self,
        name: StreamName,
        after: Option<StreamDirectoryPageId>,
        limit: usize,
    ) -> Result<DirectoryKeyScan<StreamDirectoryPageId>> {
        let prefix = StreamDirectoryPageKey::stream_prefix(name);
        let after = after.map(|page_id| {
            StreamDirectoryPageKey {
                stream_name: name,
                page_id,
            }
            .to_bytes()
        });
        self.scan_directory_ids::<StreamDirectoryPageKey, _>(&prefix, after.as_deref(), limit, |key| {
            if key.stream_name != name || key.page_id.0 == [0; 32] {
                return Err(invalid_key());
            }
            Ok(key.page_id)
        })
        .await
    }

    /// Enumerates in-flight publication identities in a bounded pass.
    /// # Errors
    /// Rejects invalid bounds, malformed keys and unavailable scans.
    pub async fn scan_directory_publications(
        &self,
        name: StreamName,
        after: Option<StreamPublicationId>,
        limit: usize,
    ) -> Result<DirectoryKeyScan<StreamPublicationId>> {
        let prefix = StreamPublicationGuardKey::stream_prefix(name);
        let after = after.map(|publication_id| {
            StreamPublicationGuardKey {
                stream_name: name,
                publication_id,
            }
            .to_bytes()
        });
        self.scan_directory_ids::<StreamPublicationGuardKey, _>(&prefix, after.as_deref(), limit, |key| {
            if key.stream_name != name || key.publication_id.0 == [0; 32] {
                return Err(invalid_key());
            }
            Ok(key.publication_id)
        })
        .await
    }

    pub(super) async fn scan_directory_ids<K: BinaryKey, T: Copy>(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
        decode: impl Fn(K) -> Result<T>,
    ) -> Result<DirectoryKeyScan<T>> {
        if limit == 0 || limit > MAX_DIRECTORY_SCAN_KEYS {
            return Err(StreamError::InvalidRequest(
                "directory key scan limit is invalid".into(),
            ));
        }
        let after = after.unwrap_or_default();
        let page = self
            .records
            .scan_keys(prefix, after, u32::try_from(limit).expect("bounded scan limit"))
            .await
            .map_err(kv_error)?;
        if page.items.len() > limit || page.items.len() != page.commit_slots.len() {
            return Err(invalid_key());
        }
        let more = page.truncated || page.timed_out;
        if page.items.is_empty() && more {
            return Err(StreamError::Backpressure);
        }
        let mut previous = after;
        let mut entries = Vec::with_capacity(page.items.len());
        for (key, value) in &page.items {
            if !key.starts_with(prefix) || key.as_ref() <= previous || !value.is_empty() {
                return Err(invalid_key());
            }
            let decoded = K::from_bytes(key).map_err(|_| invalid_key())?;
            entries.push(decode(decoded)?);
            previous = key;
        }
        let next = if more { entries.last().copied() } else { None };
        Ok(DirectoryKeyScan { entries, next })
    }
}

fn invalid_key() -> StreamError {
    StreamError::Corruption("directory key scan returned malformed or unordered records".into())
}
