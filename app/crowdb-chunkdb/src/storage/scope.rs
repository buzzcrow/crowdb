// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lifecycle read/write authority and filtered, progressing maintenance scans.

use super::{Arc, Bytes, ChunkDomain, ChunkId, ChunkStore, RangeGuard, Result, Route, StoreError};

impl ChunkStore {
    pub(super) fn capture_authority(
        &self,
        id: &ChunkId,
    ) -> Result<Option<crate::range_guard::ExecutionAuthority>> {
        self.check_authority(id)?;
        Ok(self.scope.as_ref().map(|(guard, _)| guard.capture()))
    }

    pub(super) fn check_submission(
        &self,
        id: &ChunkId,
        authority: Option<&crate::range_guard::ExecutionAuthority>,
    ) -> Result<()> {
        if let Some((guard, _)) = &self.scope {
            let authority = authority.ok_or(StoreError::Authority)?;
            guard
                .check_submission(id, authority)
                .map_err(|error| StoreError::OwnershipChanged(error.bucket))?;
        }
        Ok(())
    }
    #[must_use]
    pub fn with_scope(mut self, guard: Arc<RangeGuard>, domain: ChunkDomain) -> Self {
        self.scope = Some((guard, domain));
        self
    }

    pub(crate) fn check_authority(&self, id: &ChunkId) -> Result<()> {
        if let Some((guard, domain)) = &self.scope {
            if ChunkDomain::for_chunk(id) != Some(*domain) || guard.check(id).is_err() {
                return Err(StoreError::Authority);
            }
        }
        Ok(())
    }

    pub(super) fn route(&self, id: &ChunkId) -> Result<Route> {
        self.check_authority(id)?;
        crate::routing::route(&self.bindings, id).map_err(StoreError::from)
    }

    /// Keys begin with the owning chunk ID immediately after the prefix.
    /// Continue past foreign slots until the requested owned result is filled.
    pub(super) async fn scan_owned_records(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        max_keys: u32,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        if max_keys == 0 || self.scope.as_ref().is_some_and(|(guard, _)| guard.is_empty()) {
            return Ok(Vec::new());
        }
        let table = self.bindings.snapshot();
        if table.is_empty() {
            return Err(crate::routing::RouteError::NoBinding.into());
        }
        let limit = usize::try_from(max_keys).unwrap_or(usize::MAX);
        let mut selected = Vec::new();
        for route in table.bindings() {
            let mut cursor = Bytes::copy_from_slice(start_after);
            let mut cutoff = 0;
            let mut matched = 0;
            loop {
                let page = self
                    .kv
                    .scan_bounded_at(
                        route.kv_store_id,
                        route.kv_group_id,
                        prefix,
                        &cursor,
                        &[],
                        256,
                        false,
                        None,
                        cutoff,
                    )
                    .await
                    .map_err(|error| StoreError::Kv(error.to_string()))?;
                if page.timed_out || (page.truncated && page.items.is_empty()) {
                    return Err(StoreError::Kv("chunk scan made incomplete progress".into()));
                }
                cutoff = page.scan_cutoff;
                for (key, value) in page.items {
                    if key <= cursor {
                        return Err(StoreError::Kv("chunk scan continuation did not advance".into()));
                    }
                    cursor = key.clone();
                    let id = key_chunk_id(&key, prefix.len())?;
                    if self.check_authority(&id).is_ok() {
                        selected.push((key, value));
                        matched += 1;
                        if matched == limit {
                            break;
                        }
                    }
                }
                if !page.truncated || matched == limit {
                    break;
                }
            }
            selected.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            selected.dedup_by(|left, right| left.0 == right.0);
            selected.truncate(limit);
        }
        Ok(selected)
    }
}

fn key_chunk_id(key: &[u8], offset: usize) -> Result<ChunkId> {
    let bytes: [u8; 16] = key
        .get(offset..offset + 16)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| StoreError::Serde("missing chunk ID in metadata key".into()))?;
    Ok(crowdb_protocol::chunk_id::ChunkIdParts::from_bytes(&bytes).to_proto())
}
