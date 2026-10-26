// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{GcScan, GcSystemScan};
use crate::{catalog::StoreError, error::ValidationError};
use crowdb_chunk_kv_client::{MultiScanContinuation, MultiScanPage, MultiScanRequest};
use crowdb_protocol::chunk_kv::ScanDirection;

/// Discardable discovery scope and raw position, with no deletion authority.
#[derive(Clone, Debug)]
pub struct GcLocalScan {
    base: MultiScanRequest,
    after: Vec<u8>,
}

impl GcLocalScan {
    /// Builds a local scan from a validated catalog, scope, prefix and budget.
    /// # Errors
    /// Rejects a persisted cursor in the base scan and out-of-range raw positions.
    pub fn catalog(scan: &GcScan, after: Vec<u8>) -> Result<Self, ValidationError> {
        if !scan.after.is_empty() {
            return Err(ValidationError::Key);
        }
        Self::new(scan.request()?, after)
    }

    /// Builds a local scan inside the validated system-ledger interval.
    /// # Errors
    /// Rejects a persisted cursor in the base scan and out-of-range raw positions.
    pub fn system(scan: &GcSystemScan, after: Vec<u8>) -> Result<Self, ValidationError> {
        if !scan.after.is_empty() {
            return Err(ValidationError::Key);
        }
        Self::new(scan.request()?, after)
    }

    fn new(base: MultiScanRequest, after: Vec<u8>) -> Result<Self, ValidationError> {
        let start = base.start.as_ref().ok_or(ValidationError::Key)?;
        let end = base.end.as_ref().ok_or(ValidationError::Key)?;
        if !after.is_empty() && (after < *start || after >= *end) {
            return Err(ValidationError::Key);
        }
        Ok(Self { base, after })
    }

    #[must_use]
    pub fn byte_budget(&self) -> usize {
        self.base.max_bytes
    }

    /// Resumes strictly after the raw key using the current routed catalog generation.
    /// The continuation preserves maximum-length keys without appending a byte.
    /// # Errors
    /// Rejects an unavailable catalog generation.
    pub fn request(&self, catalog_generation: u64) -> Result<MultiScanRequest, ValidationError> {
        if catalog_generation == 0 {
            return Err(ValidationError::Record);
        }
        let mut request = self.base.clone();
        if !self.after.is_empty() {
            request.continuation = Some(MultiScanContinuation {
                direction: ScanDirection::Forward,
                original_start: request.start.clone(),
                original_end: request.end.clone(),
                last_key: self.after.clone(),
                catalog_generation,
            });
        }
        Ok(request)
    }

    /// Checks a raw page before its keys may advance a local discovery position.
    /// # Errors
    /// Rejects failed, foreign, repeated, reversed or oversized rows and continuations.
    pub fn validate_page(&self, page: &MultiScanPage) -> Result<(), StoreError> {
        let start = self.base.start.as_ref().ok_or(ValidationError::Key)?;
        let end = self.base.end.as_ref().ok_or(ValidationError::Key)?;
        let valid_token = page.continuation.as_ref().map_or(true, |token| {
            token.direction == ScanDirection::Forward
                && token.original_start == self.base.start
                && token.original_end == self.base.end
                && token.catalog_generation > 0
                && page.items.last().is_some_and(|item| item.key == token.last_key)
        });
        if page.terminal_failure.is_some()
            || page.items.len() > self.base.max_items
            || page.items.iter().fold(0_usize, |bytes, item| {
                bytes
                    .saturating_add(item.key.len())
                    .saturating_add(item.value.len())
            }) > self.base.max_bytes
            || page.items.windows(2).any(|items| items[0].key >= items[1].key)
            || page.items.iter().any(|item| {
                item.key < *start
                    || item.key >= *end
                    || item.revision == 0
                    || (!self.after.is_empty() && item.key <= self.after)
            })
            || !valid_token
        {
            return Err(StoreError::Response);
        }
        Ok(())
    }
}
