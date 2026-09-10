// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::{KeyRange, ScanDirection, ScanRequest};
use thiserror::Error;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ScanValidationError {
    #[error("scan request is invalid")]
    InvalidRequest,
    #[error("scan continuation topology is stale")]
    RefreshRequired,
    #[error("scan interval does not intersect the routed partition")]
    NotMyRange,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClippedScan {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
    pub direction: ScanDirection,
    pub limit: u32,
    pub resume_after: Option<Vec<u8>>,
}

/// Validates topology before clipping a scan to one partition's half-open range.
///
/// # Errors
///
/// Returns `RefreshRequired` for a token from any different direction, map,
/// partition, or epoch. Invalid and non-intersecting intervals remain distinct.
pub fn validate_and_clip_scan(
    request: &ScanRequest,
    partition_range: &KeyRange,
) -> Result<ClippedScan, ScanValidationError> {
    request
        .validate()
        .map_err(|_| ScanValidationError::InvalidRequest)?;
    if !request.continuation_matches_topology() {
        return Err(ScanValidationError::RefreshRequired);
    }
    let start = request.start.as_ref().map_or_else(
        || partition_range.start.clone(),
        |start| start.max(&partition_range.start).clone(),
    );
    let end = minimum_end(request.end.as_ref(), partition_range.end.as_ref());
    if end.as_ref().is_some_and(|end| start >= *end) {
        return Err(ScanValidationError::NotMyRange);
    }
    let resume_after = request
        .continuation
        .as_ref()
        .map(|continuation| continuation.last_key.clone());
    if resume_after
        .as_ref()
        .is_some_and(|key| key < &start || end.as_ref().is_some_and(|end| key >= end))
    {
        return Err(ScanValidationError::InvalidRequest);
    }
    Ok(ClippedScan {
        start,
        end,
        direction: request.direction,
        limit: request.limit,
        resume_after,
    })
}

fn minimum_end(requested: Option<&Vec<u8>>, partition: Option<&Vec<u8>>) -> Option<Vec<u8>> {
    match (requested, partition) {
        (Some(requested), Some(partition)) => Some(requested.min(partition).clone()),
        (Some(requested), None) => Some(requested.clone()),
        (None, Some(partition)) => Some(partition.clone()),
        (None, None) => None,
    }
}
