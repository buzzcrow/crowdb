// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One complete frame group and compact location per append request.

use super::super::{unix_time_ms, AppendRange, AppendRequest, Extent};
use crate::{ActiveChunkDescriptor, Result, StreamError, StreamName};
use bytes::Bytes;
use crowdb_protocol::frame::encode_append_group_views;

pub(super) struct EncodedBatch {
    pub views: Vec<Bytes>,
    pub physical_end: u64,
    pub logical_end: u64,
    pub extents: Vec<Extent>,
    pub ranges: Vec<AppendRange>,
}

pub(super) fn encode_requests(
    stream_name: StreamName,
    active: &ActiveChunkDescriptor,
    logical_begin: u64,
    requests: &[AppendRequest],
) -> Result<EncodedBatch> {
    let mut physical = active.acknowledged_cursor;
    let mut logical = logical_begin;
    let mut views = Vec::new();
    let mut extents = Vec::with_capacity(requests.len());
    let mut ranges = Vec::with_capacity(requests.len());
    for request in requests {
        let group = encode_append_group_views(active.chunk_id, physical, &request.data, unix_time_ms())
            .map_err(|error| StreamError::InvalidRequest(error.to_string()))?;
        let physical_length = u32::try_from(request.physical_len()?)
            .map_err(|_| StreamError::InvalidRequest("append group exceeds u32".into()))?;
        let logical_end = logical
            .checked_add(request.logical_len() as u64)
            .ok_or_else(|| StreamError::InvalidRequest("stream logical tail overflows".into()))?;
        extents.push(Extent {
            chunk_id: active.chunk_id,
            logical_start: logical,
            logical_end,
            frame_start: physical,
            frame_length: physical_length,
        });
        ranges.push(AppendRange {
            stream_name,
            chunk_id: Some(active.chunk_id),
            begin: logical,
            end: logical_end,
        });
        logical = logical_end;
        physical = physical
            .checked_add(u64::from(physical_length))
            .ok_or_else(|| StreamError::InvalidRequest("stream physical tail overflows".into()))?;
        for view in group {
            views.push(view);
        }
    }
    Ok(EncodedBatch {
        views,
        physical_end: physical,
        logical_end: logical,
        extents,
        ranges,
    })
}
