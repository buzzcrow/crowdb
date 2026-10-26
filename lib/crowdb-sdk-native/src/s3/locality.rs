// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded scheduling observations for one immutable object selection.

mod placement;
pub use placement::{FragmentPlacement, FragmentRole, StripPlacement};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_access_s3::retrieval::object_locations;
use crowdb_access_s3::S3ErrorCode;
use crowdb_chunk_client::{ChunkAllocator, IoError};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, Location, QueryChunkRequest};
use crowdb_protocol::common::ChunkId;

use super::{ByteRange, ObjectMetadata, ResolvedObject, S3Client};
use crate::error::{TopologyFailureKind, TopologySource};
use crate::topology::TopologyView;
use crate::{Error, OperationContext, Result};

#[derive(Clone, Debug)]
pub struct LocalityOptions {
    pub range: Option<ByteRange>,
    /// Index in the selected generation's logically ordered extent list.
    pub start_index: usize,
    pub limit: usize,
}

impl Default for LocalityOptions {
    fn default() -> Self {
        Self {
            range: None,
            start_index: 0,
            limit: 64,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChunkLayoutObservation {
    pub chunk_id: ChunkId,
    pub revision: u64,
    pub state: i32,
    pub observed_at: Instant,
    pub valid_until: Instant,
    chunk: Arc<Chunk>,
}

impl ChunkLayoutObservation {
    #[must_use]
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.valid_until
    }
}

#[derive(Clone, Debug)]
pub struct ObjectExtent {
    pub index: usize,
    pub logical: ByteRange,
    /// Includes complete integrity frames when a logical range is framed.
    pub physical: ByteRange,
    pub layout: Arc<ChunkLayoutObservation>,
    pub strips: Vec<StripPlacement>,
}

pub struct ObjectLocality {
    pub object: ObjectMetadata,
    pub range: ByteRange,
    pub extents: Vec<ObjectExtent>,
    pub next_index: Option<usize>,
    pub topology: TopologyView,
}

impl ObjectLocality {
    /// Old observations remain readable after expiry; this checks only the
    /// source layout validity, without inventing a global topology epoch.
    /// # Errors
    /// Returns a typed stale-layout outcome; execution must obtain a fresh layout.
    pub fn check_layout_freshness(&self) -> Result<()> {
        if self.extents.iter().any(|extent| extent.layout.is_expired()) {
            Err(failure(
                TopologyFailureKind::Stale,
                "observed chunk layout expired",
            ))
        } else {
            Ok(())
        }
    }
}

impl S3Client {
    /// Resolves bounded placement observations without reading object payload.
    /// These views cannot be supplied as execution routing or publish placement.
    /// # Errors
    /// Returns selection, range, admission, deadline or typed layout failures.
    pub async fn object_locality(
        &self,
        context: OperationContext,
        selected: &ResolvedObject,
        options: &LocalityOptions,
    ) -> Result<ObjectLocality> {
        let request = self.runtime.resources.admit(context)?;
        request
            .run(async {
                self.validate_selection(selected)?;
                let locations = normalized_locations(selected)?;
                let range = locality_range(selected, options, locations.len())?;
                let topology = self.runtime.topology()?;
                let placements = placement::disk_index(&topology);
                let (allocator, _) = self.execution.chunks().storage_parts();
                let mut layouts = HashMap::new();
                let mut extents = Vec::new();
                let mut next_index = None;
                for (index, location) in locations.iter().enumerate().skip(options.start_index) {
                    let end = location.logical_offset + location.logical_length;
                    if end <= range.start || location.logical_offset >= range.end {
                        continue;
                    }
                    if extents.len() == options.limit {
                        next_index = Some(index);
                        break;
                    }
                    let logical = ByteRange {
                        start: range.start.max(location.logical_offset),
                        end: range.end.min(end),
                    };
                    let physical = placement::physical_range(location, logical)?;
                    let id = location
                        .chunk_id
                        .ok_or_else(|| failure(TopologyFailureKind::Invalid, "extent has no chunk"))?;
                    let key = (id.high, id.low);
                    let layout = if let Some(layout) = layouts.get(&key) {
                        Arc::clone(layout)
                    } else {
                        let layout = query_layout(&*allocator, id).await?;
                        layouts.insert(key, Arc::clone(&layout));
                        layout
                    };
                    let strips = placement::strips(&layout.chunk, physical, &placements)?;
                    extents.push(ObjectExtent {
                        index,
                        logical,
                        physical,
                        layout,
                        strips,
                    });
                }
                let view = ObjectLocality {
                    object: selected.metadata().clone(),
                    range,
                    extents,
                    next_index,
                    topology,
                };
                view.check_layout_freshness()?;
                Ok(view)
            })
            .await
    }
}

fn locality_range(
    selected: &ResolvedObject,
    options: &LocalityOptions,
    locations: usize,
) -> Result<ByteRange> {
    if !(1..=128).contains(&options.limit) || options.start_index > locations {
        return Err(Error::S3(S3ErrorCode::InvalidRequest));
    }
    let range = options.range.unwrap_or(ByteRange {
        start: 0,
        end: selected.record.logical_length,
    });
    if range.start > range.end
        || range.end > selected.record.logical_length
        || (options.range.is_some() && range.start == range.end)
    {
        return Err(Error::S3(S3ErrorCode::InvalidRange));
    }
    Ok(range)
}

async fn query_layout(
    allocator: &dyn ChunkAllocator,
    chunk_id: ChunkId,
) -> Result<Arc<ChunkLayoutObservation>> {
    let observed_at = Instant::now();
    let response = allocator
        .query_chunk(QueryChunkRequest {
            chunk_id: Some(chunk_id),
        })
        .await
        .map_err(|error| {
            let kind = match error {
                IoError::ChunkNotFound(_) => TopologyFailureKind::Stale,
                IoError::MetadataConflict(_) => TopologyFailureKind::Fenced,
                _ => TopologyFailureKind::Unavailable,
            };
            failure(kind, &error.to_string())
        })?;
    let chunk = response
        .chunk
        .ok_or_else(|| failure(TopologyFailureKind::Stale, "chunk no longer exists"))?;
    if chunk.id != Some(chunk_id) {
        return Err(failure(TopologyFailureKind::Invalid, "chunk identity mismatch"));
    }
    if chunk.state == ChunkState::Deleted as i32 {
        return Err(failure(TopologyFailureKind::Stale, "chunk was deleted"));
    }
    let valid_until = observed_at
        .checked_add(Duration::from_millis(response.layout_validity_ms))
        .ok_or_else(|| failure(TopologyFailureKind::Invalid, "layout validity overflows"))?;
    Ok(Arc::new(ChunkLayoutObservation {
        chunk_id,
        revision: chunk.modify_ts,
        state: chunk.state,
        observed_at,
        valid_until,
        chunk: Arc::new(chunk),
    }))
}

fn normalized_locations(selected: &ResolvedObject) -> Result<Vec<Location>> {
    let mut locations =
        object_locations(&selected.record).map_err(|_| Error::S3(S3ErrorCode::InternalError))?;
    locations.retain(|location| location.length != 0 && location.logical_length != 0);
    locations.sort_unstable_by_key(|location| location.logical_offset);
    let mut cursor = 0u64;
    for location in &locations {
        if location.chunk_id.is_none()
            || location.logical_offset != cursor
            || location.logical_length > location.length
            || location.offset.checked_add(location.length).is_none()
        {
            return Err(failure(
                TopologyFailureKind::Invalid,
                "object extent geometry is inconsistent",
            ));
        }
        cursor = cursor
            .checked_add(location.logical_length)
            .ok_or_else(|| failure(TopologyFailureKind::Invalid, "logical extent overflows"))?;
    }
    if cursor != selected.record.logical_length {
        return Err(failure(
            TopologyFailureKind::Invalid,
            "object extents are incomplete",
        ));
    }
    Ok(locations)
}

fn failure(kind: TopologyFailureKind, detail: &str) -> Error {
    Error::Topology {
        origin: TopologySource::ChunkLayout,
        kind,
        detail: detail.into(),
    }
}
