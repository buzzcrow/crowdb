// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;

use crowdb_diskio_client::{DiskId, DiskPlacement};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, Location, Strip};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::frame::ChunkLocation;

use super::failure;
use crate::error::TopologyFailureKind;
use crate::s3::ByteRange;
use crate::topology::TopologyView;
use crate::Result;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentRole {
    MirrorReplica(usize),
    DataShard(usize),
    ParityShard(usize),
}

#[derive(Clone, Debug)]
pub struct FragmentPlacement {
    pub role: FragmentRole,
    pub segment: Segment,
    pub unavailable: bool,
    /// None means no disk route was observed, without guessing locality.
    pub disk: Option<DiskPlacement>,
}

#[derive(Clone, Debug)]
pub struct StripPlacement {
    pub sequence: u32,
    pub physical: ByteRange,
    pub unit_bytes: u64,
    pub fragments: Vec<FragmentPlacement>,
}

pub(super) fn disk_index(topology: &TopologyView) -> HashMap<DiskId, DiskPlacement> {
    topology
        .disks
        .as_ref()
        .map(|view| view.disks().map(|disk| (disk.disk_id, disk)).collect())
        .unwrap_or_default()
}

pub(super) fn physical_range(location: &Location, logical: ByteRange) -> Result<ByteRange> {
    let start = logical.start - location.logical_offset;
    let end = logical.end - location.logical_offset;
    if location.length == location.logical_length {
        return Ok(ByteRange {
            start: location.offset + start,
            end: location.offset + end,
        });
    }
    let framed = ChunkLocation {
        chunk_id: location
            .chunk_id
            .ok_or_else(|| failure(TopologyFailureKind::Invalid, "extent has no chunk"))?,
        frame_offset: location.offset,
        logical_length: location.logical_length,
    };
    let range = framed
        .physical_range_for_subrange(start..end)
        .map_err(|_| failure(TopologyFailureKind::Invalid, "framed extent range is invalid"))?;
    if range.end > location.offset + location.length {
        return Err(failure(
            TopologyFailureKind::Invalid,
            "framing exceeds extent length",
        ));
    }
    Ok(ByteRange {
        start: range.start,
        end: range.end,
    })
}

pub(super) fn strips(
    chunk: &Chunk,
    physical: ByteRange,
    disks: &HashMap<DiskId, DiskPlacement>,
) -> Result<Vec<StripPlacement>> {
    let mut result = Vec::new();
    let mut cursor = physical.start;
    for strip in &chunk.strips {
        let start = u64::from(strip.chunk_offset) * 1024;
        let end = start + u64::from(strip.capacity) * 1024;
        if end <= cursor || start >= physical.end {
            continue;
        }
        if start > cursor || strip.unit_kb == 0 {
            return Err(failure(
                TopologyFailureKind::Invalid,
                "strip geometry has a gap or zero unit",
            ));
        }
        let selected = ByteRange {
            start: cursor,
            end: end.min(physical.end),
        };
        let fragments = fragments(strip, disks)?;
        result.push(StripPlacement {
            sequence: strip.strip_sequence,
            physical: selected,
            unit_bytes: u64::from(strip.unit_kb) * 1024,
            fragments,
        });
        cursor = selected.end;
        if cursor == physical.end {
            break;
        }
    }
    if cursor != physical.end {
        return Err(failure(
            TopologyFailureKind::Invalid,
            "strip geometry is incomplete",
        ));
    }
    Ok(result)
}

fn fragments(strip: &ChunkStrip, disks: &HashMap<DiskId, DiskPlacement>) -> Result<Vec<FragmentPlacement>> {
    let (segments, data_shards) = match &strip.strip {
        Some(Strip::MirrorStrip(mirror)) => (&mirror.segments, None),
        Some(Strip::EcStrip(ec)) => {
            if ec.data_num == 0 || ec.segments.len() != ec.data_num as usize + ec.code_num as usize {
                return Err(failure(
                    TopologyFailureKind::Invalid,
                    "EC geometry is inconsistent",
                ));
            }
            (&ec.segments, Some(ec.data_num as usize))
        }
        None => return Err(failure(TopologyFailureKind::Invalid, "strip has no fragments")),
    };
    if segments.is_empty() {
        return Err(failure(TopologyFailureKind::Invalid, "strip has no fragments"));
    }
    Ok(segments
        .iter()
        .enumerate()
        .map(|(index, segment)| FragmentPlacement {
            role: data_shards.map_or(FragmentRole::MirrorReplica(index), |data| {
                if index < data {
                    FragmentRole::DataShard(index)
                } else {
                    FragmentRole::ParityShard(index - data)
                }
            }),
            segment: *segment,
            unavailable: strip.unavailable_segments.contains(segment),
            disk: segment
                .disk_id
                .and_then(|disk| disks.get(&DiskId::new(disk.high, disk.low)).cloned()),
        })
        .collect())
}
