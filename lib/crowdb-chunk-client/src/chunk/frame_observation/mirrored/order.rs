// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Keep each observation lane in a consistent failure domain when possible.

use crate::DiskWriter;
use crowdb_protocol::chunkdb::rpc::{Chunk, Strip};
use std::{collections::HashMap, sync::Arc};

pub(super) fn by_rack(mut chunk: Arc<Chunk>, disk_io: &dyn DiskWriter) -> Arc<Chunk> {
    let Some(topology) = disk_io.disk_topology() else {
        return chunk;
    };
    let placements: HashMap<_, _> = topology
        .disks()
        .map(|disk| {
            (
                (disk.disk_id.high, disk.disk_id.low),
                (disk.rack_id, disk.node_id, disk.disk_group_id),
            )
        })
        .collect();
    // Retained routing is an ordering hint, never authority to add or omit a
    // replica. Incomplete hints preserve the supplied layout's original order.
    let complete = chunk.strips.iter().all(|strip| match &strip.strip {
        Some(Strip::MirrorStrip(mirror)) => mirror.segments.iter().all(|segment| {
            segment
                .disk_id
                .is_some_and(|id| placements.contains_key(&(id.high, id.low)))
        }),
        _ => false,
    });
    if !complete {
        return chunk;
    }
    for strip in &mut Arc::make_mut(&mut chunk).strips {
        if let Some(Strip::MirrorStrip(mirror)) = &mut strip.strip {
            mirror.segments.sort_by_key(|segment| {
                segment
                    .disk_id
                    .and_then(|id| placements.get(&(id.high, id.low)))
                    .copied()
            });
        }
    }
    chunk
}
