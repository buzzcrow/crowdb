// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Retained topology orders equivalent mirror reads without granting authority.

use super::ChunkReader;
use crowdb_protocol::chunkdb::rpc::{Chunk, Strip};
use std::collections::HashMap;

impl ChunkReader {
    pub(super) fn prefer_local_mirrors(&self, chunk: &mut Chunk) {
        let Some(node) = self.policy.preferred_node_id else {
            return;
        };
        let Some(topology) = self.strip_reader.disk_topology() else {
            return;
        };
        let placements: HashMap<_, _> = topology
            .disks()
            .map(|disk| {
                (
                    (disk.disk_id.high, disk.disk_id.low),
                    (disk.rack_id, disk.node_id),
                )
            })
            .collect();
        let Some(rack) = placements
            .values()
            .find_map(|(rack, owner)| (*owner == node).then_some(*rack))
        else {
            return;
        };
        if placements
            .values()
            .any(|(owner_rack, owner_node)| *owner_node == node && *owner_rack != rack)
        {
            return;
        }
        for strip in &mut chunk.strips {
            let Some(Strip::MirrorStrip(mirror)) = &mut strip.strip else {
                continue;
            };
            if !mirror.segments.iter().all(|segment| {
                segment
                    .disk_id
                    .is_some_and(|id| placements.contains_key(&(id.high, id.low)))
            }) {
                continue;
            }
            mirror.segments.sort_by_key(|segment| {
                let id = segment.disk_id.expect("complete mirror placement hints");
                let (owner_rack, owner_node) = placements[&(id.high, id.low)];
                (owner_node != node, owner_rack != rack)
            });
        }
    }
}
