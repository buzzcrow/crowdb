// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::ChunkKvService;

impl ChunkKvService {
    pub(crate) fn sample_tree_resources(
        &self,
        mut observe: impl FnMut(
            &str,
            crowdb_chunk_kv::Result<Option<crowdb_tree_ffi::runtime::ResourceUsageSnapshot>>,
        ),
    ) {
        // Include prepared transfer targets, whose Trees already consume host
        // resources before activation into the serving catalog.
        for partition in self.observed_partition_snapshot().values() {
            let name = format!("chunk.tree.{}", partition.tree_id());
            observe(&name, partition.tree_resource_usage());
        }
    }
}
