// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One immutable authority context for a complete grouped RPC.

use std::{collections::HashSet, time::Instant};

use super::super::{not_my_range_failure, rpc_failure, AuthorityError, CatalogSnapshot};
use crate::{ChunkKvService, ServingContext};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogPartitionState, ChunkKvResponse, ChunkKvRpcErrorCode, PointRequest, RpcFailure,
};

pub(super) struct PointGroup {
    contexts: Vec<ServingContext>,
    generation: u64,
    wall_ms: u64,
    monotonic_ms: u64,
    started: Instant,
}

impl ChunkKvService {
    pub(super) fn capture_point_group<'a>(
        &self,
        catalog: &CatalogSnapshot,
        keys: impl Iterator<Item = &'a [u8]>,
        wall_ms: u64,
        monotonic_ms: u64,
    ) -> Result<PointGroup, RpcFailure> {
        let started = Instant::now();
        let mut seen = HashSet::new();
        let mut contexts = Vec::new();
        for key in keys {
            let entry = catalog
                .entry_for_key(key)
                .ok_or_else(|| not_my_range_failure(catalog.generation, None))?;
            if !seen.insert(entry.partition_id)
                || entry.state == ChunkKvRangeCatalogPartitionState::TargetCatchingUp
            {
                continue;
            }
            contexts.push(
                self.authority
                    .capture(
                        catalog.generation,
                        entry.partition_id,
                        entry.owner_epoch,
                        self.request_monotonic_ms(monotonic_ms, started),
                    )
                    .map_err(|error| match error {
                        AuthorityError::LeaseExpired => {
                            rpc_failure(ChunkKvRpcErrorCode::LeaseExpired, &error.to_string())
                        }
                        _ => not_my_range_failure(catalog.generation, Some(entry)),
                    })?,
            );
        }
        Ok(PointGroup {
            contexts,
            generation: catalog.generation,
            wall_ms,
            monotonic_ms,
            started,
        })
    }

    pub(super) async fn execute_group_point(
        &self,
        group: &PointGroup,
        request: PointRequest,
    ) -> ChunkKvResponse {
        if let Err(error) = self.validate_point_group(group) {
            return group_failure(group.generation, error);
        }
        let elapsed = u64::try_from(group.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let response = self
            .handle_point(
                request,
                group.wall_ms.saturating_add(elapsed),
                self.request_monotonic_ms(group.monotonic_ms, group.started),
            )
            .await;
        if response.result.is_ok() {
            if let Err(error) = self.validate_point_group(group) {
                return group_failure(group.generation, error);
            }
        }
        response
    }

    fn validate_point_group(&self, group: &PointGroup) -> Result<(), RpcFailure> {
        self.authority
            .validate_contexts(
                &group.contexts,
                self.request_monotonic_ms(group.monotonic_ms, group.started),
            )
            .map_err(|error| rpc_failure(ChunkKvRpcErrorCode::LeaseExpired, &error.to_string()))
    }
}

fn group_failure(generation: u64, error: RpcFailure) -> ChunkKvResponse {
    ChunkKvResponse {
        map_revision: generation,
        journal_position: None,
        result: Err(error),
    }
}
