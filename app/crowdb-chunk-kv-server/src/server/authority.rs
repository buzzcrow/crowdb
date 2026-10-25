// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Captured serving authority around asynchronous point execution.

use super::{
    authority_failure, ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, ChunkKvResponse,
    ChunkKvService, Instant, Partition, PointRequest, PreparedSplitWriterArtifact,
};

impl ChunkKvService {
    pub(super) async fn execute_authorized_point(
        &self,
        partition: &Partition,
        entry: &ChunkKvRangeCatalogEntry,
        generation: u64,
        request: PointRequest,
        transfer: Option<PreparedSplitWriterArtifact>,
        received_monotonic_ms: u64,
    ) -> ChunkKvResponse {
        let started = Instant::now();
        let Ok(process) = self.authority.process().capture(Instant::now()) else {
            return authority_failure(generation, &super::AuthorityError::LeaseExpired, Some(entry));
        };
        // A catching-up target uses the already validated transfer artifact.
        // Serving assignments use their independently renewable grant context.
        let context = if entry.state == ChunkKvRangeCatalogPartitionState::TargetCatchingUp {
            None
        } else {
            match self.authority.capture(
                generation,
                entry.partition_id,
                entry.owner_epoch,
                self.request_monotonic_ms(received_monotonic_ms, started),
            ) {
                Ok(context) => Some(context),
                Err(error) => return authority_failure(generation, &error, Some(entry)),
            }
        };
        let response = self
            .execute_point_operation(partition, entry, generation, request, transfer)
            .await;
        if response.result.is_ok() {
            if self
                .authority
                .process()
                .validate_context(&process, Instant::now())
                .is_err()
            {
                return authority_failure(generation, &super::AuthorityError::LeaseExpired, Some(entry));
            }
            if let Some(context) = context {
                if let Err(error) = self.authority.validate_context(
                    &context,
                    self.request_monotonic_ms(received_monotonic_ms, started),
                ) {
                    return authority_failure(generation, &error, Some(entry));
                }
            }
        }
        response
    }

    pub(super) fn request_monotonic_ms(&self, received: u64, started: Instant) -> u64 {
        let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.monotonic_ms().max(received.saturating_add(elapsed))
    }
}
