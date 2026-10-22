// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded writes and synchronization proof with safe old-peer fallback.

use super::{
    Bytes, DiskioClient, DiskioError, DiskioResult, Durability, Instant, OperationKind, OperationOptions,
    Ordering, SegmentTarget, WireClient, WireWriteTarget, WritePayload,
};

impl DiskioClient {
    /// Write caller-owned bytes to an exact segment-relative range.
    ///
    /// # Errors
    ///
    /// Returns a typed input, topology, backpressure, disk, durability, or
    /// ambiguous-outcome error.
    pub async fn write(
        &self,
        target: SegmentTarget,
        offset: u64,
        data: Bytes,
        durability: Durability,
        options: OperationOptions,
    ) -> DiskioResult<()> {
        self.write_payload(
            target,
            offset,
            WritePayload::Contiguous(data),
            durability,
            options,
        )
        .await
    }

    pub(super) async fn write_payload(
        &self,
        target: SegmentTarget,
        offset: u64,
        data: WritePayload,
        durability: Durability,
        options: OperationOptions,
    ) -> DiskioResult<()> {
        self.refresh_topology_if_due().await;
        if data.is_empty() {
            return Err(DiskioError::InvalidInput("write data must be nonempty".into()));
        }
        let (zone_offset, _) = target.checked_range(offset, data.len())?;
        let _inflight = self.begin_operation(OperationKind::Write, options.lane)?;
        let mut backoff = self.config.reconnect_initial_backoff;
        let mut last_error = None;
        let mut only_backpressure = true;
        for attempt in 0..self.config.retry_attempts {
            let (route, selected) = self.select(target.disk_id, options.lane)?;
            let write_target = WireWriteTarget {
                disk_id: target.disk_id,
                zone_index: target.zone_index,
                zone_offset,
                ordering_zone_offset: target.segment_base,
                sync: durability == Durability::Fsync,
            };
            let submitted = match &data {
                WritePayload::Contiguous(data) => {
                    self.wire
                        .write_segment_bytes(&self.server, &selected, write_target, data.clone())
                }
                WritePayload::Views(views) => self.wire.write_segment_views(
                    &self.server,
                    &selected,
                    write_target,
                    views.iter().cloned().collect(),
                ),
            };
            let future = match submitted {
                Ok(future) => future,
                Err(error) => {
                    let error = Self::classify_wire(error, OperationKind::Write);
                    only_backpressure &= matches!(error, DiskioError::Backpressure(_));
                    last_error = Some(error);
                    if attempt + 1 < self.config.retry_attempts {
                        if self
                            .prepare_retry(&route, options.lane, &selected, &mut backoff, options.deadline)
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                    break;
                }
            };
            match self.await_synchronized_write(future, options.deadline).await {
                Ok(synchronized) => {
                    if durability == Durability::Fsync && !synchronized {
                        self.fsync_inner(target.disk_id, options, true).await?;
                    }
                    return Ok(());
                }
                Err(DiskioError::PartialWrite) => return Err(DiskioError::PartialWrite),
                Err(
                    error @ (DiskioError::DiskFailure(_)
                    | DiskioError::DiskNotFound
                    | DiskioError::InvalidInput(_)
                    | DiskioError::DurabilityFailure(_)),
                ) => {
                    return Err(error);
                }
                Err(error) => {
                    only_backpressure = false;
                    last_error = Some(error);
                    if attempt + 1 < self.config.retry_attempts
                        && self
                            .prepare_retry(&route, options.lane, &selected, &mut backoff, options.deadline)
                            .await
                            .is_err()
                    {
                        break;
                    }
                }
            }
        }
        self.counters.ambiguous_writes.fetch_add(1, Ordering::Relaxed);
        if only_backpressure {
            let Some(error @ DiskioError::Backpressure(_)) = last_error.as_ref() else {
                return Err(DiskioError::AmbiguousWrite("completion was not observed".into()));
            };
            return Err(error.clone());
        }
        Err(DiskioError::AmbiguousWrite(last_error.map_or_else(
            || "completion was not observed".into(),
            |error| error.to_string(),
        )))
    }

    async fn await_synchronized_write(
        &self,
        future: crowdb_rpc_ffi::CallFuture,
        deadline: Instant,
    ) -> DiskioResult<bool> {
        if Instant::now() >= deadline {
            return Err(DiskioError::DeadlineExceeded);
        }
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            WireClient::await_synchronized_write_response(future),
        )
        .await
        .map_err(|_| DiskioError::DeadlineExceeded)?
        .map_err(|error| Self::classify_wire(error, OperationKind::Write))
    }
}
