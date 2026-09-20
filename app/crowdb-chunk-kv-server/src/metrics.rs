// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use crowdb_protocol::chunk_kv::{ChunkKvResponse, ChunkKvRpcErrorCode};
use serde::Serialize;

#[derive(Debug, Default)]
pub struct ServerMetrics {
    requests: AtomicU64,
    successes: AtomicU64,
    redirects: AtomicU64,
    lease_rejections: AtomicU64,
    deadline_rejections: AtomicU64,
    overload_rejections: AtomicU64,
    internal_errors: AtomicU64,
    split_stale_route_forwards: AtomicU64,
    retired_admission_backpressure: AtomicU64,
    retired_recoveries: AtomicU64,
    retired_split_finalizations: AtomicU64,
    retired_split_commits: AtomicU64,
    retired_split_catchup_lag_records: AtomicU64,
    retired_split_finalization_duration_us: AtomicU64,
    retired_split_tail_records: AtomicU64,
    retired_split_tail_bytes: AtomicU64,
    retired_split_preparation_duration_us: AtomicU64,
    retired_split_base_checkpoint_duration_us: AtomicU64,
    retired_split_overlay_apply_records: AtomicU64,
    retired_split_overlay_apply_bytes: AtomicU64,
    retired_materialization_duration_us: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ServerMetricsSnapshot {
    pub requests: u64,
    pub successes: u64,
    pub redirects: u64,
    pub lease_rejections: u64,
    pub deadline_rejections: u64,
    pub overload_rejections: u64,
    pub internal_errors: u64,
    pub split_stale_route_forwards: u64,
    pub admission_backpressure: u64,
    pub recoveries: u64,
    pub split_finalizations: u64,
    pub split_commits: u64,
    pub split_catchup_lag_records: u64,
    pub split_finalization_duration_us: u64,
    pub split_tail_records: u64,
    pub split_tail_bytes: u64,
    pub split_preparation_duration_us: u64,
    pub split_base_checkpoint_duration_us: u64,
    pub split_overlay_apply_records: u64,
    pub split_overlay_apply_bytes: u64,
    pub materialization_duration_us: u64,
}

impl ServerMetrics {
    pub(crate) fn request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn response(&self, response: &ChunkKvResponse) {
        match &response.result {
            Ok(_) => increment(&self.successes),
            Err(error) => match error.code {
                ChunkKvRpcErrorCode::NotMyRange | ChunkKvRpcErrorCode::RefreshRequired => {
                    increment(&self.redirects);
                }
                ChunkKvRpcErrorCode::LeaseExpired => increment(&self.lease_rejections),
                ChunkKvRpcErrorCode::RequestExpired => increment(&self.deadline_rejections),
                ChunkKvRpcErrorCode::Overloaded => increment(&self.overload_rejections),
                ChunkKvRpcErrorCode::Internal => increment(&self.internal_errors),
                ChunkKvRpcErrorCode::WriteStalled
                | ChunkKvRpcErrorCode::Recovering
                | ChunkKvRpcErrorCode::TargetNotReady
                | ChunkKvRpcErrorCode::RequestConflict
                | ChunkKvRpcErrorCode::InvalidRequest => {}
            },
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> ServerMetricsSnapshot {
        ServerMetricsSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            successes: self.successes.load(Ordering::Relaxed),
            redirects: self.redirects.load(Ordering::Relaxed),
            lease_rejections: self.lease_rejections.load(Ordering::Relaxed),
            deadline_rejections: self.deadline_rejections.load(Ordering::Relaxed),
            overload_rejections: self.overload_rejections.load(Ordering::Relaxed),
            internal_errors: self.internal_errors.load(Ordering::Relaxed),
            split_stale_route_forwards: self.split_stale_route_forwards.load(Ordering::Relaxed),
            admission_backpressure: self.retired_admission_backpressure.load(Ordering::Relaxed),
            recoveries: self.retired_recoveries.load(Ordering::Relaxed),
            split_finalizations: self.retired_split_finalizations.load(Ordering::Relaxed),
            split_commits: self.retired_split_commits.load(Ordering::Relaxed),
            split_catchup_lag_records: self.retired_split_catchup_lag_records.load(Ordering::Relaxed),
            split_finalization_duration_us: self
                .retired_split_finalization_duration_us
                .load(Ordering::Relaxed),
            split_tail_records: self.retired_split_tail_records.load(Ordering::Relaxed),
            split_tail_bytes: self.retired_split_tail_bytes.load(Ordering::Relaxed),
            split_preparation_duration_us: self.retired_split_preparation_duration_us.load(Ordering::Relaxed),
            split_base_checkpoint_duration_us: self
                .retired_split_base_checkpoint_duration_us
                .load(Ordering::Relaxed),
            split_overlay_apply_records: self.retired_split_overlay_apply_records.load(Ordering::Relaxed),
            split_overlay_apply_bytes: self.retired_split_overlay_apply_bytes.load(Ordering::Relaxed),
            materialization_duration_us: self.retired_materialization_duration_us.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn split_stale_route_forward(&self) {
        increment(&self.split_stale_route_forwards);
    }

    pub(crate) fn retire_partition(&self, metrics: &crowdb_chunk_kv::PartitionMetricsSnapshot) {
        self.retired_admission_backpressure
            .fetch_add(metrics.admission_backpressure, Ordering::Relaxed);
        self.retired_recoveries
            .fetch_add(metrics.recoveries, Ordering::Relaxed);
        self.retired_split_finalizations
            .fetch_add(metrics.split_finalizations, Ordering::Relaxed);
        self.retired_split_commits
            .fetch_add(metrics.split_commits, Ordering::Relaxed);
        self.retired_split_catchup_lag_records
            .fetch_max(metrics.split_catchup_lag_records, Ordering::Relaxed);
        self.retired_split_finalization_duration_us
            .fetch_max(metrics.split_finalization_duration_us, Ordering::Relaxed);
        self.retired_split_tail_records
            .fetch_add(metrics.split_delta_records, Ordering::Relaxed);
        self.retired_split_tail_bytes
            .fetch_add(metrics.split_tail_bytes, Ordering::Relaxed);
        self.retired_split_preparation_duration_us
            .fetch_max(metrics.split_preparation_duration_us, Ordering::Relaxed);
        self.retired_split_base_checkpoint_duration_us
            .fetch_max(metrics.split_base_checkpoint_duration_us, Ordering::Relaxed);
        self.retired_split_overlay_apply_records
            .fetch_add(metrics.split_overlay_apply_records, Ordering::Relaxed);
        self.retired_split_overlay_apply_bytes
            .fetch_add(metrics.split_overlay_apply_bytes, Ordering::Relaxed);
        self.retired_materialization_duration_us
            .fetch_add(metrics.materialization_duration_us, Ordering::Relaxed);
    }
}

fn increment(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}
