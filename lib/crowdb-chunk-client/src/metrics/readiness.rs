// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Background readiness latency, including errors and cancelled observations.

use crowdb_common::metrics::MetricsRegistry;

use super::OperationMetrics;

pub struct ReadinessMetrics {
    pub(crate) total: OperationMetrics,
    pub(crate) hardware: OperationMetrics,
    pub(crate) chunkdb: OperationMetrics,
    pub(crate) diskio: OperationMetrics,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct ReadinessPhaseSnapshot {
    pub observations: u64,
    pub errors: u64,
    pub inflight: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct ReadinessMetricsSnapshot {
    pub total: ReadinessPhaseSnapshot,
    pub hardware: ReadinessPhaseSnapshot,
    pub chunkdb: ReadinessPhaseSnapshot,
    pub diskio: ReadinessPhaseSnapshot,
}

impl Default for ReadinessMetrics {
    fn default() -> Self {
        Self::register(&mut MetricsRegistry::new())
    }
}

impl ReadinessMetrics {
    pub(super) fn register(registry: &mut MetricsRegistry) -> Self {
        Self {
            total: OperationMetrics::register(registry, "chunkio.readiness.total"),
            hardware: OperationMetrics::register(registry, "chunkio.readiness.hardware"),
            chunkdb: OperationMetrics::register(registry, "chunkio.readiness.chunkdb"),
            diskio: OperationMetrics::register(registry, "chunkio.readiness.diskio"),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> ReadinessMetricsSnapshot {
        ReadinessMetricsSnapshot {
            total: snapshot(&self.total),
            hardware: snapshot(&self.hardware),
            chunkdb: snapshot(&self.chunkdb),
            diskio: snapshot(&self.diskio),
        }
    }
}

fn snapshot(metrics: &OperationMetrics) -> ReadinessPhaseSnapshot {
    let latency = metrics.latency.snapshot_total();
    ReadinessPhaseSnapshot {
        observations: latency.total_count,
        errors: metrics.errors.snapshot().total,
        inflight: metrics.inflight.snapshot(),
        p50_ns: latency.p50,
        p99_ns: latency.p99,
        max_ns: latency.max,
    }
}
