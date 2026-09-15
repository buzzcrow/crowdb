// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free bounded-cardinality S3 request counters.

use std::sync::atomic::{AtomicU64, Ordering};

const OPERATION_COUNT: usize = 9;
const OUTCOME_COUNT: usize = 6;

#[derive(Clone, Copy)]
pub enum OutcomeClass {
    Success,
    ClientError,
    Throttled,
    Timeout,
    Unavailable,
    Internal,
}

pub struct S3Metrics {
    requests: [[AtomicU64; OUTCOME_COUNT]; OPERATION_COUNT],
    request_latency_ns: [[AtomicU64; OUTCOME_COUNT]; OPERATION_COUNT],
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    trusted_auth_bypass: AtomicU64,
    in_flight: AtomicU64,
    backpressure_wait_ns: AtomicU64,
    retained_body_bytes: AtomicU64,
}

impl Default for S3Metrics {
    fn default() -> Self {
        Self {
            requests: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            request_latency_ns: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            request_bytes: AtomicU64::new(0),
            response_bytes: AtomicU64::new(0),
            trusted_auth_bypass: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            backpressure_wait_ns: AtomicU64::new(0),
            retained_body_bytes: AtomicU64::new(0),
        }
    }
}

impl S3Metrics {
    pub fn finish_request(
        &self,
        operation: crate::route::S3Operation,
        outcome: OutcomeClass,
        request_bytes: u64,
        response_bytes: u64,
        latency_ns: u64,
    ) {
        self.requests[operation as usize][outcome as usize].fetch_add(1, Ordering::Relaxed);
        self.request_latency_ns[operation as usize][outcome as usize]
            .fetch_add(latency_ns, Ordering::Relaxed);
        self.request_bytes.fetch_add(request_bytes, Ordering::Relaxed);
        self.response_bytes.fetch_add(response_bytes, Ordering::Relaxed);
    }

    pub fn record_trusted_auth_bypass(&self) {
        self.trusted_auth_bypass.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn begin_request(&self) -> InFlightRequest<'_> {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        InFlightRequest(self)
    }

    pub fn record_backpressure_wait(&self, nanoseconds: u64) {
        self.backpressure_wait_ns
            .fetch_add(nanoseconds, Ordering::Relaxed);
    }

    pub fn set_retained_body_bytes(&self, value: u64) {
        self.retained_body_bytes.store(value, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> S3MetricsSnapshot {
        S3MetricsSnapshot {
            requests: std::array::from_fn(|operation| {
                std::array::from_fn(|outcome| self.requests[operation][outcome].load(Ordering::Relaxed))
            }),
            request_latency_ns: std::array::from_fn(|operation| {
                std::array::from_fn(|outcome| {
                    self.request_latency_ns[operation][outcome].load(Ordering::Relaxed)
                })
            }),
            request_bytes: self.request_bytes.load(Ordering::Relaxed),
            response_bytes: self.response_bytes.load(Ordering::Relaxed),
            trusted_auth_bypass: self.trusted_auth_bypass.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            backpressure_wait_ns: self.backpressure_wait_ns.load(Ordering::Relaxed),
            retained_body_bytes: self.retained_body_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3MetricsSnapshot {
    pub requests: [[u64; OUTCOME_COUNT]; OPERATION_COUNT],
    pub request_latency_ns: [[u64; OUTCOME_COUNT]; OPERATION_COUNT],
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub trusted_auth_bypass: u64,
    pub in_flight: u64,
    pub backpressure_wait_ns: u64,
    pub retained_body_bytes: u64,
}

pub struct InFlightRequest<'a>(&'a S3Metrics);

impl Drop for InFlightRequest<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct S3Readiness {
    pub dependencies: [DependencyHealth; 4],
}

impl S3Readiness {
    #[must_use]
    pub const fn is_ready(self) -> bool {
        let [listener, metadata, chunks, authentication] = self.dependencies;
        matches!(listener, DependencyHealth::Ready)
            && matches!(metadata, DependencyHealth::Ready)
            && matches!(chunks, DependencyHealth::Ready)
            && matches!(authentication, DependencyHealth::Ready)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyHealth {
    Ready,
    Unavailable,
}
