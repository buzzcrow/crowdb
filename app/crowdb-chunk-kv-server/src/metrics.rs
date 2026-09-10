// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use crowdb_protocol::chunk_kv::{ChunkKvResponse, ChunkKvRpcErrorCode};

#[derive(Debug, Default)]
pub struct ServerMetrics {
    requests: AtomicU64,
    successes: AtomicU64,
    redirects: AtomicU64,
    lease_rejections: AtomicU64,
    deadline_rejections: AtomicU64,
    overload_rejections: AtomicU64,
    internal_errors: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ServerMetricsSnapshot {
    pub requests: u64,
    pub successes: u64,
    pub redirects: u64,
    pub lease_rejections: u64,
    pub deadline_rejections: u64,
    pub overload_rejections: u64,
    pub internal_errors: u64,
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
        }
    }
}

fn increment(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}
