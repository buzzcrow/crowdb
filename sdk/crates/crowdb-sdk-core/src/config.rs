// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

/// Limits belong to a shared client, including all its clones.
#[derive(Clone, Debug)]
pub struct HttpConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub pool_idle_timeout: Duration,
    pub max_idle_connections_per_host: usize,
    pub max_requests: usize,
    pub max_in_flight_bytes: usize,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(60),
            pool_idle_timeout: Duration::from_secs(30),
            max_idle_connections_per_host: 8,
            max_requests: 8,
            max_in_flight_bytes: 128 * 1024 * 1024,
            max_request_bytes: 8 * 1024 * 1024,
            max_response_bytes: 8 * 1024 * 1024,
        }
    }
}

impl HttpConfig {
    pub(crate) fn valid(&self) -> bool {
        !self.connect_timeout.is_zero()
            && !self.request_timeout.is_zero()
            && !self.pool_idle_timeout.is_zero()
            && self.max_requests > 0
            && self.max_request_bytes > 0
            && self.max_response_bytes > 0
            && self
                .max_request_bytes
                .checked_add(self.max_response_bytes)
                .is_some_and(|bytes| bytes <= self.max_in_flight_bytes)
    }
}
