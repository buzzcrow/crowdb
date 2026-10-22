// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use crowdb_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs};
use crowdb_rpc_ffi::co_bench::CoBenchHandler;
use crowdb_rpc_ffi::{Buffer, RpcError};
use flatbuffers::FlatBufferBuilder;

pub struct TestCoHandler {
    pub stride: u64,
    pub iterations: u64,
    pub expect_timeout: bool,
    pub responses: AtomicU64,
    pub errors: AtomicU64,
}

impl TestCoHandler {
    pub fn new(stride: u64, iterations: u64, expect_timeout: bool) -> Self {
        Self {
            stride,
            iterations,
            expect_timeout,
            responses: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }
}

impl CoBenchHandler for TestCoHandler {
    fn build_request(&self, id: u64) -> Option<(Buffer, Buffer)> {
        if id / self.stride >= self.iterations {
            return None;
        }
        let mut fbb = FlatBufferBuilder::new();
        let request = ConnectionPingRequest::create(
            &mut fbb,
            &ConnectionPingRequestArgs {
                id,
                rpc_create_nano: 0,
            },
        );
        fbb.finish(request, None);
        let mut data = vec![0xA5; 128];
        data[..8].copy_from_slice(&id.to_le_bytes());
        Some((Buffer::from_bytes(fbb.finished_data()), Buffer::from_vec(data)))
    }

    fn on_response(&self, _id: u64, _status: Result<(), RpcError>, _latency_ns: u64) -> bool {
        self.errors.fetch_add(1, Ordering::Relaxed);
        false
    }

    fn on_response_data(
        &self,
        id: u64,
        status: Result<(), RpcError>,
        _latency_ns: u64,
        _control: &[u8],
        data: &[u8],
    ) -> bool {
        let valid = if self.expect_timeout {
            matches!(status, Err(RpcError::Timeout))
        } else {
            status.is_ok()
                && data.len() == 128
                && data[..8] == id.to_le_bytes()
                && data[8..].iter().all(|&b| b == 0xA5)
        };
        self.responses.fetch_add(1, Ordering::Relaxed);
        if !valid {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        valid && !self.expect_timeout
    }
}
