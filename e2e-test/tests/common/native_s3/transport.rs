// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_rpc_ffi::RdmaConfig;
use crowdb_sdk_native::ClientRuntime;
use serde_json::Value;

pub fn enabled() -> bool {
    std::env::var("CROWDB_NATIVE_S3_RDMA").as_deref() == Ok("1")
}

pub fn configuration() -> RdmaConfig {
    if !enabled() {
        return RdmaConfig {
            enabled: false,
            ..RdmaConfig::default()
        };
    }
    RdmaConfig {
        enabled: true,
        allow_tcp_fallback: false,
        listen_enabled: false,
        device: std::env::var("CROWDB_RDMA_DEVICE").unwrap(),
        source_address: std::env::var("CROWDB_RDMA_SOURCE_ADDR").unwrap(),
        cq_count: 1,
        cq_depth: 512,
        sq_depth: 16,
        rq_depth: 16,
        posted_receive_target: 16,
        registered_memory_budget_bytes: 512 << 20,
        aggregation_enabled: false,
        ..RdmaConfig::default()
    }
    .with_control_data_capacity(1 << 20)
    .unwrap()
}

pub fn data_status(runtime: &ClientRuntime) -> Value {
    serde_json::from_slice(&runtime.topology().unwrap().data_transport_json.unwrap()).unwrap()
}

pub fn assert_selected(runtime: &ClientRuntime) {
    let status = data_status(runtime);
    if !enabled() {
        assert!(status["rdma"].is_null());
        return;
    }
    assert_eq!(status["rdma"]["ready"], true, "{status}");
    assert_eq!(status["allow_tcp_fallback"], false, "{status}");
    for field in [
        "fallback_count",
        "protocol_errors",
        "cq_errors",
        "send_errors",
        "receive_post_errors",
    ] {
        assert_eq!(status["rdma"][field], 0, "{field}: {status}");
    }
}
