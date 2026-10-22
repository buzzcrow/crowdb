// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Required counters must be present before a benchmark can be valid.

use serde_json::Value;

pub(super) fn validate(status: &Value, rdma: bool) -> Result<(), String> {
    if status["schema_version"] != 1 {
        return Err("unsupported transport status schema".into());
    }
    for field in [
        "io_engines",
        "io_workers",
        "send_queue_rejects",
        "rpc_frames_sent",
        "wire_bytes_sent",
        "writev_calls",
        "writev_progress_calls",
    ] {
        if !status[field].is_u64() {
            return Err(format!("missing transport counter: {field}"));
        }
    }
    if rdma {
        let native = &status["rdma"];
        if status["allow_tcp_fallback"] != false || native["ready"] != true {
            return Err(
                "RDMA benchmark requires a ready transport and disabled TCP fallback on both peers".into(),
            );
        }
        for field in [
            "cq_errors",
            "protocol_errors",
            "send_errors",
            "receive_post_errors",
            "submit_rejects",
            "fallback_count",
            "registration_failures",
            "sends",
            "rpc_frames_sent",
            "wire_bytes_sent",
            "poller_count",
            "cq_count",
            "registered_bytes",
        ] {
            if !native[field].is_u64() {
                return Err(format!("missing RDMA counter: {field}"));
            }
        }
        if native["fallback_count"] != 0 {
            return Err("RDMA transport recorded TCP fallback".into());
        }
        if native["poller_count"] != native["cq_count"] {
            return Err("CQ/poller ownership count mismatch".into());
        }
    }
    Ok(())
}
