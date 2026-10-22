// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use crowdb_protocol::fb::{ConnectionPingResponse, ConnectionPingResponseArgs};
use crowdb_rpc_ffi::{RdmaConfig, RpcServer};
use flatbuffers::FlatBufferBuilder;
use serde_json::Value;

static NEXT: AtomicU64 = AtomicU64::new(0);

struct TestServer(RpcServer);

impl TestServer {
    fn new() -> Self {
        let server = RpcServer::new(None);
        server.configure_rdma(&RdmaConfig::default()).unwrap();
        server.listen("127.0.0.1", 0).unwrap();
        server.register_status_handler(101);
        Self(server)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.0.stop();
    }
}

fn run(server: &TestServer, verb: &str, args: &[&str]) -> (Output, Value) {
    let root = std::env::temp_dir().join(format!(
        "crowdb-rpc-bench-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    let result: PathBuf = root.join("result.json");
    let output = Command::new(env!("CARGO_BIN_EXE_crowdb-rpc-test"))
        .env("CROWDB_LOG_ROOT", root.join("logs"))
        .env("CROWDB_RUNTIME_ROOT", &root)
        .args([
            "--log-dir",
            root.join("logs").to_str().unwrap(),
            "--server-port",
            &server.0.port().to_string(),
            "--warmup-secs",
            "1",
            "--duration-secs",
            "1",
            "--metrics-interval",
            "0",
            "--output",
            result.to_str().unwrap(),
        ])
        .args(if verb == "rdma" { vec!["--rdma"] } else { Vec::new() })
        .args(args)
        .output()
        .unwrap();
    let report = std::fs::read(&result).unwrap_or_else(|error| panic!("{error}: {output:?}"));
    let report = serde_json::from_slice(&report).unwrap();
    std::fs::remove_dir_all(root).unwrap();
    (output, report)
}

#[test]
fn tcp_coroutine_and_tokio_validate_timed_completions() {
    let server = TestServer::new();
    server.0.register_echo_handler(100);
    server.0.start();
    for mode in ["coroutine", "tokio"] {
        let (output, report) = run(
            &server,
            "rpc",
            &["--loader-count", "8", "--connections", "2", "--mode", mode],
        );
        assert!(output.status.success(), "{output:?}");
        assert_eq!(report["valid"], true, "{report}");
        assert_eq!(report["actual_path"], "tcp");
        assert_eq!(report["qpair_count"], 0);
        assert_eq!(report["errors_total"], 0);
        assert_eq!(report["measure_s"], 1);
        let completed = report["completed_in_window"].as_u64().unwrap();
        assert!(completed > 0);
        assert_eq!(report["latency_count"], completed);
        let expected = f64::from(u32::try_from(completed).unwrap());
        assert!((report["rpc_tps"].as_f64().unwrap() - expected).abs() < f64::EPSILON);
        assert_eq!(report["submitted"], report["native_total_ops"]);
        assert_eq!(
            report["warmup_completed"].as_u64().unwrap()
                + report["window_callbacks"].as_u64().unwrap()
                + report["drain_completed"].as_u64().unwrap(),
            report["native_total_ops"].as_u64().unwrap()
        );
        assert!(report["warmup_completed"].as_u64().unwrap() > 0);
        assert!(report["server_after"]["cpu_user_ns"].is_u64());
        assert!(report["p90_ns"].as_u64().unwrap() <= report["max_ns"].as_u64().unwrap());
    }
}

#[test]
fn payload_corruption_is_reported_and_never_counted_as_success() {
    let server = TestServer::new();
    server.0.register_reply_for_tests(100, |request| {
        let mut builder = FlatBufferBuilder::new();
        let response = ConnectionPingResponse::create(
            &mut builder,
            &ConnectionPingResponseArgs {
                id: request.request_id,
                ..ConnectionPingResponseArgs::default()
            },
        );
        builder.finish(response, None);
        (builder.finished_data().to_vec(), vec![0xFF; 128])
    });
    server.0.start();
    let (output, report) = run(&server, "rpc", &[]);
    assert!(!output.status.success(), "{output:?}");
    assert_eq!(report["valid"], false);
    assert_eq!(report["completed_in_window"], 0);
    assert!(report["payload_mismatches"].as_u64().unwrap() > 0, "{report}");
    assert!(report["errors_total"].as_u64().unwrap() > 0);
}

#[test]
fn response_timeouts_survive_into_invalid_result() {
    let server = TestServer::new();
    server.0.register_handler(100, |_request| {});
    server.0.start();
    let (output, report) = run(&server, "rpc", &["--request-timeout-secs", "1"]);
    assert!(!output.status.success(), "{output:?}");
    assert_eq!(report["valid"], false);
    assert!(report["timeouts"].as_u64().unwrap() > 0, "{report}");
    assert!(report["native_total_errors"].as_u64().unwrap() > 0);
}

#[test]
fn strict_rdma_and_invalid_input_produce_setup_failure_reports() {
    let server = TestServer::new();
    server.0.register_echo_handler(100);
    server.0.start();
    for (verb, args) in [
        ("rdma", vec!["--device", "crowdb-missing-device"]),
        ("rpc", vec!["--loader-count", "0"]),
    ] {
        let (output, report) = run(&server, verb, &args);
        assert!(!output.status.success(), "{output:?}");
        assert_eq!(report["valid"], false);
        assert_eq!(report["setup_errors"], 1);
        assert_eq!(report["allow_tcp_fallback"], false);
        assert!(report["actual_path"].is_null());
        assert!(report["submit_rejects"].is_null());
    }
}
