// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc;

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
}

fn tempdir(tag: &str) -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("crowdb-s3-cli-{tag}-{}-{nonce}", std::process::id()));
    std::fs::create_dir_all(&path).expect("create tempdir");
    path
}

fn write_cluster_record(root: &Path, endpoint: &str) {
    std::fs::create_dir_all(root).expect("create cluster root");
    let record = serde_json::json!({
        "version": 1,
        "endpoint": endpoint,
        "tenant": "local",
        "storage_profile": "persistent"
    });
    std::fs::write(
        root.join("s3-mini-cluster.json"),
        serde_json::to_vec_pretty(&record).expect("record json"),
    )
    .expect("write record");
}

fn mock_http_once(response_content_type: &str, response_body: &[u8]) -> (String, mpsc::Receiver<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock HTTP server");
    let endpoint = format!("http://{}", listener.local_addr().expect("mock address"));
    let body = response_body.to_vec();
    let content_type = response_content_type.to_string();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept request");
        let mut request = Vec::new();
        let mut buffer = [0; 4096];
        let mut expected = None;
        loop {
            let read = stream.read(&mut buffer).expect("read request");
            assert_ne!(read, 0, "request ended before body completed");
            request.extend_from_slice(&buffer[..read]);
            if expected.is_none() {
                if let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.split_once(':').and_then(|(name, value)| {
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().expect("content length"))
                            })
                        })
                        .unwrap_or(0);
                    expected = Some(header_end + 4 + content_length);
                }
            }
            if expected.is_some_and(|length| request.len() >= length) {
                break;
            }
        }
        sender.send(request).expect("send captured request");
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\nx-test-header: visible\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        )
        .expect("write response headers");
        stream.write_all(&body).expect("write response body");
    });
    (endpoint, receiver)
}

#[test]
fn s3_benchmark_exposes_all_memory_workloads() {
    for workload in ["write", "read", "range-read", "list", "mix"] {
        let output = cli()
            .args(["bench", "s3", workload, "--help"])
            .output()
            .expect("run help");
        assert!(
            output.status.success(),
            "{workload}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("--memory-budget-bytes"));
        assert!(stdout.contains("--operations"));
    }
}

#[test]
fn object_range_rejects_regression_before_cluster_access() {
    let output = cli()
        .args([
            "s3",
            "object",
            "get",
            "--data-dir",
            "/path/that/is/not/opened",
            "bucket",
            "key",
            "--range",
            "9-3",
        ])
        .output()
        .expect("run invalid range");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("range start must not exceed end"));
}

#[test]
fn help_is_console_first_and_uses_http_operation_names() {
    let output = cli().arg("--help").output().expect("run CLI help");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--system-ip"));
    assert!(stdout.contains("--system-port"));
    for removed in ["--config", "--json", "--log-root", "Group-0 leader"] {
        assert!(
            !stdout.contains(removed),
            "unexpected {removed} in help: {stdout}"
        );
    }

    let output = cli()
        .args(["s3", "object", "--help"])
        .output()
        .expect("run object help");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("head"));
    assert!(!stdout.contains("inspect"));

    let output = cli()
        .args(["s3", "bucket", "--help"])
        .output()
        .expect("run bucket help");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for operation in ["put", "get", "delete", "list"] {
        assert!(
            stdout
                .lines()
                .any(|line| line.trim_start().starts_with(operation)),
            "missing {operation}: {stdout}"
        );
    }
    for legacy in ["add", "remove", "inspect"] {
        assert!(
            !stdout.lines().any(|line| line.trim_start().starts_with(legacy)),
            "unexpected {legacy}: {stdout}"
        );
    }

    let output = cli()
        .args(["s3", "object", "put", "--help"])
        .output()
        .expect("run put help");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for option in ["--root", "--file", "--text", "--random-size"] {
        assert!(stdout.contains(option), "missing {option}: {stdout}");
    }
    assert!(!stdout.contains("--data-dir"), "legacy option exposed: {stdout}");
}

#[test]
fn simple_s3_command_uses_console_without_file_log() {
    let root = tempdir("no-file-log");
    let output = cli()
        .current_dir(&root)
        .args(["s3", "bucket", "list", "--root", "missing"])
        .output()
        .expect("run list against missing cluster");
    assert!(!output.status.success());
    assert!(!root.join("cli-log").exists());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("log dir:"));
    assert!(stderr.contains("\x1b[1;31mERROR\x1b[0m"), "stderr={stderr}");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn bucket_get_prints_headers_and_formats_xml() {
    let xml = br#"<?xml version="1.0"?><ListBucketResult><Name>test</Name></ListBucketResult>"#;
    let (endpoint, captured) = mock_http_once("application/xml", xml);
    let root = tempdir("bucket-get");
    let cluster_root = root.join("data");
    write_cluster_record(&cluster_root, &endpoint);

    let output = cli()
        .current_dir(&root)
        .args(["s3", "bucket", "get", "test", "--root"])
        .arg(&cluster_root)
        .output()
        .expect("run bucket get");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let request = captured.recv().expect("captured request");
    let request = String::from_utf8_lossy(&request);
    assert!(request.starts_with("GET /test HTTP/1.1"), "request={request}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\n  <Name>test</Name>\n"), "stdout={stdout}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("HTTP REQUEST"));
    assert!(stderr.contains("HTTP RESPONSE"));
    assert!(stderr.contains("x-test-header: visible"));
    assert!(stderr.contains("body: XML"));
    assert!(!root.join("cli-log").exists());
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn object_put_text_sets_content_type_and_shows_text_body() {
    let (endpoint, captured) = mock_http_once("application/xml", b"");
    let root = tempdir("put-text");
    let cluster_root = root.join("data");
    write_cluster_record(&cluster_root, &endpoint);

    let output = cli()
        .current_dir(&root)
        .args(["s3", "object", "put", "bucket", "key", "--root"])
        .arg(&cluster_root)
        .args(["--text", "object content"])
        .output()
        .expect("run object put --text");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let request = captured.recv().expect("captured request");
    let request = String::from_utf8_lossy(&request);
    assert!(
        request.starts_with("PUT /bucket/key HTTP/1.1"),
        "request={request}"
    );
    assert!(request
        .to_ascii_lowercase()
        .contains("content-type: text/plain; charset=utf-8"));
    assert!(request.ends_with("object content"), "request={request}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("> | object content"), "stderr={stderr}");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn object_put_random_size_sends_exact_binary_length() {
    let (endpoint, captured) = mock_http_once("application/xml", b"");
    let root = tempdir("put-random");
    let cluster_root = root.join("data");
    write_cluster_record(&cluster_root, &endpoint);

    let output = cli()
        .current_dir(&root)
        .args(["s3", "object", "put", "bucket", "random.bin", "--root"])
        .arg(&cluster_root)
        .args(["--random-size", "257"])
        .output()
        .expect("run object put --random-size");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let request = captured.recv().expect("captured request");
    let body_start = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("request headers")
        + 4;
    assert_eq!(request.len() - body_start, 257);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("body: binary (257 bytes, not shown)"));
    let _ = std::fs::remove_dir_all(root);
}
