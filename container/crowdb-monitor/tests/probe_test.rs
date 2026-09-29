// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crowdb_monitor::{ProbeExecutor, ProbeKind, ProbeProfile, RestartProfile, ServiceProfile};
use crowdb_rpc_ffi::RpcServer;
use tokio::net::TcpListener;

fn service(kind: ProbeKind, target: String) -> ServiceProfile {
    ServiceProfile {
        id: "test".into(),
        program: PathBuf::from("/bin/true"),
        args: Vec::new(),
        env: BTreeMap::new(),
        dependencies: Vec::new(),
        fence_listeners: Vec::new(),
        config_template: None,
        probe: ProbeProfile {
            kind,
            target,
            bearer_env: None,
            timeout_ms: 1000,
            failure_threshold: 1,
        },
        additional_probes: Vec::new(),
        restart: RestartProfile {
            max_attempts: 1,
            backoff_base_ms: 1,
            backoff_max_ms: 1,
            stable_after_ms: 60_000,
        },
    }
}

#[tokio::test]
async fn tcp_probe_requires_a_listener() {
    let probes = ProbeExecutor::new(false).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = listener.local_addr().unwrap().to_string();
    assert!(probes
        .probe_service(&service(ProbeKind::Tcp, target.clone()), &BTreeMap::new())
        .await
        .is_ok());
    drop(listener);
    assert!(probes
        .probe_service(&service(ProbeKind::Tcp, target), &BTreeMap::new())
        .await
        .is_err());
}

#[tokio::test]
async fn all_service_probes_must_pass() {
    let probes = ProbeExecutor::new(false).unwrap();
    let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut service = service(ProbeKind::Tcp, first.local_addr().unwrap().to_string());
    service.additional_probes.push(ProbeProfile {
        kind: ProbeKind::Tcp,
        target: second.local_addr().unwrap().to_string(),
        bearer_env: None,
        timeout_ms: 1000,
        failure_threshold: 1,
    });
    assert!(probes.probe_service(&service, &BTreeMap::new()).await.is_ok());
    drop(second);
    assert!(probes.probe_service(&service, &BTreeMap::new()).await.is_err());
}

#[tokio::test]
async fn http_probe_requires_success_status() {
    let probes = ProbeExecutor::new(false).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!("http://{}/ready", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for response in [
            "HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        ] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4];
            stream.read_exact(&mut request).await.unwrap();
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    assert!(probes
        .probe_service(&service(ProbeKind::Http, target.clone()), &BTreeMap::new())
        .await
        .is_err());
    assert!(probes
        .probe_service(&service(ProbeKind::Http, target), &BTreeMap::new())
        .await
        .is_ok());
    server.await.unwrap();
}

#[tokio::test]
async fn authenticated_probe_uses_runtime_token_without_storing_it_in_profile() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let probes = ProbeExecutor::new(false).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut service = service(
        ProbeKind::Http,
        format!("http://{}/v1/config", listener.local_addr().unwrap()),
    );
    service.probe.bearer_env = Some("CROWDB_ICEBERG_READ_TOKEN".into());
    assert!(probes.probe_service(&service, &BTreeMap::new()).await.is_err());
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = vec![0_u8; 4096];
        let size = stream.read(&mut request).await.unwrap();
        let body = std::str::from_utf8(&request[..size]).unwrap();
        assert!(body
            .to_ascii_lowercase()
            .contains("authorization: bearer private-token\r\n"));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });
    let environment = BTreeMap::from([("CROWDB_ICEBERG_READ_TOKEN".into(), "private-token".into())]);
    probes.probe_service(&service, &environment).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn rpc_ping_requires_an_application_response() {
    let probes = ProbeExecutor::new(true).unwrap();
    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).unwrap();
    server.start();
    let target = format!("127.0.0.1:{}", server.port());
    probes
        .probe_service(&service(ProbeKind::RpcPing, target), &BTreeMap::new())
        .await
        .unwrap();
    server.stop();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut stalled = service(ProbeKind::RpcPing, listener.local_addr().unwrap().to_string());
    stalled.probe.timeout_ms = 100;
    assert!(probes.probe_service(&stalled, &BTreeMap::new()).await.is_err());
}
