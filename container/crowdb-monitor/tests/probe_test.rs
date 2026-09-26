// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crowdb_monitor::{ProbeExecutor, ProbeKind, ProbeProfile, RestartProfile, ServiceProfile};
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
            timeout_ms: 1000,
            failure_threshold: 1,
        },
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
    let probes = ProbeExecutor::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = listener.local_addr().unwrap().to_string();
    assert!(probes
        .probe_service(&service(ProbeKind::Tcp, target.clone()))
        .await
        .is_ok());
    drop(listener);
    assert!(probes
        .probe_service(&service(ProbeKind::Tcp, target))
        .await
        .is_err());
}

#[tokio::test]
async fn http_probe_requires_success_status() {
    let probes = ProbeExecutor::new().unwrap();
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
        .probe_service(&service(ProbeKind::Http, target.clone()))
        .await
        .is_err());
    assert!(probes
        .probe_service(&service(ProbeKind::Http, target))
        .await
        .is_ok());
    server.await.unwrap();
}
