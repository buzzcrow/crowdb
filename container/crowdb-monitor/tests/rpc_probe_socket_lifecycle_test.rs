// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![cfg(target_os = "linux")]

use crowdb_monitor::{ProbeExecutor, ProbeKind, ProbeProfile, RestartProfile, ServiceProfile};
use crowdb_rpc_ffi::RpcServer;
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

fn descriptors() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

#[tokio::test]
async fn repeated_idle_rpc_probes_reclaim_descriptors_while_the_executor_stays_alive() {
    let probes = ProbeExecutor::new(true).unwrap();
    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).unwrap();
    server.start();
    let service = ServiceProfile {
        id: "socket-lifecycle".into(),
        program: PathBuf::from("/bin/true"),
        args: vec![],
        env: BTreeMap::new(),
        dependencies: vec![],
        fence_listeners: vec![],
        config_template: None,
        probe: ProbeProfile {
            kind: ProbeKind::RpcPing,
            target: format!("127.0.0.1:{}", server.port()),
            bearer_env: None,
            timeout_ms: 1000,
            failure_threshold: 1,
        },
        additional_probes: vec![],
        restart: RestartProfile {
            max_attempts: 1,
            backoff_base_ms: 1,
            backoff_max_ms: 1,
            stable_after_ms: 60_000,
        },
    };
    let before = descriptors();
    for _ in 0..128 {
        probes.probe_service(&service, &BTreeMap::new()).await.unwrap();
    }
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        while descriptors() > before + 8 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "128 completed probes leaked idle connections: before={before}, after={}",
        descriptors()
    );
    probes.probe_service(&service, &BTreeMap::new()).await.unwrap();
    server.stop();
}
