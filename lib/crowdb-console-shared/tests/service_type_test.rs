// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::config::{LocalLaunchSpec, ServiceType};

#[test]
fn service_kinds_share_the_canonical_wire_and_persisted_names() {
    for (kind, name) in [
        (ServiceType::PaxosKv, "paxos-kv"),
        (ServiceType::ChunkKv, "chunk-kv"),
        (ServiceType::Chunkdb, "chunkdb"),
        (ServiceType::Diskdb, "diskdb"),
        (ServiceType::Diskio, "diskio"),
        (ServiceType::AccessServer, "access-server"),
    ] {
        assert_eq!(serde_json::to_value(kind).unwrap(), name);
        assert_eq!(
            serde_json::from_value::<ServiceType>(serde_json::json!(name)).unwrap(),
            kind
        );
    }
    assert!(serde_json::from_str::<ServiceType>("\"kv\"").is_err());
}

#[tokio::test]
async fn legacy_access_restart_is_rejected_before_spawning_or_stopping() {
    let spec = LocalLaunchSpec {
        program: "/nonexistent/crowdb-access-server".into(),
        readiness_url: Some("http://127.0.0.1:9091/_crowdb/health/ready".into()),
        ..Default::default()
    };
    let error =
        crowdb_console_shared::lifecycle::restart_local_service("access-server-1", std::process::id(), &spec)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("reconciliation"));
}

#[test]
fn access_health_launch_never_falls_back_to_the_s3_listener() {
    let mut spec = LocalLaunchSpec {
        env: [
            ("CROWDB_ACCESS_HEALTH_LISTEN".into(), "127.0.0.1:9093".into()),
            ("CROWDB_S3_PUBLIC_URI".into(), "http://127.0.0.1:9091".into()),
        ]
        .into(),
        readiness_url: Some("http://127.0.0.1:9093/_crowdb/health/ready".into()),
        ..Default::default()
    };
    assert!(spec.access_health_url().is_some());
    spec.env
        .insert("CROWDB_ACCESS_HEALTH_LISTEN".into(), "127.0.0.1:9091".into());
    spec.readiness_url = Some("http://127.0.0.1:9091/_crowdb/health/ready".into());
    assert!(spec.access_health_url().is_none());
}
