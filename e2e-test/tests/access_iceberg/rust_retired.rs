// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    frontend::TestFrontend,
    native::{activate, TestNativeStorage},
    now_ms,
};
use crowdb_access_iceberg::catalog::CatalogError;
use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds, ManagementPrivilege},
    key::OperationId,
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires pinned Apache Iceberg Rust client and native retirement grace"]
async fn official_rust_client_rejects_retired_native_catalog_after_full_grace() {
    let stack = TestNativeStorage::new();
    let repository = CatalogRepository::new(
        stack.store().await,
        ClearBounds {
            request_ms: 300_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
    )
    .unwrap();
    repository
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: now_ms(),
                },
                principal: "manager".into(),
                action: ManagementAction::Initialize,
                expected_epoch: 0,
                display_name: "rust-retired-native".into(),
                confirmation: None,
                capabilities: None,
            },
            ManagementPrivilege::Manage,
            now_ms(),
        )
        .await
        .unwrap();
    activate(&repository).await;
    let first = TestFrontend::start(12).await;
    let second = TestFrontend::start(13).await;
    run_client_across_clear(&first.origin.clone(), &second.origin.clone(), &repository, 1_500).await;
}

async fn run_client_across_clear(
    origin: &str,
    second_origin: &str,
    repository: &CatalogRepository,
    timeout_seconds: u64,
) {
    let control = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let control_address = control.local_addr().unwrap().to_string();
    let origin = origin.to_owned();
    let second_origin = second_origin.to_owned();
    let client = tokio::task::spawn_blocking(move || {
        std::process::Command::new("timeout")
            .arg(timeout_seconds.to_string())
            .arg("pixi")
            .args(["run", "--"])
            .arg(std::env::var_os("CROWDB_ICEBERG_RUST_CLIENT_BIN").expect("build Rust SDK fixture first"))
            .env("CROWDB_ICEBERG_RUST_ORIGIN", origin)
            .env("CROWDB_ICEBERG_RUST_SECOND_ORIGIN", second_origin)
            .env("CROWDB_ICEBERG_RUST_TOKEN", "w".repeat(32))
            .env("CROWDB_ICEBERG_RUST_NAMESPACE", "rust_sdk_retired")
            .env("CROWDB_ICEBERG_RUST_RETIRE_CONTROL", control_address)
            .status()
            .unwrap()
    });
    let (mut signal, _) = tokio::time::timeout(Duration::from_secs(20), control.accept())
        .await
        .unwrap()
        .unwrap();
    signal.read_exact(&mut [0]).await.unwrap();
    let (root, authority) = repository.status().await.unwrap();
    let clear = ManagementRequest {
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: now_ms(),
        },
        principal: "manager".into(),
        action: ManagementAction::Clear,
        expected_epoch: root.context.activation_epoch,
        display_name: authority.display_name,
        confirmation: Some(root.context.catalog),
        capabilities: None,
    };
    assert!(matches!(
        repository
            .execute(clear.clone(), ManagementPrivilege::Clear, now_ms())
            .await,
        Err(CatalogError::Busy)
    ));
    let (root, _) = repository.status().await.unwrap();
    let crowdb_access_iceberg::catalog::RootState::Published(transition) = root.state else {
        panic!("clear did not publish its durable grace boundary");
    };
    let remaining = transition.complete_after_ms.saturating_sub(now_ms());
    assert!(remaining < (timeout_seconds - 10) * 1_000);
    tokio::time::sleep(Duration::from_millis(remaining + 10)).await;
    repository
        .execute(clear, ManagementPrivilege::Clear, now_ms())
        .await
        .unwrap();
    activate(repository).await;
    signal.write_all(&[1]).await.unwrap();
    assert!(client.await.unwrap().success());
}
