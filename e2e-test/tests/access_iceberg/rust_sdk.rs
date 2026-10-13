// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::response_loss::TestResponseLossProxy;
use super::{
    frontend::TestFrontend,
    native::{activate, TestNativeStorage},
    now_ms,
};
use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds, ManagementPrivilege},
    key::OperationId,
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires pinned Apache Iceberg Rust client and native storage"]
async fn official_rust_client_lost_reply_survives_native_storage_restart() {
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
                display_name: "rust-native".into(),
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
    let proxy =
        TestResponseLossProxy::start(first.origin.clone(), "/v1/namespaces/rust_sdk_loss/tables").await;
    assert!(run_rust_fixture(&proxy.origin, &second.origin.clone(), true, false, true).await);
    proxy.assert_dropped();
    first.stop().await;
    second.stop().await;
    stack.restart_catalog().await;
    let first = TestFrontend::start(12).await;
    let second = TestFrontend::start(13).await;
    assert!(run_rust_fixture(&first.origin.clone(), &second.origin.clone(), false, true, false).await);
}

async fn run_rust_fixture(
    origin: &str,
    second_origin: &str,
    response_loss: bool,
    verify_existing: bool,
    keep_table: bool,
) -> bool {
    let origin = origin.to_owned();
    let second_origin = second_origin.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut command = std::process::Command::new("timeout");
        command
            .arg("600")
            .arg("pixi")
            .args(["run", "--"])
            .arg(std::env::var_os("CROWDB_ICEBERG_RUST_CLIENT_BIN").expect("build Rust SDK fixture first"))
            .env("CROWDB_ICEBERG_RUST_ORIGIN", origin)
            .env("CROWDB_ICEBERG_RUST_SECOND_ORIGIN", second_origin)
            .env("CROWDB_ICEBERG_RUST_TOKEN", "w".repeat(32))
            .env("CROWDB_ICEBERG_RUST_NAMESPACE", "rust_sdk_loss");
        if response_loss {
            command.env("CROWDB_ICEBERG_RUST_RESPONSE_LOSS", "1");
        }
        if verify_existing {
            command.env("CROWDB_ICEBERG_RUST_VERIFY_EXISTING", "1");
        }
        if keep_table {
            command.env("CROWDB_ICEBERG_RUST_KEEP_TABLE", "1");
        }
        command.status().unwrap().success()
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the pinned Apache client and owned release frontends"]
async fn official_rust_client_namespace_and_table_lifecycle() {
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
                display_name: "rust-native".into(),
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
    assert!(run_rust_fixture(&first.origin, &second.origin, false, false, false).await);
}
