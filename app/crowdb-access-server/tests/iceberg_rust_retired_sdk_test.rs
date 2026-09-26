#![cfg(feature = "iceberg-e2e")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/iceberg_stack.rs"]
#[allow(dead_code)]
mod native_stack;
#[path = "common/iceberg_process.rs"]
#[allow(dead_code)]
mod process;

use std::{sync::Arc, time::Duration};

use crowdb_access_iceberg::{
    catalog::{CatalogError, CatalogRepository, ClearBounds, ManagementPrivilege},
    key::OperationId,
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
    wire::BearerAuthenticator,
};
use crowdb_access_server::iceberg::{serve, IcebergHttpService};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "builds the pinned official Apache Iceberg Rust client"]
async fn official_rust_client_rejects_retired_catalog_after_clear() {
    let fixture = fixture::TestTableHttp::writable().await;
    let repository = Arc::new(CatalogRepository::new(fixture.store.clone(), ClearBounds::default()).unwrap());
    let service = IcebergHttpService::new(
        repository.clone(),
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap(),
        Duration::from_secs(2),
    )
    .with_namespaces(fixture.store.clone())
    .unwrap()
    .with_tables(fixture.store.clone(), Arc::new(blocks::TestFileBlocks::default()))
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_origin = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        serve(listener, Arc::new(service), async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    run_client_across_clear(&fixture.endpoint(), &second_origin, &repository, 60).await;
    stop.send(()).unwrap();
    server.await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires pinned Apache Iceberg Rust client and native retirement grace"]
async fn official_rust_client_rejects_retired_native_catalog_after_full_grace() {
    let stack = native_stack::TestIcebergStack::start().await;
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
    native_stack::activate(&repository).await;
    let first = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let second = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    run_client_across_clear(
        &format!("http://{}", first.address),
        &format!("http://{}", second.address),
        &repository,
        1_500,
    )
    .await;
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
            .args(["run", "cargo", "run", "--locked", "--manifest-path"])
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_rust/Cargo.toml"
            ))
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
    common::activate(repository).await;
    signal.write_all(&[1]).await.unwrap();
    assert!(client.await.unwrap().success());
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
