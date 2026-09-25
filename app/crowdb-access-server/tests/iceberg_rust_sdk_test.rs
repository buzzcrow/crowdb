#![cfg(feature = "iceberg-e2e")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds},
    wire::BearerAuthenticator,
};
use crowdb_access_server::iceberg::{serve, IcebergHttpService};
use fixture::TestTableHttp;
use std::{sync::Arc, time::Duration};

#[tokio::test]
#[ignore = "builds the pinned official Apache Iceberg Rust client"]
async fn official_rust_client_namespace_and_table_lifecycle() {
    let fixture = TestTableHttp::writable().await;
    let origin = fixture.endpoint();
    let service = IcebergHttpService::new(
        Arc::new(CatalogRepository::new(fixture.store.clone(), ClearBounds::default()).unwrap()),
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
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new("timeout")
            .arg("600")
            .arg("pixi")
            .args(["run", "cargo", "run", "--locked", "--manifest-path"])
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_rust/Cargo.toml"
            ))
            .env("CROWDB_ICEBERG_RUST_ORIGIN", origin)
            .env("CROWDB_ICEBERG_RUST_SECOND_ORIGIN", second_origin)
            .env("CROWDB_ICEBERG_RUST_TOKEN", "w".repeat(32))
            .env("CROWDB_ICEBERG_RUST_NAMESPACE", "rust_sdk")
            .status()
            .unwrap()
    })
    .await
    .unwrap();
    stop.send(()).unwrap();
    server.await.unwrap();
    assert!(status.success(), "official Rust REST client failed");
    fixture.finish().await;
}
