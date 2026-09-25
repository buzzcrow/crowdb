#![cfg(feature = "iceberg-e2e")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/iceberg_response_loss.rs"]
mod response_loss;

use std::{sync::Arc, time::Duration};

use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds},
    wire::BearerAuthenticator,
};
use crowdb_access_server::iceberg::{serve, IcebergHttpService};
use response_loss::TestResponseLossProxy;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Maven and pinned Apache Iceberg Java dependencies"]
async fn official_java_client_observes_lost_create_reply_on_another_listener() {
    let fixture = fixture::TestTableHttp::writable().await;
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
    let proxy = TestResponseLossProxy::start(fixture.endpoint(), "/v1/namespaces/analytics/tables").await;
    let origin = proxy.origin.clone();
    let status = tokio::task::spawn_blocking(move || {
        let maven = std::env::var_os("CROWDB_ICEBERG_E2E_MVN").unwrap_or_else(|| "mvn".into());
        std::process::Command::new("timeout")
            .arg("60")
            .arg(maven)
            .args(["-o", "--batch-mode", "--no-transfer-progress", "-f"])
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_java/pom.xml"
            ))
            .args(["compile", "exec:java", "-Dexec.mainClass=TestIcebergResponseLoss"])
            .arg(format!("-Dexec.args={origin} {second_origin}"))
            .status()
            .unwrap()
    })
    .await
    .unwrap();
    proxy.assert_dropped();
    stop.send(()).unwrap();
    server.await.unwrap();
    assert!(
        status.success(),
        "official Java REST client response-loss acceptance failed"
    );
    fixture.finish().await;
}
