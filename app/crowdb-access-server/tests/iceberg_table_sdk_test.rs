#![cfg(feature = "iceberg-e2e")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Maven and pinned Apache Iceberg Java dependencies"]
async fn official_rest_catalog_reads_fixture_generations_without_fileio() {
    let fixture = fixture::TestTableHttp::new().await;
    for name in ["events", "a+b", "%2F"] {
        fixture.install(name).await;
    }
    let endpoint = fixture.endpoint();
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
            .args(["compile", "exec:java", "-Dexec.mainClass=TestIcebergCatalogReads"])
            .arg(format!("-Dexec.args={endpoint}"))
            .status()
            .unwrap()
    })
    .await
    .unwrap();
    fixture.finish().await;
    assert!(status.success(), "official RESTCatalog read acceptance failed");
}

#[tokio::test]
#[ignore = "requires Maven and pinned Apache Iceberg Java dependencies"]
async fn official_staged_catalog_preserves_exact_draft_credential_refresh_uri() {
    let status = tokio::task::spawn_blocking(|| {
        let maven = std::env::var_os("CROWDB_ICEBERG_E2E_MVN").unwrap_or_else(|| "mvn".into());
        std::process::Command::new("timeout")
            .arg("60")
            .arg(maven)
            .args(["-o", "--batch-mode", "--no-transfer-progress", "-f"])
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_java/pom.xml"
            ))
            .args([
                "compile",
                "exec:java",
                "-Dexec.mainClass=TestIcebergDraftCredentials",
            ])
            .status()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        status.success(),
        "official staged credential refresh acceptance failed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Maven and pinned Apache Iceberg Java dependencies"]
async fn official_catalog_creates_commits_upgrades_stages_and_refreshes_native_credentials() {
    let fixture = fixture::TestTableHttp::vending().await;
    let endpoint = fixture.endpoint();
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
            .args([
                "compile",
                "exec:java",
                "-Dexec.mainClass=TestIcebergCatalogWrites",
            ])
            .arg(format!("-Dexec.args={endpoint}"))
            .status()
            .unwrap()
    })
    .await
    .unwrap();
    fixture.finish().await;
    assert!(
        status.success(),
        "official native catalog write acceptance failed"
    );
}
