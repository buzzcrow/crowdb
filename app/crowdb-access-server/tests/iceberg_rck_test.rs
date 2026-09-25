#![cfg(feature = "iceberg-e2e")]

#[path = "common/iceberg_stack.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/iceberg_process.rs"]
#[allow(dead_code)]
mod process;

use std::time::Duration;

use common::{now_ms, TestIcebergStack};
use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds, ManagementPrivilege},
    key::OperationId,
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires pinned Apache Iceberg 1.11.0 source, Gradle and native storage"]
async fn apache_rest_compatibility_kit_basic_create() {
    let source =
        std::env::var("CROWDB_ICEBERG_RCK_ROOT").expect("set the pinned Apache Iceberg 1.11.0 source root");
    let revision = std::process::Command::new("git")
        .args(["-C", &source, "rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(revision.status.success());
    assert_eq!(
        String::from_utf8(revision.stdout).unwrap().trim(),
        "6976e020b894f6a6777704df2b8c4458cb291ae9"
    );
    let stack = TestIcebergStack::start().await;
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
                display_name: "rest-kit".into(),
                confirmation: None,
            },
            ManagementPrivilege::Manage,
            now_ms(),
        )
        .await
        .unwrap();
    let process = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let origin = format!("http://{}", process.address);
    let selector = std::env::var("CROWDB_ICEBERG_RCK_SELECTOR").unwrap_or_else(|_| {
        "org.apache.iceberg.rest.RESTCompatibilityKitCatalogTests.testBasicCreateTable".into()
    });
    let result = tokio::task::spawn_blocking(move || {
        std::process::Command::new("timeout")
            .arg("900")
            .arg("./gradlew")
            .arg(":iceberg-open-api:test")
            .arg("--tests")
            .arg(selector)
            .args([
                "--no-daemon",
                "-Drck.local=false",
                "-Drck.requires-namespace-create=true",
            ])
            .env("CATALOG_URI", origin)
            .env("CATALOG_WAREHOUSE", "")
            .env("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .env("CATALOG_TOKEN", "w".repeat(32))
            .env(
                "JAVA_HOME",
                concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../.pixi/envs/iceberg-e2e/lib/jvm"
                ),
            )
            .current_dir(source)
            .status()
            .unwrap()
    });
    let status = tokio::time::timeout(Duration::from_secs(930), result)
        .await
        .unwrap()
        .unwrap();
    assert!(
        status.success(),
        "Apache Iceberg 1.11.0 REST Compatibility Kit selected catalog tests failed"
    );
}
