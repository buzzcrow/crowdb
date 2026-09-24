#![cfg(feature = "iceberg-e2e")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    catalog::StoredValue,
    key::{NamespaceId, OperationId},
    namespace::{
        authority_key, name_key, NamespaceAuthority, NamespaceIdentifier, NamespaceLifecycle,
        NamespaceMapping, NamespaceMappingState, NamespaceProperties,
    },
    record::StorageRecord,
};
use fixture::TestTableHttp;

fn install(test: &TestTableHttp, count: usize, padding: usize, stale: bool) {
    let mut values = (*test.store.values.load_full()).clone();
    for index in 0..count {
        let name = format!("{index:04}{}", "\"".repeat(padding));
        let namespace = NamespaceId::random();
        let mapping = NamespaceMapping {
            catalog: test.context.catalog,
            parent: None,
            name: name.clone(),
            namespace,
            name_epoch: 1,
            operation: OperationId::random(),
            state: NamespaceMappingState::Published,
        };
        values.insert(
            name_key(test.context.catalog, None, &name)
                .unwrap()
                .encode()
                .unwrap(),
            StoredValue {
                bytes: StorageRecord::NamespaceMapping(mapping).encode().unwrap(),
                revision: 1,
            },
        );
        if !stale {
            let authority = NamespaceAuthority {
                catalog: test.context.catalog,
                namespace,
                parent: None,
                identifier: NamespaceIdentifier::new(vec![name]).unwrap(),
                name_epoch: 1,
                property_revision: 1,
                admission_fence: 1,
                mutation_revision: 1,
                lifecycle: NamespaceLifecycle::Ready,
                pending_operation: None,
                properties: NamespaceProperties::default(),
            };
            values.insert(
                authority_key(test.context.catalog, namespace).encode().unwrap(),
                StoredValue {
                    bytes: StorageRecord::NamespaceAuthority(Box::new(authority))
                        .encode()
                        .unwrap(),
                    revision: 1,
                },
            );
        }
    }
    test.store.values.store(Arc::new(values));
}

async fn python(test: &TestTableHttp, mode: &str, count: usize) {
    let endpoint = test.endpoint();
    let mode = mode.to_owned();
    let status = tokio::task::spawn_blocking(move || {
        let python = std::env::var_os("CROWDB_ICEBERG_E2E_PYTHON").expect("set pinned Python path");
        std::process::Command::new("timeout")
            .arg("60")
            .arg(python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_namespace_client.py"
            ))
            .args([endpoint, mode, count.to_string()])
            .status()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        status.success(),
        "official namespace complete-list acceptance failed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires pinned PyIceberg environment"]
async fn official_complete_listing_rejects_each_spool_limit_and_releases_resources() {
    let test = TestTableHttp::new().await;
    let baseline = test.store.values.load_full();
    test.store.scan_delay_ms.store(500, Ordering::SeqCst);
    python(&test, "concurrency", 1).await;
    test.store.scan_delay_ms.store(0, Ordering::SeqCst);
    python(&test, "complete", 1).await;
    for (count, padding, stale) in [(1024, 0, false), (400, 3500, false), (4100, 0, true)] {
        install(&test, count, padding, stale);
        python(&test, "overflow", 0).await;
        test.store.values.store(baseline.clone());
        python(&test, "complete", 1).await;
    }
    test.store.scan_delay_ms.store(2500, Ordering::SeqCst);
    python(&test, "overflow", 0).await;
    test.store.scan_delay_ms.store(0, Ordering::SeqCst);
    python(&test, "complete", 1).await;
    install(&test, 10, 0, false);
    python(&test, "complete", 11).await;
    test.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Maven and pinned Apache Iceberg Java dependencies"]
async fn official_catalog_continues_through_empty_namespace_pages() {
    let test = TestTableHttp::new().await;
    install(&test, 3, 0, true);
    let mapping = NamespaceMapping {
        catalog: test.context.catalog,
        parent: None,
        name: "zzzzzzzzz".into(),
        namespace: NamespaceId::random(),
        name_epoch: 1,
        operation: OperationId::random(),
        state: NamespaceMappingState::Published,
    };
    test.put(
        &name_key(test.context.catalog, None, &mapping.name).unwrap(),
        &StorageRecord::NamespaceMapping(mapping.clone()),
    );
    let endpoint = test.endpoint();
    let status = tokio::task::spawn_blocking(move || {
        let maven = std::env::var_os("CROWDB_ICEBERG_E2E_MVN").expect("set pinned Maven path");
        std::process::Command::new("timeout")
            .arg("60")
            .arg(maven)
            .args(["-o", "--batch-mode", "--no-transfer-progress", "-f"])
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_java/pom.xml"
            ))
            .args(["compile", "exec:java", "-Dexec.mainClass=TestIcebergNamespaces"])
            .arg(format!("-Dexec.args={endpoint}"))
            .status()
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(test.store.scans.load(Ordering::SeqCst), 7);
    test.finish().await;
    assert!(
        status.success(),
        "official namespace pagination acceptance failed"
    );
}
