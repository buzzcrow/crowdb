#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;

use std::{collections::BTreeMap, sync::atomic::Ordering, time::Duration};

use crowdb_access_iceberg::{
    catalog::StoredValue,
    key::{CatalogScope, IcebergKey, OperationId, StorageKey, TableId, TableStorageKey, TableStorageScope},
    record::StorageRecord,
    table::{name_key, storage_head_key, TableHead, TableLifecycle, TableMapping, TableMappingState},
};
use fixture::TestTableHttp;
use reqwest::Method;
use serde_json::{json, Value};

const TABLES: &str = "/v1/namespaces/analytics/tables";
const TABLE: &str = "/v1/namespaces/analytics/tables/events";

async fn value(response: reqwest::Response, status: u16) -> Value {
    let actual = response.status().as_u16();
    let body = response.text().await.unwrap();
    assert_eq!(actual, status, "{body}");
    serde_json::from_str(&body).unwrap()
}

async fn create(test: &TestTableHttp) -> Value {
    value(
        test.post(
            TABLES,
            "w",
            None,
            &json!({"name":"events", "schema":{
        "type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"long","required":true}]}}),
        )
        .await,
        200,
    )
    .await
}

#[tokio::test]
async fn concurrent_commit_cannot_mix_all_refs_or_conditional_http_loads() {
    let test = TestTableHttp::writable().await;
    create(&test).await;
    for mode in ["all", "refs"] {
        for conditional in [false, true] {
            let path = format!("{TABLE}?snapshots={mode}");
            let before = test.request(Method::GET, &path, "r", None).await;
            assert_eq!(before.status(), 200);
            let etag = before.headers()["etag"].to_str().unwrap().to_owned();
            before.bytes().await.unwrap();
            test.store.pause_file_read.store(true, Ordering::SeqCst);
            let read = test.request(Method::GET, &path, "r", conditional.then_some(etag.as_str()));
            tokio::pin!(read);
            tokio::select! {
                response = &mut read => panic!("read completed before barrier: {}", response.status()),
                () = test.store.file_read_entered.notified() => {},
                () = tokio::time::sleep(Duration::from_secs(1)) => panic!("file read did not reach barrier"),
            }
            let marker = format!("{mode}-{conditional}");
            value(
                test.post(
                    TABLE,
                    "w",
                    None,
                    &json!({"requirements":[], "updates":[
                        {"action":"set-properties","updates":{"marker":marker}}
                    ]}),
                )
                .await,
                200,
            )
            .await;
            test.store.file_read_release.notify_one();
            let failed = value(read.await, 503).await;
            assert!(failed.get("metadata").is_none());
            let after = test.request(Method::GET, &path, "r", Some(&etag)).await;
            assert_ne!(after.headers()["etag"], etag);
            assert_eq!(
                value(after, 200).await["metadata"]["properties"]["marker"],
                marker
            );
        }
    }
    test.finish().await;
}

fn mapping(test: &TestTableHttp, head: &TableHead, name: &str, state: TableMappingState) {
    test.put(
        &name_key(head.catalog, head.namespace, name).unwrap(),
        &StorageRecord::TableMapping(TableMapping {
            storage_namespace: head.storage_location().storage_namespace,
            catalog: head.catalog,
            namespace: head.namespace,
            name: name.into(),
            table: head.table,
            name_epoch: head.name_epoch,
            operation: OperationId::random(),
            state,
        }),
    );
}

#[tokio::test]
async fn mixed_mapping_pages_and_exists_expose_only_current_table_heads() {
    let test = TestTableHttp::new().await;
    let (head, _) = test.install("events").await;
    mapping(&test, &head, "alias", TableMappingState::Published);
    mapping(&test, &head, "reserved", TableMappingState::Reserved);
    let mut absent = head.clone();
    absent.table = TableId::random();
    mapping(&test, &absent, "missing", TableMappingState::Published);
    let (mut dropped, _) = test.install("dropped").await;
    dropped.lifecycle = TableLifecycle::Tombstone;
    dropped.pending_operation = Some(OperationId::random());
    test.put_storage(
        &storage_head_key(dropped.storage_location()),
        &StorageRecord::TableHead(Box::new(dropped)),
    );
    let mut token = String::new();
    let mut names = Vec::new();
    let mut pages = 0;
    let scans = test.store.scans.load(Ordering::SeqCst);
    loop {
        let path = format!("{TABLES}?pageSize=1&pageToken={token}");
        let page = value(test.request(Method::GET, &path, "r", None).await, 200).await;
        names.extend(page["identifiers"].as_array().unwrap().iter().cloned());
        pages += 1;
        assert!(pages <= 5);
        match page["next-page-token"].as_str() {
            Some(next) => token = next.into(),
            None => break,
        }
    }
    assert_eq!(pages, 5);
    assert_eq!(test.store.scans.load(Ordering::SeqCst) - scans, 5);
    assert_eq!(names, vec![json!({"namespace":["analytics"],"name":"events"})]);
    let complete = value(test.request(Method::GET, TABLES, "r", None).await, 200).await;
    assert_eq!(complete["identifiers"], json!(names));
    assert!(complete["next-page-token"].is_null());
    for name in ["alias", "reserved", "missing", "dropped", "events"] {
        let response = test
            .request(Method::HEAD, &format!("{TABLES}/{name}"), "r", None)
            .await;
        assert_eq!(
            response.status().as_u16(),
            if name == "events" { 204 } else { 404 }
        );
        assert!(response.bytes().await.unwrap().is_empty());
    }
    test.finish().await;
}

fn authority(test: &TestTableHttp) -> BTreeMap<Vec<u8>, StoredValue> {
    test.store
        .values
        .load()
        .iter()
        .filter(|(key, _)| {
            matches!(
                StorageKey::decode(key),
                Ok(StorageKey::Legacy(IcebergKey::Catalog {
                    scope: CatalogScope::TableHead
                        | CatalogScope::TableName
                        | CatalogScope::File
                        | CatalogScope::FileLocation
                        | CatalogScope::Reclamation,
                    ..
                }) | StorageKey::NativeTable(TableStorageKey {
                    scope: TableStorageScope::Head | TableStorageScope::File,
                    ..
                }))
            )
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

#[tokio::test]
async fn unsupported_table_operations_do_not_change_any_table_or_file_authority() {
    let test = TestTableHttp::writable().await;
    create(&test).await;
    let before = authority(&test);
    for path in [
        "/v1/namespaces/analytics/register",
        "/v1/namespaces/analytics/tables/events/unknown",
    ] {
        let error = value(test.post(path, "w", None, &json!({})).await, 406).await;
        assert_eq!(error["error"]["type"], "UnsupportedOperationException");
        assert_eq!(authority(&test), before);
    }
    test.finish().await;
}

#[tokio::test]
async fn lost_drop_reply_retries_the_current_name_and_preserves_exact_purge_ownership() {
    for purge in [false, true] {
        let test = TestTableHttp::writable().await;
        let original = create(&test).await;
        let before = authority(&test);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let identity = format!("{:08x}-{:04x}-7000-8000-000000000001", now >> 16, now & 0xffff);
        let path = format!("{}{TABLE}?purgeRequested={purge}", test.endpoint());
        let client = reqwest::Client::new();
        test.store.lose_reply_kind.store(5, Ordering::SeqCst);
        let failed = delete(&client, &path, &identity).await;
        value(failed, 503).await;
        assert_eq!(test.request(Method::HEAD, TABLE, "r", None).await.status(), 404);
        for _ in 0..2 {
            value(delete(&client, &path, &identity).await, 404).await;
        }
        let after = authority(&test);
        assert_files_preserved(&before, &after);
        let tasks = after
            .keys()
            .filter(|key| {
                matches!(
                    StorageKey::decode(key).unwrap(),
                    StorageKey::Legacy(IcebergKey::Catalog {
                        scope: CatalogScope::Reclamation,
                        ..
                    })
                )
            })
            .count();
        assert_eq!(tasks, usize::from(purge));
        let replacement = create(&test).await;
        assert_ne!(
            original["metadata"]["table-uuid"],
            replacement["metadata"]["table-uuid"]
        );
        let replacement_authority: BTreeMap<_, _> = authority(&test)
            .into_iter()
            .filter(|(key, stored)| after.get(key) != Some(stored))
            .collect();
        assert!(replacement_authority.keys().any(|key| is_file(key)));
        assert_eq!(
            value(test.request(Method::GET, TABLE, "r", None).await, 200).await["metadata"],
            replacement["metadata"]
        );
        let response = delete(&client, &path, &identity).await;
        assert_eq!(response.status(), 204);
        assert!(response.bytes().await.unwrap().is_empty());
        assert_files_preserved(&replacement_authority, &authority(&test));
        value(delete(&client, &path, &identity).await, 404).await;
        test.finish().await;
    }
}

async fn delete(client: &reqwest::Client, path: &str, identity: &str) -> reqwest::Response {
    client
        .delete(path)
        .bearer_auth("w".repeat(32))
        .header("idempotency-key", identity)
        .send()
        .await
        .unwrap()
}

fn is_file(key: &[u8]) -> bool {
    matches!(
        StorageKey::decode(key).unwrap(),
        StorageKey::Legacy(IcebergKey::Catalog {
            scope: CatalogScope::File | CatalogScope::FileLocation,
            ..
        }) | StorageKey::NativeTable(TableStorageKey {
            scope: TableStorageScope::File,
            ..
        })
    )
}

fn assert_files_preserved(before: &BTreeMap<Vec<u8>, StoredValue>, after: &BTreeMap<Vec<u8>, StoredValue>) {
    for (key, value) in before.iter().filter(|(key, _)| is_file(key)) {
        assert_eq!(after.get(key), Some(value));
    }
}
