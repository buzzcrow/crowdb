#![cfg(feature = "iceberg")]

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
    key::{CatalogScope, IcebergKey, OperationId, TableId},
    record::StorageRecord,
    table::{head_key, name_key, TableHead, TableLifecycle, TableMapping, TableMappingState},
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
    test.put(
        &head_key(dropped.catalog, dropped.table),
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
                IcebergKey::decode(key),
                Ok(IcebergKey::Catalog {
                    scope: CatalogScope::TableHead
                        | CatalogScope::TableName
                        | CatalogScope::File
                        | CatalogScope::FileLocation
                        | CatalogScope::Reclamation,
                    ..
                })
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
async fn lost_drop_publication_reply_replays_once_and_preserves_files_for_both_purge_modes() {
    for purge in [false, true] {
        let test = TestTableHttp::writable().await;
        create(&test).await;
        let before = authority(&test);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let identity = format!("{:08x}-{:04x}-7000-8000-000000000001", now >> 16, now & 0xffff);
        let path = format!("{}{TABLE}?purgeRequested={purge}", test.endpoint());
        let client = reqwest::Client::new();
        test.store.lose_reply_kind.store(5, Ordering::SeqCst);
        let failed = client
            .delete(&path)
            .bearer_auth("w".repeat(32))
            .header("idempotency-key", &identity)
            .send()
            .await
            .unwrap();
        value(failed, 503).await;
        assert_eq!(test.request(Method::HEAD, TABLE, "r", None).await.status(), 404);
        for _ in 0..2 {
            let response = client
                .delete(&path)
                .bearer_auth("w".repeat(32))
                .header("idempotency-key", &identity)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 204);
            assert!(response.bytes().await.unwrap().is_empty());
        }
        let after = authority(&test);
        for (key, value) in before {
            if matches!(
                IcebergKey::decode(&key).unwrap(),
                IcebergKey::Catalog {
                    scope: CatalogScope::File | CatalogScope::FileLocation,
                    ..
                }
            ) {
                assert_eq!(after.get(&key), Some(&value));
            }
        }
        let tasks = after
            .keys()
            .filter(|key| {
                matches!(
                    IcebergKey::decode(key).unwrap(),
                    IcebergKey::Catalog {
                        scope: CatalogScope::Reclamation,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(tasks, usize::from(purge));
        test.finish().await;
    }
}
