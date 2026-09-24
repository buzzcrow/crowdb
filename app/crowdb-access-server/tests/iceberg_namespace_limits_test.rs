#![cfg(feature = "iceberg")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;

use std::collections::BTreeMap;

use crowdb_access_iceberg::namespace::{NamespaceIdentifier, NamespaceProperties, NamespaceRepository};
use crowdb_access_iceberg::record::{StorageRecord, MAX_RECORD_BYTES};
use fixture::TestTableHttp;
use reqwest::Method;
use serde_json::{json, Value};

const NAMESPACE: &str = "/v1/namespaces/analytics";
const PROPERTIES: &str = "/v1/namespaces/analytics/properties";

async fn properties(test: &TestTableHttp) -> Value {
    let response = test.request(Method::GET, NAMESPACE, "r", None).await;
    assert_eq!(response.status().as_u16(), 200);
    response.json::<Value>().await.unwrap()["properties"].clone()
}

async fn update(test: &TestTableHttp, changes: Value, expected: u16) -> Value {
    let before = properties(test).await;
    let authority_key = crowdb_access_iceberg::namespace::authority_key(test.context.catalog, test.namespace)
        .encode()
        .unwrap();
    let authority = test.store.values.load()[&authority_key].clone();
    let response = test.post(PROPERTIES, "w", None, &changes).await;
    let status = response.status().as_u16();
    let body = response.text().await.unwrap();
    assert_eq!(status, expected, "{body}");
    let result: Value = serde_json::from_str(&body).unwrap();
    if expected != 200 {
        assert_eq!(result["error"]["code"], expected);
        assert_eq!(properties(test).await, before);
        let after = test.store.values.load()[&authority_key].clone();
        assert_eq!(after.bytes, authority.bytes);
        assert_eq!(after.revision, authority.revision);
    }
    result
}

#[tokio::test]
async fn property_cardinality_replacement_and_overlap_are_atomic_over_http() {
    let test = TestTableHttp::new().await;
    let mut full: BTreeMap<_, _> = (0..256).map(|index| (index.to_string(), "value")).collect();
    update(&test, json!({"updates": full}), 200).await;
    assert_eq!(properties(&test).await, json!(full));
    update(&test, json!({"updates":{"overflow":"value"}}), 400).await;
    let result = update(
        &test,
        json!({"removals":["0"],"updates":{"replacement":"new"}}),
        200,
    )
    .await;
    assert_eq!(result["removed"], json!(["0"]));
    assert_eq!(result["updated"], json!(["replacement"]));
    let retained = properties(&test).await;
    assert_eq!(retained.as_object().unwrap().len(), 256);
    assert!(retained.get("0").is_none());
    assert_eq!(retained["replacement"], "new");
    full.remove("0");
    full.insert("replacement".into(), "new");
    assert_eq!(retained, json!(full));
    update(
        &test,
        json!({"removals":["replacement"],"updates":{"replacement":"bad"}}),
        422,
    )
    .await;
    test.finish().await;
}

#[tokio::test]
async fn property_utf8_key_and_value_byte_limits_fail_without_mutation() {
    let test = TestTableHttp::new().await;
    let key = format!("{}a", "键".repeat(341));
    let value = "值".repeat(2730) + "ab";
    update(&test, json!({"updates":{key.clone():value.clone()}}), 200).await;
    assert_eq!(properties(&test).await, json!({key.clone():value.clone()}));
    for (invalid_key, invalid_value) in [
        (key.clone() + "b", value.clone()),
        (key.clone(), value + "c"),
        ("nul\0key".into(), "valid".into()),
        ("valid".into(), "nul\0value".into()),
    ] {
        update(&test, json!({"updates":{invalid_key:invalid_value}}), 400).await;
    }
    test.finish().await;
}

#[tokio::test]
async fn encoded_authority_limit_is_checked_before_http_property_publication() {
    let test = TestTableHttp::new().await;
    let mut authority = NamespaceRepository::new(test.store.clone())
        .load(
            test.context,
            &NamespaceIdentifier::new(vec!["analytics".into()]).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let mut entries: BTreeMap<_, _> = (0..7)
        .map(|index| (index.to_string(), "v".repeat(8192)))
        .collect();
    let mut lower = 0_usize;
    let mut upper = 8192_usize;
    while lower < upper {
        let middle = (lower + upper).div_ceil(2);
        entries.insert("tail".into(), "v".repeat(middle));
        authority.properties = NamespaceProperties::new(entries.clone()).unwrap();
        if StorageRecord::NamespaceAuthority(Box::new(authority.clone()))
            .encode()
            .is_ok()
        {
            lower = middle;
        } else {
            upper = middle - 1;
        }
    }
    assert!(lower > 0 && lower < 8192);
    entries.insert("tail".into(), "v".repeat(lower));
    authority.properties = NamespaceProperties::new(entries.clone()).unwrap();
    let encoded = StorageRecord::NamespaceAuthority(Box::new(authority))
        .encode()
        .unwrap();
    assert!(encoded.len() <= MAX_RECORD_BYTES && encoded.len() + 8 > MAX_RECORD_BYTES);
    update(&test, json!({"updates":entries}), 200).await;
    assert_eq!(properties(&test).await, json!(entries));
    update(&test, json!({"updates":{"tail":"v".repeat(lower + 1)}}), 400).await;
    test.finish().await;
}
