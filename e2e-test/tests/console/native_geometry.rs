// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::native_context::TestNativeContext;
use reqwest::Method;
use serde_json::{json, Value};

#[tokio::test]
#[ignore = "requires normal DiskDB ownership and public geometry validation"]
async fn mixed_units_reject_consumers_without_starting_services() {
    let context = TestNativeContext::connect().await;
    super::native_storage::start_diskdb(&context, [128 * 1024, 1024 * 1024, 1024 * 1024]).await;
    let original = context.api(Method::GET, "/api/servers", Value::Null, 200).await;
    assert_eq!(original.as_array().unwrap().len(), 6);
    for (kind, http, rpc) in [("chunkdb", 7300, 7301), ("chunk-kv", 7400, 7401)] {
        let mut body = context
            .api(
                Method::GET,
                "/api/deployment-defaults?node_id=1",
                Value::Null,
                200,
            )
            .await[kind]
            .clone();
        body["kind"] = json!(kind);
        body["local_deployment"] = json!(false);
        body["http_port"] = json!(http);
        body["rpc_port"] = json!(rpc);
        if kind == "chunk-kv" {
            body["metadata_store_id"] = json!(0);
            body["bootstrap_group_id"] = json!(2);
        }
        let response = context
            .http
            .post(format!("{}/api/nodes/1/services/deploy", context.base))
            .json(&body)
            .send()
            .await;
        let response = response.unwrap();
        let status = response.status();
        let error = response.text().await.unwrap();
        assert_eq!(status.as_u16(), 409, "{error}");
        assert!(error.contains("uniform allocation unit"), "{error}");
        assert!(error.contains("131072") && error.contains("1048576"), "{error}");
    }
    let current = context.api(Method::GET, "/api/servers", Value::Null, 200).await;
    assert_eq!(
        current, original,
        "no incompatible service spawned or prior owner replaced"
    );
}
