// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::native_context::TestNativeContext;
use serde_json::Value;

async fn call(context: &TestNativeContext, method: &str, path: &str, body: Value) -> Value {
    let response = context
        .http
        .request(method.parse().unwrap(), format!("{}{}", context.base, path))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.bytes().await.unwrap();
    assert!(
        status.is_success(),
        "{path}: {status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}

pub(super) async fn s3_request(
    context: &TestNativeContext,
    method: &str,
    path: &str,
    body: Vec<u8>,
) -> Vec<u8> {
    let response = context
        .http
        .request(method.parse().unwrap(), format!("{}{}", context.base, path))
        .body(body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.bytes().await.unwrap();
    assert!(
        status.is_success(),
        "{path}: {status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    assert!(bytes.len() <= 16 * 1024 * 1024);
    bytes.to_vec()
}

#[tokio::test]
#[ignore = "requires the formal three-node storage and Access deployment"]
async fn multipart_locations_bind_actual_chunks_and_reject_stale_cursors() {
    let context = TestNativeContext::connect().await;
    super::native_storage::start(&context).await;
    assert_native_locations(&context).await;
}

async fn upload_native_multipart(app: &TestNativeContext, object: &str) -> Vec<u8> {
    let initialized = s3_request(app, "POST", &format!("{object}?uploads"), vec![]).await;
    let xml = String::from_utf8(initialized).unwrap();
    let upload = xml
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let mut payload = vec![0x51; 8 * 1024 * 1024];
    let tail = vec![0xa3; 1024 * 1024];
    s3_request(
        app,
        "PUT",
        &format!("{object}?uploadId={upload}&partNumber=1"),
        payload.clone(),
    )
    .await;
    s3_request(
        app,
        "PUT",
        &format!("{object}?uploadId={upload}&partNumber=2"),
        tail.clone(),
    )
    .await;
    // Completion validates the actual part digests, not synthetic location records.
    let parts = s3_request(app, "GET", &format!("{object}?uploadId={upload}"), vec![]).await;
    let parts = String::from_utf8(parts).unwrap();
    let etags: Vec<_> = parts
        .split("<ETag>")
        .skip(1)
        .map(|part| part.split_once("</ETag>").unwrap().0)
        .collect();
    assert_eq!(
        etags.len(),
        2,
        "ListParts response after two successful uploads: {parts}"
    );
    let completion = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", etags[0], etags[1]);
    s3_request(
        app,
        "POST",
        &format!("{object}?uploadId={upload}"),
        completion.into_bytes(),
    )
    .await;
    payload.extend(tail);
    payload
}

pub(super) async fn assert_native_locations(app: &TestNativeContext) {
    let bucket = "/api/access/s3/native-locations";
    s3_request(app, "PUT", bucket, vec![]).await;
    let object = format!("{bucket}/multipart.bin");
    let payload = upload_native_multipart(app, &object).await;
    assert_eq!(s3_request(app, "GET", &object, vec![]).await, payload);
    let inspect = "/api/access/s3-inspect/locations?bucket=native-locations&key=multipart.bin&limit=1";
    let first = call(app, "GET", inspect, Value::Null).await;
    assert_eq!(first["logical_length"], payload.len().to_string());
    assert_eq!(first["locations"].as_array().unwrap().len(), 1);
    assert_eq!(first["locations"][0]["logical_offset"], "0");
    assert_eq!(
        first["locations"][0]["logical_length"],
        (8 * 1024 * 1024).to_string()
    );
    let cursor = first["next_cursor"]
        .as_str()
        .expect("multipart location continuation");
    let cursor: String =
        percent_encoding::utf8_percent_encode(cursor, percent_encoding::NON_ALPHANUMERIC).collect();
    let next_path = format!("{inspect}&cursor={cursor}");
    let second = call(app, "GET", &next_path, Value::Null).await;
    assert_eq!(second["generation"], first["generation"]);
    assert!(second["next_cursor"].is_null());
    assert_eq!(
        second["locations"][0]["logical_offset"],
        (8 * 1024 * 1024).to_string()
    );
    assert_eq!(
        second["locations"][0]["logical_length"],
        (1024 * 1024).to_string()
    );
    assert_ne!(
        first["locations"][0]["chunk_id"],
        second["locations"][0]["chunk_id"]
    );
    assert_eq!(first["locations"][0]["offset"], "0");
    for page in [&first, &second] {
        let id = page["locations"][0]["chunk_id"].as_str().unwrap();
        let detail = call(app, "GET", &format!("/api/chunks/{id}"), Value::Null).await;
        assert_eq!(detail["chunk"]["id_hex"], id);
        assert!(!detail["chunk"]["strips"].as_array().unwrap().is_empty());
    }
    let id = first["locations"][0]["chunk_id"].as_str().unwrap();
    let chunkdb = crowdb_chunkdb_client::ChunkdbClient::new(
        crowdb_kv_client::ServiceRegistryClient::from_shared(app.kv.clone()),
        std::sync::Arc::new(crowdb_chunkdb_client::ChunkdbRpcTransport::new()),
    );
    let chunk = chunkdb
        .query_chunk(crowdb_protocol::chunkdb::rpc::QueryChunkRequest {
            chunk_id: Some(crowdb_protocol::common::ChunkId {
                high: u64::from_str_radix(&id[..16], 16).unwrap(),
                low: u64::from_str_radix(&id[16..], 16).unwrap(),
            }),
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    let physical = first["locations"][0]["length"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_ne!(physical % 1024, 0);
    assert_eq!(chunk.acknowledged_cursor, physical);
    s3_request(app, "PUT", &object, b"replacement".to_vec()).await;
    let response = app
        .http
        .get(format!("{}{}", app.base, next_path))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 409);
}
