// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::access_iceberg::{now_ms, signed_file as signed};
use crowdb_access_iceberg::file::{FileGrant, FileGrantIssuer, FileOperation, FileOperations, TableLocation};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::storage;
use crowdb_access_iceberg::wire::BearerAuthenticator;
use crowdb_chunk_client::{ChunkReadPolicy, SmallWritePolicy};
use crowdb_chunkdb_client::ChunkdbRpcTransport;
use crowdb_console_shared::ops::s3;
use crowdb_protocol::chunkdb::rpc::{ChunkType, ListChunksRequest, Strip};
use reqwest::{Client, Method};
use std::net::SocketAddr;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned combined Access release container"]
async fn combined_http_listeners_keep_protocol_chunk_policies_separate() {
    let ips = crate::common::management::addresses(13);
    let seeds: Vec<_> = ips[..3]
        .iter()
        .map(|ip| crate::common::management::origin(ip))
        .collect();
    write_s3(format!("{}:7500", ips[12]).parse().unwrap()).await;
    write_iceberg(format!("{}:7502", ips[12]).parse().unwrap(), &seeds).await;
    assert_chunk_layouts(&ips[4]).await;
}

async fn write_s3(s3_addr: SocketAddr) {
    let s3_client = s3::S3HttpClient::new(format!("http://{s3_addr}")).unwrap();
    s3_client
        .request(Method::PUT, Some("policy"), None, &[], None, None)
        .await
        .unwrap();
    for (name, bytes) in [
        ("small-a", vec![0x31; 60 * 1024]),
        ("small-b", vec![0x33; 20 * 1024]),
        ("large", vec![0x32; 2 * 1024 * 1024]),
    ] {
        s3_client
            .request(
                Method::PUT,
                Some("policy"),
                Some(name),
                &[],
                Some(bytes.clone()),
                None,
            )
            .await
            .unwrap();
        let (_, read) = s3_client
            .request(Method::GET, Some("policy"), Some(name), &[], None, None)
            .await
            .unwrap();
        assert_eq!(read, bytes);
    }
}

async fn write_iceberg(iceberg_addr: SocketAddr, seeds: &[String]) {
    let client = Client::new();
    let endpoint = format!("http://{iceberg_addr}");
    let namespace = client
        .post(format!("{endpoint}/v1/namespaces"))
        .bearer_auth("w".repeat(32))
        .json(&serde_json::json!({"namespace": ["analytics"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(namespace.status(), 200, "{}", namespace.text().await.unwrap());
    let draft = client
        .post(format!("{endpoint}/v1/namespaces/analytics/tables"))
        .bearer_auth("w".repeat(32))
        .json(&serde_json::json!({"name": "files", "stage-create": true,
            "schema": {"type": "struct", "fields": []}}))
        .send()
        .await
        .unwrap();
    assert_eq!(draft.status(), 200, "{}", draft.text().await.unwrap());
    let draft: serde_json::Value = draft.json().await.unwrap();
    let table: TableLocation = format!("{}/", draft["metadata"]["location"].as_str().unwrap())
        .parse()
        .unwrap();
    let authenticator =
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let issuer = FileGrantIssuer::new(authenticator.namespace_token_key(), 900_000).unwrap();
    let (repository, _, chunks) = storage::connect(
        seeds.to_vec(),
        ChunkReadPolicy::default(),
        SmallWritePolicy {
            mirror_copies: 2,
            conversion_enabled: false,
            ..SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
        },
        2,
        2,
    )
    .await
    .unwrap();
    let context = repository.status().await.unwrap().0.context;
    let credentials = issuer
        .issue(FileGrant {
            storage_namespace: None,
            context,
            table: table.table,
            principal: [7; 32],
            nonce: OperationId::random(),
            issued_ms: now_ms() - 1_000,
            expires_ms: now_ms() + 600_000,
            operations: FileOperations::new(&[FileOperation::Get, FileOperation::Put, FileOperation::Head])
                .unwrap(),
            max_request_bytes: 8 * 1024 * 1024,
            max_file_bytes: 8 * 1024 * 1024,
        })
        .unwrap();
    let file_client = signed::TestFileClient {
        client,
        credentials,
        address: iceberg_addr,
    };
    for (name, bytes) in [
        ("small-a", vec![0x41; 800 * 1024]),
        ("small-b", vec![0x43; 200 * 1024]),
        ("large", vec![0x42; 4 * 1024 * 1024]),
    ] {
        let path = format!(
            "/{}/{}",
            table.bucket(),
            table.file(&format!("data/{name}.bin")).unwrap().object_key()
        );
        let put = file_client.send(Method::PUT, &path, "", &bytes, true).await;
        assert_eq!(put.status(), 200, "{}", put.text().await.unwrap());
        let get = file_client.send(Method::GET, &path, "", b"", false).await;
        assert_eq!(get.status(), 200);
        assert_eq!(get.bytes().await.unwrap().as_ref(), bytes);
    }
    chunks.shutdown_small_writes().await.unwrap();
}

async fn assert_chunk_layouts(chunkdb_ip: &str) {
    let transport = ChunkdbRpcTransport::new();
    let mut saw = [false; 4];
    {
        let listed = transport
            .send_list_chunks(
                &format!("{chunkdb_ip}:7302"),
                &ListChunksRequest {
                    max_keys: 1_024,
                    ..ListChunksRequest::default()
                },
            )
            .await
            .unwrap();
        for chunk in listed.chunks {
            let is_s3 = chunk.chunk_type == ChunkType::S3 as i32;
            let is_iceberg = chunk.chunk_type == ChunkType::IcebergTable as i32;
            if !is_s3 && !is_iceberg {
                continue;
            }
            assert_eq!(
                chunk.id.unwrap().high >> 56,
                u64::try_from(chunk.chunk_type).unwrap()
            );
            for strip in chunk.strips {
                match strip.strip.unwrap() {
                    Strip::MirrorStrip(mirror) => {
                        assert_eq!(mirror.segments.len(), 2);
                        saw[if is_s3 { 0 } else { 2 }] = true;
                    }
                    Strip::EcStrip(ec) => {
                        assert_eq!((ec.data_num, ec.code_num), if is_s3 { (2, 1) } else { (4, 2) });
                        saw[if is_s3 { 1 } else { 3 }] = true;
                    }
                }
            }
        }
    }
    assert!(
        saw.into_iter().all(|seen| seen),
        "both protocols must write small mirror and large EC strips"
    );
}
