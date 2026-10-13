// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::signed_file::TestFileClient;
use super::setup::{path, setup};
use crowdb_access_iceberg::file::{FileKind, FileRepository, TableLocation};
use md5::{Digest, Md5};
use reqwest::{Client, Method};
use std::{fmt::Write as _, time::Instant};
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release Iceberg FileIO stack"]
async fn signed_standard_put_get_and_multipart_publish_unbound_files() {
    let (stack, _frontend, client, table) = setup().await;
    let repository = FileRepository::new(stack.store().await);
    unbound_data(&client, table, &repository).await;
    medium_ranges(&client, table, &repository).await;
    metadata_multipart(&client, table, &repository).await;
}

async fn unbound_data(client: &TestFileClient, table: TableLocation, repository: &FileRepository) {
    let parquet = b"PAR1datafoot\x04\0\0\0PAR1";
    let object = path(table, "data/a.parquet");
    let put = client.send(Method::PUT, &object, "", parquet, true).await;
    assert_eq!(put.status(), 200, "{}", put.text().await.unwrap());
    let head = client.send(Method::HEAD, &object, "", b"", false).await;
    assert_eq!(head.status(), 200);
    assert_eq!(head.headers()["content-length"], parquet.len().to_string());
    let get = client.send(Method::GET, &object, "", b"", false).await;
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().as_ref(), parquet);
    let range = client
        .send_range(Method::GET, &object, "", b"", false, Some("bytes=4-7"))
        .await;
    assert_eq!(range.status(), 206);
    assert_eq!(range.headers()["content-range"], "bytes 4-7/20");
    assert_eq!(range.bytes().await.unwrap().as_ref(), b"data");
    let record = repository
        .load(
            client.credentials.grant().context,
            &table.file("data/a.parquet").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.kind, FileKind::Unbound);
    assert!(record.bind_kind(FileKind::EqualityDelete).is_ok());
    let conflict = client
        .send(Method::PUT, &object, "", b"PAR1difffoot\x04\0\0\0PAR1", true)
        .await;
    assert_eq!(conflict.status(), 409);
}

async fn medium_ranges(client: &TestFileClient, table: TableLocation, repository: &FileRepository) {
    let medium = path(table, "data/medium.parquet");
    let medium_bytes = (0..1_200_000)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect::<Vec<_>>();
    let before = file_request_counts(client).await;
    let started = Instant::now();
    let response = client.send(Method::PUT, &medium, "", &medium_bytes, true).await;
    let elapsed = started.elapsed();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    let after = file_request_counts(client).await;
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "1.2 MiB PUT took {elapsed:?}"
    );
    assert!(
        after.0 - before.0 <= 15 && after.1 - before.1 <= 2,
        "1.2 MiB PUT used {} gets and {} CAS operations",
        after.0 - before.0,
        after.1 - before.1
    );
    let stored = repository
        .load(
            client.credentials.grant().context,
            &table.file("data/medium.parquet").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.content.locations(stored.length).unwrap().unwrap().len(), 1);
    let range = client
        .send_range(
            Method::GET,
            &medium,
            "",
            b"",
            false,
            Some("bytes=1048550-1048600"),
        )
        .await;
    assert_eq!(range.status(), 206);
    assert_eq!(
        range.bytes().await.unwrap().as_ref(),
        &medium_bytes[1_048_550..1_048_601]
    );
    let metrics: serde_json::Value = Client::new()
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(metrics["chunk_read"]["stream_windows"].as_u64().unwrap() > 0);
    assert!(metrics["chunk_small_write"]["completed"].as_u64().unwrap() > 0);
}

async fn metadata_multipart(client: &TestFileClient, table: TableLocation, repository: &FileRepository) {
    let metadata = path(table, "metadata/b.json");
    let create = client.send(Method::POST, &metadata, "uploads=", b"", false).await;
    assert_eq!(create.status(), 200);
    let xml = create.text().await.unwrap();
    let upload = xml
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let mut document = b"{\"answer\":\"".to_vec();
    document.resize(document.len() + 5 * 1024 * 1024, b'x');
    document.extend_from_slice(b"\"}");
    let first = &document[..5 * 1024 * 1024];
    let second = &document[5 * 1024 * 1024..];
    let mut etags = Vec::new();
    for (number, bytes) in [(1, first), (2, second)] {
        let query = format!("partNumber={number}&uploadId={upload}");
        let response = client.send(Method::PUT, &metadata, &query, bytes, true).await;
        assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
        etags.push(response.headers()["etag"].to_str().unwrap().to_owned());
    }
    let listed = client
        .send(Method::GET, &metadata, &format!("uploadId={upload}"), b"", false)
        .await;
    assert_eq!(listed.status(), 200);
    assert!(listed
        .text()
        .await
        .unwrap()
        .contains("<PartNumber>2</PartNumber>"));
    let complete_xml = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Part><ETag>{}</ETag><PartNumber>1</PartNumber></Part><Part><ETag>{}</ETag><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>", etags[0], etags[1]);
    let complete = client
        .send(
            Method::POST,
            &metadata,
            &format!("uploadId={upload}"),
            complete_xml.as_bytes(),
            false,
        )
        .await;
    assert_eq!(complete.status(), 200, "{}", complete.text().await.unwrap());
    assert!(complete
        .text()
        .await
        .unwrap()
        .ends_with("</CompleteMultipartUploadResult>"));
    let mut composite = Md5::new();
    composite.update(Md5::digest(first));
    composite.update(Md5::digest(second));
    let mut expected_etag = String::with_capacity(34);
    for byte in composite.finalize() {
        write!(&mut expected_etag, "{byte:02x}").unwrap();
    }
    expected_etag.push_str("-2");
    let published = repository
        .load(
            client.credentials.grant().context,
            &table.file("metadata/b.json").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(published.content.etag(), Some(expected_etag.as_str()));
    assert_eq!(
        published
            .content
            .locations(published.length)
            .unwrap()
            .unwrap()
            .len(),
        2
    );
    let replay = client
        .send(
            Method::POST,
            &metadata,
            &format!("uploadId={upload}"),
            complete_xml.as_bytes(),
            false,
        )
        .await;
    assert_eq!(replay.status(), 200);
    assert!(replay
        .text()
        .await
        .unwrap()
        .ends_with("</CompleteMultipartUploadResult>"));
    let get = client.send(Method::GET, &metadata, "", b"", false).await;
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().as_ref(), document);
}

async fn file_request_counts(client: &TestFileClient) -> (u64, u64) {
    let response: serde_json::Value = client
        .client
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let catalog = &response["routes"][6][0]["catalog"];
    (
        catalog["get"].as_u64().unwrap(),
        catalog["compare_exchange"].as_u64().unwrap(),
    )
}
