// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::setup::{path, setup_with_bounds_and_file_limit};
use crowdb_access_iceberg::catalog::ClearBounds;
use crowdb_access_iceberg::file::FileRepository;
use md5::{Digest, Md5};
use reqwest::Method;
use std::fmt::Write as _;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release Iceberg FileIO stack"]
async fn multipart_100_mib_survives_restart_and_complete_replay() {
    const PART_BYTES: usize = 5 * 1024 * 1024;
    const PART_COUNT: usize = 20;
    let (stack, process, mut client, table) = setup_with_bounds_and_file_limit(
        ClearBounds {
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
        128 * 1024 * 1024,
        128 * 1024 * 1024,
    )
    .await;
    let object = path(table, "data/large-multipart.parquet");
    let created = client.send(Method::POST, &object, "uploads=", b"", false).await;
    assert_eq!(created.status(), 200, "{}", created.text().await.unwrap());
    let created = created.text().await.unwrap();
    let upload = created
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let mut manifest = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    let mut composite = Md5::new();
    for first_number in (1..=PART_COUNT).step_by(2) {
        let second_number = first_number + 1;
        let first_bytes = vec![u8::try_from(first_number).unwrap(); PART_BYTES];
        let second_bytes = vec![u8::try_from(second_number).unwrap(); PART_BYTES];
        let first_query = format!("partNumber={first_number}&uploadId={upload}");
        let second_query = format!("partNumber={second_number}&uploadId={upload}");
        let (first_response, second_response) = tokio::join!(
            client.send(Method::PUT, &object, &first_query, &first_bytes, true),
            client.send(Method::PUT, &object, &second_query, &second_bytes, true),
        );
        for (number, bytes, response) in [
            (first_number, first_bytes, first_response),
            (second_number, second_bytes, second_response),
        ] {
            assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
            let etag = response.headers()["etag"].to_str().unwrap();
            composite.update(Md5::digest(&bytes));
            write!(
                manifest,
                "<Part><ETag>{etag}</ETag><PartNumber>{number}</PartNumber></Part>"
            )
            .unwrap();
        }
    }
    manifest.push_str("</CompleteMultipartUpload>");
    let mut expected_etag = String::new();
    for byte in composite.finalize() {
        write!(expected_etag, "{byte:02x}").unwrap();
    }
    expected_etag.push_str("-20");
    let query = format!("uploadId={upload}");
    let completed = client
        .send(Method::POST, &object, &query, manifest.as_bytes(), false)
        .await;
    assert_eq!(completed.status(), 200);
    let completed = completed.text().await.unwrap();
    assert!(
        completed.contains("</CompleteMultipartUploadResult>"),
        "{completed}"
    );
    let repository = FileRepository::new(stack.store().await);
    let record = repository
        .load(
            client.credentials.grant().context,
            &table.file("data/large-multipart.parquet").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.length, (PART_BYTES * PART_COUNT) as u64);
    assert_eq!(record.content.etag(), Some(expected_etag.as_str()));
    assert_eq!(
        record.content.locations(record.length).unwrap().unwrap().len(),
        PART_COUNT
    );

    process.stop().await;
    stack.restart_catalog().await;
    let restarted = super::super::frontend::TestFrontend::start(13).await;
    client.address = restarted.address();
    let replay = client
        .send(Method::POST, &object, &query, manifest.as_bytes(), false)
        .await;
    assert_eq!(replay.status(), 200, "{}", replay.text().await.unwrap());
    let response = client.send(Method::GET, &object, "", b"", false).await;
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    let bytes = response.bytes().await.unwrap();
    assert_eq!(bytes.len(), PART_BYTES * PART_COUNT);
    for (index, part) in bytes.chunks_exact(PART_BYTES).enumerate() {
        assert!(part.iter().all(|byte| *byte == u8::try_from(index + 1).unwrap()));
    }
}
