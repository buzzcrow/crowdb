#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;

use fixture::TestTableHttp;

#[tokio::test]
async fn inspection_uses_authoritative_table_identity_without_requiring_a_location_slash() {
    use crowdb_access_iceberg::file::{ContentFormat, FileContent, FileKind, FileRecord, FileRepository};
    use sha2::{Digest, Sha256};
    let fixture = TestTableHttp::new().await;
    let (head, _) = fixture.install_with_location("events", false).await;
    let bytes = b"invalid avro header";
    FileRepository::new(fixture.store.clone())
        .publish(
            fixture.context,
            &FileRecord {
                file: crowdb_access_iceberg::key::FileId::random(),
                location: head.metadata_location.table().file("metadata/20.avro").unwrap(),
                kind: FileKind::ManifestList,
                format: ContentFormat::Avro,
                length: bytes.len() as u64,
                digest: Sha256::digest(bytes).into(),
                hint: None,
                content: FileContent::select_inline(FileKind::ManifestList, bytes).unwrap(),
            },
        )
        .await
        .unwrap();
    let response = reqwest::Client::new()
        .get(format!(
            "{}/v1/namespaces/analytics/tables/events/inspect",
            fixture.endpoint()
        ))
        .bearer_auth("r".repeat(32))
        .query(&[
            ("metadata", head.metadata_location.to_string()),
            ("snapshot", "20".into()),
        ])
        .send()
        .await
        .unwrap();
    // The reference belongs to this table and reaches the Avro parser.
    assert_eq!(response.status(), 422, "{}", response.text().await.unwrap());
    fixture.finish().await;
}

#[tokio::test]
async fn inspection_requires_authentication_generation_and_reachable_snapshot() {
    let fixture = TestTableHttp::new().await;
    let (head, _) = fixture.install("events").await;
    let url = format!(
        "{}/v1/namespaces/analytics/tables/events/inspect",
        fixture.endpoint()
    );
    let client = reqwest::Client::new();
    assert_eq!(client.get(&url).send().await.unwrap().status(), 401);
    for (metadata, snapshot, offset, expected) in [
        ("stale".to_owned(), "20", "0", 409),
        (head.metadata_location.to_string(), "999999999999999999", "0", 404),
        (head.metadata_location.to_string(), "20", "100000", 400),
        (head.metadata_location.to_string(), "20", "0", 404),
    ] {
        let response = client
            .get(&url)
            .bearer_auth("r".repeat(32))
            .query(&[
                ("metadata", metadata.as_str()),
                ("snapshot", snapshot),
                ("offset", offset),
            ])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{}", response.text().await.unwrap());
    }
    let response = client
        .get(&url)
        .bearer_auth("r".repeat(32))
        .query(&[
            ("metadata", head.metadata_location.to_string()),
            ("snapshot", "20".into()),
            ("file", "file:///etc/passwd".into()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    fixture.finish().await;
}
