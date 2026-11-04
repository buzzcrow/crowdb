// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/bucket_client.rs"]
mod bucket_client;
#[path = "common/bucket_listing_collision.rs"]
mod fixture;

use crowdb_access_s3::{
    bucket::{self, BucketError, BucketListRequest, BucketListSigner},
    metadata::{BucketId, MetadataKey, ObjectRecord, TenantId},
};
use fixture::{multipart_rows, paginated, session, store};

#[tokio::test]
async fn simple_listing_skips_valid_multipart_records_sharing_the_bucket_interval() {
    let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
    let mut failures = Vec::new();
    for (kind, key, value) in multipart_rows(&tenant) {
        assert!(
            key >= MetadataKey::bucket_name_prefix(&tenant) && key < MetadataKey::bucket_name_end(&tenant)
        );
        let store = store(&tenant, key, value).await;
        match bucket::list_buckets(&store, &tenant, 100).await {
            Ok(buckets) => assert_eq!(
                buckets.iter().map(|b| b.name.as_slice()).collect::<Vec<_>>(),
                vec![b"expected"]
            ),
            Err(error) => failures.push(format!("{kind}: {error:?}")),
        }
    }
    assert!(
        failures.is_empty(),
        "valid multipart resource poisoned simple bucket listing: {failures:?}"
    );
}

#[tokio::test]
async fn paginated_listing_skips_valid_multipart_records_sharing_the_bucket_interval() {
    let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
    let mut failures = Vec::new();
    for (kind, key, value) in multipart_rows(&tenant) {
        let store = store(&tenant, key, value).await;
        match paginated(&store, &tenant).await {
            Ok(names) => assert_eq!(names, vec![b"expected".to_vec()]),
            Err(error) => failures.push(format!("{kind}: {error:?}")),
        }
    }
    assert!(
        failures.is_empty(),
        "valid multipart resource poisoned paginated bucket listing: {failures:?}"
    );
}

#[tokio::test]
async fn malformed_bucket_rows_are_not_hidden_as_multipart_resources() {
    let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
    for value in [b"corrupt-bucket".to_vec(), session().encode().unwrap()] {
        let key = MetadataKey::bucket_name(&tenant, b"bad-bucket").unwrap();
        let store = store(&tenant, key, value).await;
        assert!(matches!(
            bucket::list_buckets(&store, &tenant, 100).await,
            Err(BucketError::Record(_))
        ));
        assert!(matches!(
            paginated(&store, &tenant).await,
            Err(BucketError::Record(_))
        ));
    }
}

#[tokio::test]
async fn legacy_objects_are_skipped_only_under_their_exact_key() {
    let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
    let record = ObjectRecord {
        bucket_id: session().bucket_id,
        key: b"legacy-object".to_vec(),
        logical_length: 1,
        checksum: vec![9; 16],
        etag: "etag".into(),
        created_at_ms: 100,
        modified_at_ms: 100,
        content_type: "text/plain".into(),
        attributes: Vec::new(),
        data_reference: vec![7],
        data_length: 1,
    };
    let bytes = record.encode().unwrap();
    let good = store(
        &tenant,
        MetadataKey::object(&tenant, record.bucket_id, &record.key).unwrap(),
        bytes.clone(),
    )
    .await;
    assert_eq!(bucket::list_buckets(&good, &tenant, 100).await.unwrap().len(), 1);
    assert_eq!(
        paginated(&good, &tenant).await.unwrap(),
        vec![b"expected".to_vec()]
    );
    let bad = store(
        &tenant,
        MetadataKey::bucket_name(&tenant, b"bad-object").unwrap(),
        bytes,
    )
    .await;
    assert!(matches!(
        bucket::list_buckets(&bad, &tenant, 100).await,
        Err(BucketError::Record(_))
    ));
    assert!(matches!(
        paginated(&bad, &tenant).await,
        Err(BucketError::Record(_))
    ));
}

#[tokio::test]
async fn foreign_only_page_keeps_its_cursor_and_returns_later_buckets_once() {
    let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
    let (_, key, value) = multipart_rows(&tenant).remove(0);
    let store = store(&tenant, key, value).await;
    let signer = BucketListSigner::new(b"secret".to_vec()).unwrap();
    let options = |continuation| BucketListRequest {
        prefix: b"",
        continuation,
        max_buckets: 1,
        max_scan_items: 1,
        max_scan_bytes: 65536,
        now: 150,
        ttl: 30,
    };
    let first = signer.list(&store, &tenant, &options(None)).await.unwrap();
    assert!(first.buckets.is_empty());
    assert!(first.continuation.is_some());
    let second = signer
        .list(&store, &tenant, &options(first.continuation.as_deref()))
        .await
        .unwrap();
    assert_eq!(
        second
            .buckets
            .iter()
            .map(|b| b.name.as_slice())
            .collect::<Vec<_>>(),
        vec![b"expected"]
    );
    assert!(second.continuation.is_none());
}

#[tokio::test]
async fn multipart_rows_with_foreign_or_corrupt_identities_remain_errors() {
    let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
    let other = TenantId::new(b"other-tenant".to_vec()).unwrap();
    let mut bad = Vec::new();
    for (kind, key, value) in multipart_rows(&tenant) {
        let mut wrong_value = value.clone();
        wrong_value[0] ^= 0x40;
        bad.push((format!("{kind} unknown format"), key.clone(), wrong_value));
        let mut wrong_key = key.clone();
        let last = wrong_key.len() - 1;
        wrong_key[last] ^= 1;
        bad.push((format!("{kind} foreign key"), wrong_key, value));
    }
    let original = session();
    let mut foreign = original.clone();
    foreign.bucket_id = BucketId::new([3; 16]);
    bad.push((
        "foreign bucket session".into(),
        MetadataKey::multipart_session(&tenant, original.bucket_id, &original.upload_id),
        foreign.encode().unwrap(),
    ));
    let (_, index_key, index_value) = multipart_rows(&tenant).pop().unwrap();
    let foreign_pointer = MetadataKey::multipart_session(&other, original.bucket_id, &original.upload_id);
    bad.push(("foreign tenant index".into(), index_key, foreign_pointer));
    let mut malformed_index = MetadataKey::multipart_session_prefix(&tenant, original.bucket_id);
    malformed_index.extend_from_slice(b"object\0\x01");
    malformed_index.extend_from_slice(&original.upload_id);
    bad.push((
        "invalid ordered index component".into(),
        malformed_index,
        index_value,
    ));
    for (kind, key, value) in bad {
        let store = store(&tenant, key, value).await;
        assert!(
            matches!(
                bucket::list_buckets(&store, &tenant, 100).await,
                Err(BucketError::Record(_))
            ),
            "simple listing accepted {kind}"
        );
        assert!(
            matches!(paginated(&store, &tenant).await, Err(BucketError::Record(_))),
            "paginated listing accepted {kind}"
        );
    }
}
