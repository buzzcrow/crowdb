// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::metadata::{BucketId, BucketNameRecord, ObjectRecord, TenantId};
use crowdb_access_s3::object::ListObjectsV2Page;
use crowdb_access_s3::wire;

#[test]
fn list_buckets_escapes_names_and_uses_the_s3_namespace() {
    let xml = wire::list_buckets(
        b"tenant",
        &[BucketNameRecord {
            tenant: TenantId::new(b"tenant".to_vec()).unwrap(),
            name: b"a&b".to_vec(),
            bucket_id: BucketId::new([1; 16]),
            tombstone: false,
        }],
    );
    assert!(xml.contains("xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\""));
    assert!(xml.contains("<Name>a&amp;b</Name>"));
}

#[test]
fn list_objects_serializes_stable_etag_time_and_continuation() {
    let page = ListObjectsV2Page {
        objects: vec![ObjectRecord {
            bucket_id: BucketId::new([2; 16]),
            key: b"prefix/a<b".to_vec(),
            logical_length: 7,
            checksum: vec![1; 16],
            etag: "etag".into(),
            created_at_ms: 1_000,
            modified_at_ms: 1_000,
            content_type: "application/octet-stream".into(),
            attributes: Vec::new(),
            data_reference: vec![1],
            data_length: 7,
        }],
        common_prefixes: vec![b"prefix/sub/".to_vec()],
        next_continuation_token: Some("opaque".into()),
    };
    let xml = wire::list_objects(b"bucket", b"prefix/", Some(b"/"), 2, &page);
    assert!(xml.contains("<Key>prefix/a&lt;b</Key>"));
    assert!(xml.contains("<ETag>&quot;etag&quot;</ETag>"));
    assert!(xml.contains("<LastModified>1970-01-01T00:00:01Z</LastModified>"));
    assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
    assert!(xml.contains("<NextContinuationToken>opaque</NextContinuationToken>"));
}
