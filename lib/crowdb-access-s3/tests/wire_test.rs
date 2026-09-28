// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::metadata::{
    BucketId, BucketNameRecord, MultipartPartPage, MultipartPartRecord, ObjectRecord, TenantId,
};
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

#[test]
fn multipart_xml_escapes_names_and_reports_selected_part_metadata() {
    let id = [0xab; 16];
    let created = wire::create_multipart_upload(b"b&", b"k<", &id);
    assert!(created.contains("<Bucket>b&amp;</Bucket>"));
    assert!(created.contains("<Key>k&lt;</Key>"));
    assert!(created.contains(&format!("<UploadId>{}</UploadId>", "ab".repeat(16))));

    let page = MultipartPartPage {
        parts: vec![MultipartPartRecord {
            bucket_id: BucketId::new([1; 16]),
            upload_id: id,
            number: 4,
            revision: 1,
            modified_ms: 1_000,
            length: 8,
            raw_md5: [9; 16],
            locations: Vec::new(),
        }],
        next_part_number_marker: Some(4),
    };
    let listed = wire::list_multipart_parts(b"b&", b"k<", &id, 2, 1, &page);
    assert!(listed.contains("<PartNumberMarker>2</PartNumberMarker>"));
    assert!(listed.contains("<NextPartNumberMarker>4</NextPartNumberMarker>"));
    assert!(listed.contains("<ETag>&quot;09090909090909090909090909090909&quot;</ETag>"));
    assert!(listed.contains("<Size>8</Size>"));
    let completed = wire::complete_multipart_upload("http://host/b&/k<", b"b&", b"k<", "abc-1");
    assert!(completed.contains("<Location>http://host/b&amp;/k&lt;</Location>"));
    assert!(completed.contains("<ETag>&quot;abc-1&quot;</ETag>"));
}
