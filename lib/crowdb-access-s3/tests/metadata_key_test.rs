// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::metadata::{BucketId, MetadataKey, TenantId};

#[test]
fn object_keys_are_binary_safe_and_ordered_within_a_bucket() {
    let tenant = TenantId::new(b"tenant\0a".to_vec()).expect("tenant is valid");
    let bucket = BucketId::new([7; 16]);
    let first = MetadataKey::object(&tenant, bucket, b"a\0").expect("valid key");
    let second = MetadataKey::object(&tenant, bucket, b"a/").expect("valid key");
    let start = MetadataKey::object_prefix(&tenant, bucket);
    let end = MetadataKey::object_end(&tenant, bucket);

    assert!(start < first && first < second && second < end);
}

#[test]
fn object_prefix_interval_is_binary_safe_and_resume_is_strict() {
    let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
    let bucket = BucketId::new([3; 16]);
    let lower = MetadataKey::object_key_prefix(&tenant, bucket, b"a\0").unwrap();
    let upper = MetadataKey::object_key_prefix_end(&tenant, bucket, b"a\0").unwrap();
    let exact = MetadataKey::object(&tenant, bucket, b"a\0").unwrap();
    let child = MetadataKey::object(&tenant, bucket, b"a\0z").unwrap();
    let outside = MetadataKey::object(&tenant, bucket, b"a\x01").unwrap();
    assert!(lower <= exact && exact < upper);
    assert!(lower <= child && child < upper);
    assert!(outside >= upper);
    let resume = MetadataKey::object_after(&tenant, bucket, b"a\0").unwrap();
    assert!(exact < resume && resume < child);
}

#[test]
fn bucket_prefix_does_not_include_another_bucket() {
    let tenant = TenantId::new(b"tenant".to_vec()).expect("tenant is valid");
    let first_bucket = BucketId::new([1; 16]);
    let second_bucket = BucketId::new([2; 16]);
    let start = MetadataKey::object_prefix(&tenant, first_bucket);
    let end = MetadataKey::object_end(&tenant, first_bucket);
    let second = MetadataKey::object(&tenant, second_bucket, b"key").expect("valid key");

    assert!(second < start || second >= end);
}

#[test]
fn object_key_rejects_an_empty_name() {
    let tenant = TenantId::new(b"tenant".to_vec()).expect("valid tenant");
    let bucket = BucketId::new([8; 16]);

    assert!(MetadataKey::object(&tenant, bucket, &[]).is_err());
}

#[test]
fn maximum_binary_object_key_stays_within_its_bucket_interval() {
    let tenant = TenantId::new(b"tenant".to_vec()).expect("valid tenant");
    let bucket = BucketId::new([5; 16]);
    let key = vec![0xff; 1024];
    let encoded = MetadataKey::object(&tenant, bucket, &key).expect("maximum key is valid");

    assert!(MetadataKey::object_prefix(&tenant, bucket) < encoded);
    assert!(encoded < MetadataKey::object_end(&tenant, bucket));
}

#[test]
fn multipart_keys_keep_uploads_and_parts_in_separate_bounded_intervals() {
    let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
    let bucket = BucketId::new([5; 16]);
    let upload = [9; 16];
    let session = MetadataKey::multipart_session(&tenant, bucket, b"a\0", &upload).unwrap();
    let start = MetadataKey::multipart_session_prefix(&tenant, bucket);
    let end = MetadataKey::multipart_session_end(&tenant, bucket);
    assert!(start < session && session < end);
    let part_start = MetadataKey::multipart_part_prefix(&tenant, bucket, &upload);
    let part_end = MetadataKey::multipart_part_end(&tenant, bucket, &upload);
    let first = MetadataKey::multipart_part(&tenant, bucket, &upload, 1).unwrap();
    let last = MetadataKey::multipart_part(&tenant, bucket, &upload, 10_000).unwrap();
    assert!(part_start < first && first < last && last < part_end);
    assert!(MetadataKey::multipart_part(&tenant, bucket, &upload, 0).is_err());
    assert!(MetadataKey::multipart_part(&tenant, bucket, &upload, 10_001).is_err());
    assert!(MetadataKey::object_end(&tenant, bucket) <= start);
    assert!(end <= part_start);
}
