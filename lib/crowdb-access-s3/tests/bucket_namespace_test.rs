// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::bucket::{BucketIdGenerator, RandomBucketIdGenerator};
use crowdb_access_s3::metadata::{BucketDeleteOutcome, BucketId, BucketNamespace, ObjectRecord, TenantId};

#[test]
fn production_bucket_ids_are_uuid_v4_values() {
    let id = RandomBucketIdGenerator.next_id();
    let uuid = uuid::Uuid::from_bytes(*id.as_bytes());

    assert_eq!(uuid.get_version(), Some(uuid::Version::Random));
}

fn generation(bucket_id: BucketId, generation: u64, reference: &[u8]) -> ObjectRecord {
    ObjectRecord {
        bucket_id,
        key: b"object".to_vec(),
        logical_length: 3,
        checksum: b"sum".to_vec(),
        etag: format!("etag-{generation}"),
        created_at_ms: generation,
        modified_at_ms: generation,
        content_type: "application/octet-stream".into(),
        attributes: Vec::new(),
        data_reference: reference.to_vec(),
        data_length: 3,
    }
}

#[test]
fn late_old_bucket_publication_stays_unreachable_after_delete_and_recreate() {
    let tenant = TenantId::new(b"tenant".to_vec()).expect("valid tenant");
    let mut namespace = BucketNamespace::default();
    let old = namespace
        .create(tenant.clone(), b"bucket".to_vec())
        .expect("create bucket");

    assert_eq!(namespace.delete(&tenant, b"bucket"), BucketDeleteOutcome::Deleted);
    namespace.publish(old, b"late".to_vec());
    let new = namespace
        .create(tenant.clone(), b"bucket".to_vec())
        .expect("recreate bucket");

    assert_ne!(old, new);
    assert_eq!(namespace.head(&tenant, b"bucket"), Some(new));
}

#[test]
fn bucket_create_is_idempotent_and_delete_rejects_visible_objects() {
    let tenant = TenantId::new(b"tenant".to_vec()).expect("valid tenant");
    let mut namespace = BucketNamespace::default();
    let bucket = namespace
        .create(tenant.clone(), b"bucket".to_vec())
        .expect("create bucket");
    assert_eq!(
        namespace
            .create(tenant.clone(), b"bucket".to_vec())
            .expect("idempotent retry"),
        bucket
    );

    namespace.publish(bucket, b"visible".to_vec());
    assert_eq!(
        namespace.delete(&tenant, b"bucket"),
        BucketDeleteOutcome::NotEmpty
    );
    assert_eq!(namespace.head(&tenant, b"bucket"), Some(bucket));
}

#[test]
fn overwrite_keeps_the_generation_selected_by_an_existing_reader() {
    let tenant = TenantId::new(b"tenant".to_vec()).expect("valid tenant");
    let mut namespace = BucketNamespace::default();
    let bucket = namespace
        .create(tenant, b"bucket".to_vec())
        .expect("create bucket");
    let old = generation(bucket, 1, b"old");
    namespace.publish_object(&old).expect("publish old generation");
    let reader_generation = namespace
        .read_object(bucket, b"object")
        .expect("old generation is visible")
        .clone();

    let new = generation(bucket, 2, b"new");
    namespace.publish_object(&new).expect("publish new generation");

    assert_eq!(reader_generation.data_reference, b"old");
    assert_eq!(
        namespace
            .read_object(bucket, b"object")
            .expect("new generation is visible"),
        &new
    );
    assert_eq!(old.data_reference, b"old");
}
