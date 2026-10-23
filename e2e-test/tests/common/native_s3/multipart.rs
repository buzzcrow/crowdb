// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{context, payload, TestNativeS3};
use crowdb_sdk_native::s3::{
    CompletionPart, CopyMetadata, GetOptions, ListUploads, MultipartUpload, PartMetadata, ReadPreconditions,
    UserMetadata,
};

pub async fn exercise(fixture: &TestNativeS3) {
    let upload = fixture
        .client
        .begin_multipart(
            context(),
            fixture.bucket,
            b"multipart",
            CopyMetadata {
                content_type: "application/native".into(),
                user_metadata: UserMetadata::default(),
            },
        )
        .await
        .unwrap();
    let first = write_part(fixture, &upload, 1, 5 << 20).await;
    let replaced = write_part(fixture, &upload, 2, 7).await;
    let source = fixture
        .client
        .resolve_object(context(), fixture.bucket, b"framed-small")
        .await
        .unwrap();
    let tail = fixture
        .client
        .copy_part_resolved(
            context(),
            &upload,
            2,
            &source,
            None,
            &ReadPreconditions::default(),
        )
        .await
        .unwrap();
    assert!(tail.generation > replaced.generation);
    let listed = fixture.client.list_parts(context(), &upload, 0, 1).await.unwrap();
    assert_eq!(listed.parts, vec![first.clone()]);
    assert_eq!(listed.next_part_number_marker, Some(1));
    assert_eq!(
        fixture
            .client
            .list_parts(context(), &upload, 1, 1)
            .await
            .unwrap()
            .parts,
        vec![tail.clone()]
    );
    let selection = [first, tail].map(|part| CompletionPart {
        number: part.number,
        etag: part.etag,
    });
    let completed = fixture
        .client
        .complete_multipart(context(), &upload, &selection)
        .await
        .unwrap();
    assert_eq!(completed.logical_length, (5 << 20) + 513);
    assert_eq!(
        fixture
            .client
            .complete_multipart(context(), &upload, &selection)
            .await
            .unwrap(),
        completed
    );
    let abort = fixture
        .client
        .begin_multipart(
            context(),
            fixture.bucket,
            b"aborted",
            CopyMetadata {
                content_type: "application/native".into(),
                user_metadata: UserMetadata::default(),
            },
        )
        .await
        .unwrap();
    fixture.client.abort_multipart(context(), &abort).await.unwrap();
    assert!(fixture
        .client
        .list_multipart_uploads(context(), fixture.bucket, &ListUploads::default())
        .await
        .unwrap()
        .uploads
        .is_empty());
}

async fn write_part(
    fixture: &TestNativeS3,
    upload: &MultipartUpload,
    number: u16,
    length: usize,
) -> PartMetadata {
    let mut writer = fixture
        .client
        .begin_upload_part(context(), upload, number, length as u64, None)
        .await
        .unwrap();
    let lease = fixture
        .runtime
        .resources()
        .retain_vec(payload(length), 1, 1, 0, 0)
        .unwrap();
    for start in (0..length).step_by(1 << 20) {
        writer
            .write(lease.slice(start..length.min(start + (1 << 20))).unwrap())
            .await
            .unwrap();
    }
    writer.finish().await.unwrap()
}

pub async fn verify(fixture: &TestNativeS3) {
    let (metadata, data) = fixture.read(b"multipart", GetOptions::default()).await;
    assert_eq!(metadata.content_type, "application/native");
    let mut expected = payload(5 << 20);
    expected.extend_from_slice(&payload(513));
    assert_eq!(data, expected);
}
