// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/write_fixture.rs"]
mod fixture;
#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/write_storage.rs"]
mod write_storage;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crowdb_access_s3::S3ErrorCode;
use crowdb_sdk_native::s3::{
    CompletionPart, CopyMetadata, ListUploads, MultipartUpload, PartMetadata, ReadPreconditions, S3Client,
    UserMetadata,
};
use crowdb_sdk_native::{ClientRuntime, Error};
use fixture::{context, fixture, options, read};

fn attributes() -> CopyMetadata {
    CopyMetadata {
        content_type: "application/native".into(),
        user_metadata: UserMetadata::default(),
    }
}

async fn part(
    runtime: &ClientRuntime,
    client: &S3Client,
    upload: &MultipartUpload,
    number: u16,
    data: Vec<u8>,
    id: u64,
) -> PartMetadata {
    let mut input = client
        .begin_upload_part(context(id), upload, number, data.len() as u64, None)
        .await
        .unwrap();
    let lease = runtime.resources().retain_vec(data, id, id, 0, 0).unwrap();
    for start in (0..lease.as_slice().len()).step_by(1 << 20) {
        input
            .write(
                lease
                    .slice(start..(start + (1 << 20)).min(lease.as_slice().len()))
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    input.finish().await.unwrap()
}

fn selection(part: &PartMetadata) -> CompletionPart {
    CompletionPart {
        number: part.number,
        etag: part.etag.clone(),
    }
}

#[tokio::test]
async fn multipart_streams_ordinary_and_framed_parts_and_replays_frozen_completion() {
    let (runtime, client, _) = fixture(None).await;
    let upload = client
        .begin_multipart(context(10), b"bucket", b"joined", attributes())
        .await
        .unwrap();
    let first = part(&runtime, &client, &upload, 1, vec![0x52; 5 << 20], 11).await;
    let mut input = client
        .begin_upload_part(context(12), &upload, 2, 513, None)
        .await
        .unwrap();
    let frozen = runtime
        .allocate_producer(65_536)
        .unwrap()
        .initialize_framed(513, 0x63, |_, _| {})
        .unwrap();
    input.write_framed(frozen).await.unwrap();
    let second = input.finish().await.unwrap();
    let first_page = client.list_parts(context(13), &upload, 0, 1).await.unwrap();
    assert_eq!(first_page.parts, vec![first.clone()]);
    assert_eq!(first_page.next_part_number_marker, Some(1));
    let second_page = client.list_parts(context(14), &upload, 1, 1).await.unwrap();
    assert_eq!(second_page.parts, vec![second.clone()]);
    assert_eq!(second_page.next_part_number_marker, None);
    assert_eq!(
        client
            .complete_multipart(context(15), &upload, &[selection(&second), selection(&first)])
            .await,
        Err(Error::S3(S3ErrorCode::InvalidPartOrder))
    );
    let selected = [selection(&first), selection(&second)];
    let completed = client
        .complete_multipart(context(16), &upload, &selected)
        .await
        .unwrap();
    assert_eq!(completed.logical_length, (5 << 20) + 513);
    assert!(completed.etag.ends_with("-2"));
    assert_eq!(
        client
            .complete_multipart(context(17), &upload, &selected)
            .await
            .unwrap(),
        completed
    );
    let data = read(&client, b"joined").await;
    assert!(data[..5 << 20].iter().all(|byte| *byte == 0x52));
    assert_eq!(&data[5 << 20..], &[0x63; 513]);
    let metadata = client
        .resolve_object(context(18), b"bucket", b"joined")
        .await
        .unwrap();
    assert_eq!(metadata.metadata().content_type, "application/native");
    assert!(matches!(
        client.begin_upload_part(context(19), &upload, 3, 7, None).await,
        Err(Error::S3(S3ErrorCode::NoSuchUpload))
    ));
    assert_eq!(runtime.resources().status().requests, 0);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn copied_parts_bind_the_source_generation_and_replacement_is_visible_in_listing() {
    let (runtime, client, _) = fixture(None).await;
    client
        .put_vec(
            context(10),
            b"bucket",
            b"source",
            b"original".to_vec(),
            options(8),
        )
        .await
        .unwrap();
    let source = client
        .resolve_object(context(11), b"bucket", b"source")
        .await
        .unwrap();
    let upload = client
        .begin_multipart(context(12), b"bucket", b"copied", attributes())
        .await
        .unwrap();
    let old = part(&runtime, &client, &upload, 1, b"old".to_vec(), 13).await;
    client
        .put_vec(
            context(14),
            b"bucket",
            b"source",
            b"replacement".to_vec(),
            options(11),
        )
        .await
        .unwrap();
    let copied = client
        .copy_part_resolved(
            context(15),
            &upload,
            1,
            &source,
            None,
            &ReadPreconditions::default(),
        )
        .await
        .unwrap();
    assert!(copied.generation > old.generation);
    assert_eq!(copied.length, 8);
    assert_eq!(
        client
            .list_parts(context(16), &upload, 0, 1000)
            .await
            .unwrap()
            .parts,
        vec![copied.clone()]
    );
    client
        .complete_multipart(context(17), &upload, &[selection(&copied)])
        .await
        .unwrap();
    assert_eq!(read(&client, b"copied").await, b"original");
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn upload_paging_abort_and_reattachment_preserve_exact_identity() {
    let (runtime, client, _) = fixture(None).await;
    let first = client
        .begin_multipart(context(10), b"bucket", b"a", attributes())
        .await
        .unwrap();
    let second = client
        .begin_multipart(context(11), b"bucket", b"b", attributes())
        .await
        .unwrap();
    let first_page = client
        .list_multipart_uploads(
            context(12),
            b"bucket",
            &ListUploads {
                limit: 1,
                ..ListUploads::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(first_page.uploads[0].upload_id(), first.upload_id());
    let (key, id) = first_page.next.unwrap();
    let second_page = client
        .list_multipart_uploads(
            context(13),
            b"bucket",
            &ListUploads {
                key_marker: Some(key),
                upload_marker: Some(id),
                limit: 1,
                ..ListUploads::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(second_page.uploads[0].upload_id(), second.upload_id());
    let attached = client
        .resolve_multipart(context(14), b"bucket", b"a", &first.upload_id())
        .await
        .unwrap();
    assert_eq!(attached.key(), b"a");
    client.abort_multipart(context(15), &attached).await.unwrap();
    assert!(matches!(
        client.begin_upload_part(context(16), &first, 1, 7, None).await,
        Err(Error::S3(S3ErrorCode::NoSuchUpload))
    ));
    let page = client
        .list_multipart_uploads(context(17), b"bucket", &ListUploads::default())
        .await
        .unwrap();
    assert_eq!(page.uploads.len(), 1);
    assert_eq!(page.uploads[0].upload_id(), second.upload_id());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn lost_multipart_mutations_return_uncertain_and_explicit_completion_retry_keeps_data() {
    let loss = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(metadata::TestMutationGate::default());
    let (runtime, client, backend) =
        fixture::fixture_with_metadata(metadata::client_with_loss_and_gate(loss.clone(), gate.clone()).await)
            .await;
    let reads = gate.reads.load(Ordering::Acquire);
    let mutations = gate.writes.load(Ordering::Acquire);
    loss.store(true, Ordering::Release);
    assert!(matches!(
        client
            .begin_multipart(context(10), b"bucket", b"uncertain-create", attributes())
            .await,
        Err(Error::UncertainMutation { request_id: 10, .. })
    ));
    // The first index write lost its reply. No confirmation read or second
    // creation mutation follows that unknown outcome.
    assert_eq!(gate.reads.load(Ordering::Acquire) - reads, 1);
    assert_eq!(gate.writes.load(Ordering::Acquire) - mutations, 1);
    assert_eq!(backend.writes.load(Ordering::Acquire), 0);
    assert_eq!(runtime.resources().status().requests, 0);
    loss.store(false, Ordering::Release);
    let upload = client
        .begin_multipart(context(11), b"bucket", b"confirmed", attributes())
        .await
        .unwrap();
    let saved = part(&runtime, &client, &upload, 1, b"retained".to_vec(), 12).await;
    let writes = backend.writes.load(Ordering::Acquire);
    loss.store(true, Ordering::Release);
    assert!(matches!(
        client
            .complete_multipart(context(13), &upload, &[selection(&saved)])
            .await,
        Err(Error::UncertainMutation { request_id: 13, .. })
    ));
    assert_eq!(backend.writes.load(Ordering::Acquire), writes);
    assert_eq!(runtime.resources().status().requests, 0);
    loss.store(false, Ordering::Release);
    let completed = client
        .complete_multipart(context(14), &upload, &[selection(&saved)])
        .await
        .unwrap();
    assert_eq!(
        client
            .complete_multipart(context(15), &upload, &[selection(&saved)])
            .await
            .unwrap(),
        completed
    );
    assert_eq!(backend.writes.load(Ordering::Acquire), writes);
    assert_eq!(read(&client, b"confirmed").await, b"retained");
    assert_eq!(runtime.resources().status().requests, 0);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn unconfirmed_publication_is_uncertain_and_retry_confirms_the_frozen_generation() {
    let fault = Arc::new(metadata::TestMutationBlackout::default());
    let (runtime, client, backend) =
        fixture::fixture_with_metadata(metadata::client_with_blackout(fault.clone()).await).await;
    let upload = client
        .begin_multipart(context(60), b"bucket", b"uncertain", attributes())
        .await
        .unwrap();
    let saved = part(&runtime, &client, &upload, 1, b"retained".to_vec(), 61).await;
    let bucket = client.head_bucket(context(62), b"bucket").await.unwrap();
    let tenant = crowdb_access_s3::metadata::TenantId::new(b"tenant".to_vec()).unwrap();
    let key = crowdb_access_s3::metadata::MetadataKey::object(&tenant, bucket, b"uncertain").unwrap();
    fault.key.store(Some(Arc::new(key)));
    let writes = backend.writes.load(Ordering::Acquire);
    assert_eq!(
        client
            .complete_multipart(context(63), &upload, &[selection(&saved)])
            .await,
        Err(Error::UncertainMutation {
            request_id: 63,
            reason: crowdb_sdk_native::error::MutationInterruption::Transport
        })
    );
    assert_eq!(backend.deletes.load(Ordering::Acquire), 0);
    assert_eq!(runtime.resources().status().requests, 0);
    fault.key.store(None);
    fault.blocked.store(false, Ordering::Release);
    client
        .complete_multipart(context(64), &upload, &[selection(&saved)])
        .await
        .unwrap();
    assert_eq!(backend.writes.load(Ordering::Acquire), writes);
    assert_eq!(read(&client, b"uncertain").await, b"retained");
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_completion_retry_overwrites_a_later_ordinary_put_without_reuploading_parts() {
    for lost_reply in [false, true] {
        let fault = Arc::new(metadata::TestMutationBlackout::default());
        let (runtime, client, backend) =
            fixture::fixture_with_metadata(metadata::client_with_blackout(fault.clone()).await).await;
        let upload = client
            .begin_multipart(context(70), b"bucket", b"overwrite", attributes())
            .await
            .unwrap();
        let saved = part(&runtime, &client, &upload, 1, b"original A".to_vec(), 71).await;
        if lost_reply {
            let bucket = client.head_bucket(context(72), b"bucket").await.unwrap();
            let tenant = crowdb_access_s3::metadata::TenantId::new(b"tenant".to_vec()).unwrap();
            let key = crowdb_access_s3::metadata::MetadataKey::object(&tenant, bucket, b"overwrite").unwrap();
            fault.key.store(Some(Arc::new(key)));
        }
        let completed = client
            .complete_multipart(context(73), &upload, &[selection(&saved)])
            .await;
        if lost_reply {
            assert!(matches!(
                completed,
                Err(Error::UncertainMutation { request_id: 73, .. })
            ));
        } else {
            assert_eq!(completed.unwrap().logical_length, 10);
        }
        fault.key.store(None);
        fault.blocked.store(false, Ordering::Release);
        for id in [74, 77] {
            client
                .put_vec(
                    context(id),
                    b"bucket",
                    b"overwrite",
                    b"later B".to_vec(),
                    options(7),
                )
                .await
                .unwrap();
            assert_eq!(read(&client, b"overwrite").await, b"later B");
            let writes = backend.writes.load(Ordering::Acquire);
            let restored = client
                .complete_multipart(context(id + 1), &upload, &[selection(&saved)])
                .await
                .unwrap();
            assert_eq!(restored.logical_length, 10);
            assert_eq!(read(&client, b"overwrite").await, b"original A");
            assert_eq!(backend.writes.load(Ordering::Acquire), writes);
        }
        assert_eq!(backend.deletes.load(Ordering::Acquire), 0);
        assert_eq!(runtime.resources().status().requests, 0);
        assert_eq!(runtime.resources().status().held_bytes, 0);
        runtime.shutdown().await.unwrap();
    }
}
