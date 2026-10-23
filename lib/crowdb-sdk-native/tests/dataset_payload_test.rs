// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_composition.rs"]
mod composition;
#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/write_storage.rs"]
mod write_storage;

use crowdb_access_dataset::{DatasetIdentity, DatasetUploads, NamespacePath, OperationId};
use crowdb_access_s3::metadata::TenantId;
use crowdb_protocol::frame::MAX_FRAME_BYTES;
use crowdb_sdk_native::{
    dataset::{BatchRequest, DatasetClient},
    s3::S3Client,
    OperationContext,
};
use md5::{Digest, Md5};
use std::{
    num::NonZeroU64,
    time::{Duration, Instant},
};

fn context(id: u64) -> OperationContext {
    OperationContext::new(
        NonZeroU64::new(id).unwrap(),
        Instant::now() + Duration::from_secs(10),
    )
}

#[tokio::test]
async fn framed_dataset_upload_and_s3_share_runtime_but_preserve_independent_authority() {
    let backend = write_storage::TestWriteStorage::new();
    let runtime = backend.runtime(metadata::client().await);
    let identity = DatasetIdentity::new(NamespacePath::root(), "payload").unwrap();
    let mut policy =
        crowdb_chunk_client::SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::Dataset);
    policy.conversion_enabled = false;
    policy.max_pipelines = 1;
    let dataset = DatasetClient::with_writer_policy(&runtime, identity.clone(), &policy).unwrap();
    let s3 = S3Client::new(runtime.clone(), TenantId::new(b"tenant".to_vec()).unwrap()).unwrap();
    s3.create_bucket(context(1), b"bucket").await.unwrap();
    dataset.create(context(2)).await.unwrap();
    let upload = OperationId::random();
    let digest: [u8; 16] = Md5::digest(b"retained").into();
    dataset
        .create_upload(context(3), upload, 8, 8, digest, u64::MAX)
        .await
        .unwrap();
    let buffer = runtime
        .allocate_producer(u32::try_from(MAX_FRAME_BYTES).unwrap())
        .unwrap()
        .initialize_framed(8, 0, |_, output| output.copy_from_slice(b"retained"))
        .unwrap();
    let views = buffer.payload_views();
    let pointer = views[0].as_ptr();
    dataset
        .upload_framed_part(context(4), upload, 0, buffer)
        .await
        .unwrap();
    assert!(
        backend.data.iter().any(|entry| {
            let bytes = entry.value();
            let start = bytes.as_ptr() as usize;
            (start..start + bytes.len()).contains(&(pointer as usize))
        }),
        "storage must retain the original framed producer allocation"
    );
    dataset.complete_upload(context(5), upload).await.unwrap();
    let location = DatasetUploads::location(&identity, upload).unwrap();
    let snapshot = composition::publish(&dataset, location.clone(), digest).await;
    dataset
        .control(context(60), |scope| async move {
            assert!(matches!(
                scope.uploads.abort(&location).await,
                Err(crowdb_access_dataset::UploadError::Conflict)
            ));
            Ok(())
        })
        .await
        .unwrap();
    let mut read = dataset
        .read_batch(
            context(7),
            BatchRequest {
                snapshot,
                sample_ids: vec![b"one".to_vec()],
                fields: vec!["value".into()],
            },
        )
        .await
        .unwrap();
    let held = read.body.next().await.unwrap().unwrap().buffer;
    assert_eq!(held.as_slice(), b"retained");
    assert!(read.body.next().await.unwrap().is_none());
    drop(dataset);
    assert!(s3.head_bucket(context(8), b"bucket").await.is_ok());
    runtime.shutdown().await.unwrap();
    assert_eq!(held.as_slice(), b"retained");
    assert_eq!(views[0].as_ref(), b"retained");
}

#[tokio::test]
async fn maximum_upload_part_keeps_every_producer_allocation_through_durable_write() {
    let backend = write_storage::TestWriteStorage::new();
    let runtime = backend.runtime(metadata::client().await);
    let identity = DatasetIdentity::new(NamespacePath::root(), "large-part").unwrap();
    let dataset = DatasetClient::new(&runtime, identity).unwrap();
    dataset.create(context(1)).await.unwrap();
    let data: Vec<u8> = (0..4 * 1024 * 1024)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();
    let upload = OperationId::random();
    dataset
        .create_upload(
            context(2),
            upload,
            data.len() as u64,
            4 << 20,
            Md5::digest(&data).into(),
            u64::MAX,
        )
        .await
        .unwrap();
    let buffer = runtime.prepare_framed(&data).unwrap();
    let views = buffer.payload_views();
    dataset
        .upload_framed_part(context(3), upload, 0, buffer)
        .await
        .unwrap();
    for view in &views {
        assert!(
            backend.data.iter().any(|entry| {
                let bytes = entry.value();
                let start = bytes.as_ptr() as usize;
                (start..start + bytes.len()).contains(&(view.as_ptr() as usize))
            }),
            "every physical write must retain its original producer allocation"
        );
    }
    dataset.complete_upload(context(4), upload).await.unwrap();
    assert_eq!(views.concat(), data);
    runtime.shutdown().await.unwrap();
}
