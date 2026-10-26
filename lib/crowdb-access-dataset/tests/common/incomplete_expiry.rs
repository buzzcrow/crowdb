// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "upload.rs"]
#[allow(dead_code)]
mod upload;
use crate::common::TestExpiryFixture;
use bytes::Bytes;
use crowdb_access_dataset::*;
use md5::{Digest, Md5};
use std::{sync::Arc, time::Duration};

pub const EXPIRES: u64 = 1000 + 86400;

pub async fn staged(f: &TestExpiryFixture, marker: u8) -> SnapshotId {
    f.authority
        .prepare_snapshot_with_operation(
            &f.identity,
            Some(f.root),
            crate::publication::publication_manifest(marker),
            OperationId::from_bytes(&[marker; 16]).unwrap(),
        )
        .await
        .unwrap()
}

pub async fn wait_for_pause(f: &TestExpiryFixture) {
    tokio::time::timeout(Duration::from_secs(2), f.store.started.notified())
        .await
        .unwrap();
}

pub fn index_row(f: &TestExpiryFixture, snapshot: SnapshotId) -> Vec<u8> {
    [
        b"DS\0I\0".as_slice(),
        &f.identity.key().unwrap(),
        snapshot.as_bytes(),
        b"S",
        &0_u32.to_be_bytes(),
    ]
    .concat()
}

pub async fn finish_reclaim(
    f: &TestExpiryFixture,
    uploads: &DatasetUploads,
    snapshot: SnapshotId,
) -> DatasetAuthority {
    let mut authority = DatasetAuthority::new(f.store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
    let mut cursor = SnapshotReclaimSweep::default();
    for window in 0..128 {
        if window == 3 {
            authority = DatasetAuthority::new(f.store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
            cursor = SnapshotReclaimSweep::default();
        }
        authority
            .reclaim_due_page(uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap();
        if authority
            .mark_snapshot_reclaim(&f.identity, snapshot)
            .await
            .unwrap()
            .complete
        {
            return authority;
        }
    }
    panic!("durable deletion facts did not finish after restart");
}

pub fn uploads(f: &TestExpiryFixture) -> DatasetUploads {
    DatasetUploads::new(f.store.clone(), Arc::new(upload::TestPayload::default()))
}

pub struct TestPreparedUpload {
    pub uploads: DatasetUploads,
    pub location: Vec<u8>,
    pub operation: OperationId,
    pub snapshot: SnapshotId,
    pub manifest: Vec<u8>,
}
impl TestPreparedUpload {
    pub async fn new(f: &TestExpiryFixture) -> Self {
        let uploads = uploads(f);
        let location = DatasetUploads::location(&f.identity, OperationId::random()).unwrap();
        let md5 = Md5::digest(b"data").into();
        uploads.create(&location, 4, 4, md5, u64::MAX).await.unwrap();
        uploads
            .put_part(&location, 0, Bytes::from_static(b"data"), 1000)
            .await
            .unwrap();
        uploads.complete(&location, 1000).await.unwrap();
        let operation = OperationId::from_bytes(&[2; 16]).unwrap();
        let mut manifest: ManifestRecord =
            bincode::deserialize(&crate::publication::publication_manifest(2)).unwrap();
        manifest.samples[0].fields[0].value = FieldLocator::Chunk {
            location: location.clone(),
            length: 4,
            md5,
        };
        let manifest = bincode::serialize(&manifest).unwrap();
        let snapshot = f
            .authority
            .prepare_snapshot_with_operation(&f.identity, Some(f.root), manifest.clone(), operation)
            .await
            .unwrap();
        Self {
            uploads,
            location,
            operation,
            snapshot,
            manifest,
        }
    }

    pub async fn claim(&self) {
        self.uploads
            .claim(&self.location, self.operation, 1000)
            .await
            .unwrap();
    }

    pub async fn prepare_shared_owner(&self, f: &TestExpiryFixture) -> SnapshotId {
        let operation = OperationId::from_bytes(&[3; 16]).unwrap();
        let snapshot = f
            .authority
            .prepare_snapshot_with_operation(&f.identity, Some(f.root), self.manifest.clone(), operation)
            .await
            .unwrap();
        self.uploads.claim(&self.location, operation, 1000).await.unwrap();
        f.authority
            .complete_snapshot_candidate(&f.identity, snapshot)
            .await
            .unwrap();
        f.authority.retain_snapshot(&f.identity, snapshot).await.unwrap();
        snapshot
    }
}
