// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    FileIdentity, FileTree, MultipartLimits, MultipartPart, MultipartPhase, MultipartRepository,
    MultipartSession, TableLocation,
};
use crowdb_access_iceberg::key::{FileId, OperationId};
use sha2::{Digest, Sha256};

use super::TestNativeStorage;
use crate::common::control;

pub async fn verify(stack: &TestNativeStorage, context: CatalogContext, table: TableLocation) {
    let store = stack.store().await;
    let initial = MultipartSession {
        context,
        upload: OperationId::random(),
        owner: FileIdentity {
            table,
            file: FileId::random(),
        },
        location: table.file("multipart/expired.bin").unwrap(),
        principal: [1; 32],
        revision: 1,
        created_ms: 1,
        expires_ms: 1001,
        limits: MultipartLimits {
            max_parts: 1,
            max_part_bytes: 100,
            max_file_bytes: 100,
            max_staged_bytes: 100,
            ttl_ms: 1000,
        },
        phase: MultipartPhase::Open,
        part_count: 0,
        staged_bytes: 0,
        completion: None,
        published: None,
        pending: None,
        credit: None,
    };
    let repository = MultipartRepository::new(store);
    let admitted = repository.begin(&initial, 1).await.unwrap();
    let part = MultipartPart {
        upload: initial.upload,
        number: 1,
        revision: 1,
        modified_ms: 2,
        owner: FileIdentity {
            table,
            file: FileId::random(),
        },
        tree: Some(FileTree {
            root: None,
            length: 0,
            digest: Sha256::digest([]).into(),
        }),
        stream: None,
    };
    assert!(repository.reserve_part(&admitted, &part, 2).await.unwrap());
    control::node("start", 13).await;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            assert!(
                control::node("status", 13).await["Running"] == true,
                "Iceberg worker exited"
            );
            let current = repository.load(context, initial.upload).await.unwrap().unwrap();
            if current.phase == MultipartPhase::Aborted {
                assert!(current.credit.is_none());
                assert!(current.pending.is_none());
                assert_eq!((current.part_count, current.staged_bytes), (1, 0));
                assert_eq!(repository.part(&current, 1).await.unwrap().as_ref(), Some(&part));
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
