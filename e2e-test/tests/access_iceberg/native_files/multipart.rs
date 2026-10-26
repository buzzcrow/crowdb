// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    FileIdentity, FileReader, FileTreeWriter, MultipartLimits, MultipartMutation, MultipartPart,
    MultipartPhase, MultipartRecovery, MultipartRepository, MultipartSelection, MultipartSession,
    NativeFileBlocks, SelectedPart, TableLocation,
};
use crowdb_access_iceberg::key::{FileId, OperationId};

use super::{chunks, read_all, TestNativeStorage};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release native-file storage"]
async fn native_file_multipart_progress_and_bytes_survive_catalog_storage_restart() {
    let mut stack = TestNativeStorage::new();
    let context = super::seed_root(&stack).await;
    Box::pin(verify_restart(&mut stack, context, super::owner(context).table)).await;
}

fn initial_session(context: CatalogContext, table: TableLocation) -> MultipartSession {
    MultipartSession {
        context,
        upload: OperationId::random(),
        owner: FileIdentity {
            table,
            file: FileId::random(),
        },
        location: table.file("multipart/native.bin").unwrap(),
        principal: [1; 32],
        revision: 1,
        created_ms: 100,
        expires_ms: 10_100,
        limits: MultipartLimits {
            max_parts: 10,
            max_part_bytes: 100,
            max_file_bytes: 1000,
            max_staged_bytes: 1000,
            ttl_ms: 10_000,
        },
        phase: MultipartPhase::Open,
        part_count: 0,
        staged_bytes: 0,
        completion: None,
        published: None,
        pending: None,
        credit: None,
    }
}

pub async fn verify_restart(stack: &mut TestNativeStorage, context: CatalogContext, table: TableLocation) {
    let client = chunks(stack).await;
    let blocks = Arc::new(NativeFileBlocks::new(client.clone(), stack.store().await));
    let initial = initial_session(context, table);
    let repository = MultipartRepository::new(stack.store().await);
    repository.begin(&initial, 100).await.unwrap();
    let owner = FileIdentity {
        table,
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(blocks.clone(), owner, 8).unwrap();
    writer.push(b"durable multipart bytes").await.unwrap();
    let part = MultipartPart {
        upload: initial.upload,
        number: 1,
        revision: 1,
        modified_ms: 101,
        owner,
        tree: Some(writer.finish().await.unwrap()),
        stream: None,
    };
    assert!(repository.reserve_part(&initial, &part, 101).await.unwrap());
    let recovery = MultipartRecovery::new(stack.store().await);
    let page = recovery.recover_page(context, None, 102).await.unwrap();
    assert!(page.failures.is_empty(), "{:?}", page.failures);
    assert_eq!(page.progressed, 1);
    let current = repository.load(context, initial.upload).await.unwrap().unwrap();
    assert!(current.pending.is_none());
    let selection = MultipartSelection::new(vec![SelectedPart {
        number: 1,
        revision: 1,
        digest: part.selection_digest(),
    }])
    .unwrap();
    let MultipartMutation::Applied { session: frozen, .. } = repository
        .freeze_completion(&current, &selection, 103)
        .await
        .unwrap()
    else {
        panic!("freeze must accept this selected part generation");
    };
    let page = recovery.recover_page(context, None, 104).await.unwrap();
    assert!(page.failures.is_empty(), "{:?}", page.failures);
    assert_eq!((page.progressed, page.retained), (0, 1));
    assert!(page.awaiting_seal.is_empty());
    let tree = repository
        .assemble(
            &frozen,
            &selection,
            blocks.clone(),
            initial.limits.max_file_bytes,
            8,
        )
        .await
        .unwrap();
    let reader = FileReader::from_tree(blocks.clone(), initial.owner, tree, None, 4096).unwrap();
    assert_eq!(read_all(reader).await, b"durable multipart bytes");
    let current = repository.load(context, initial.upload).await.unwrap().unwrap();
    assert_eq!(current, frozen);
    assert_eq!(current.completion.as_ref().unwrap().progress.completed_bytes, 0);
    client.shutdown_small_writes().await.unwrap();
    drop(recovery);
    drop(repository);
    drop(blocks);
    drop(client);
    stack.restart_catalog().await;
    verify_retried(stack, &initial, &frozen, &selection).await;
}

async fn verify_retried(
    stack: &TestNativeStorage,
    initial: &MultipartSession,
    frozen: &MultipartSession,
    selection: &MultipartSelection,
) {
    let client = chunks(stack).await;
    let blocks = Arc::new(NativeFileBlocks::new(client.clone(), stack.store().await));
    let repository = MultipartRepository::new(stack.store().await);
    let recovered = repository
        .load(initial.context, initial.upload)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&recovered, frozen);
    let progress = &recovered.completion.as_ref().unwrap().progress;
    assert_eq!((progress.next_part, progress.completed_bytes), (0, 0));
    assert!(progress.writer.is_none());
    let recovery = MultipartRecovery::new(stack.store().await);
    for _ in 0..3 {
        let page = recovery.recover_page(initial.context, None, 105).await.unwrap();
        assert!(page.failures.is_empty(), "{:?}", page.failures);
        assert_eq!((page.progressed, page.retained), (0, 1));
        assert!(page.awaiting_seal.is_empty());
    }
    let page = recovery.recover_page(initial.context, None, 105).await.unwrap();
    assert_eq!((page.progressed, page.retained), (0, 1));
    assert!(page.awaiting_seal.is_empty());
    let current = repository
        .load(initial.context, initial.upload)
        .await
        .unwrap()
        .unwrap();
    let tree = repository
        .assemble(
            &current,
            selection,
            blocks.clone(),
            initial.limits.max_file_bytes,
            8,
        )
        .await
        .unwrap();
    let reader = FileReader::from_tree(blocks, initial.owner, tree, None, 4096).unwrap();
    assert_eq!(read_all(reader).await, b"durable multipart bytes");
    assert!(
        crowdb_access_iceberg::file::FileRepository::new(stack.store().await)
            .load(initial.context, &initial.location)
            .await
            .unwrap()
            .is_none()
    );
    assert!(repository.abort(&current).await.unwrap());
    client.shutdown_small_writes().await.unwrap();
}
