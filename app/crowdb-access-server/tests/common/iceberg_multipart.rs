use std::sync::Arc;

use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    FileIdentity, FileReader, FileTreeWriter, MultipartLimits, MultipartPart, MultipartPhase,
    MultipartRecovery, MultipartRepository, MultipartSelection, MultipartSession, NativeFileBlocks,
    SelectedPart, TableLocation,
};
use crowdb_access_iceberg::key::{FileId, OperationId};

use crate::common::TestIcebergStack;

pub async fn verify_restart(stack: &mut TestIcebergStack, context: CatalogContext, table: TableLocation) {
    let client = crate::chunks(stack).await;
    let blocks = Arc::new(NativeFileBlocks::new(client.clone()));
    let initial = MultipartSession {
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
    };
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
        owner,
        tree: writer.finish().await.unwrap(),
    };
    assert!(repository.reserve_part(&initial, &part, 101).await.unwrap());
    let recovery = MultipartRecovery::new(stack.store().await, blocks.clone(), 7, 8).unwrap();
    let page = recovery.recover_page(context, None, 102).await.unwrap();
    assert!(page.failures.is_empty(), "{:?}", page.failures);
    assert_eq!(page.progressed, 1);
    let current = repository.load(context, initial.upload).await.unwrap().unwrap();
    assert!(current.pending.is_none());
    let selection = MultipartSelection::new(vec![SelectedPart {
        number: 1,
        revision: 1,
        digest: part.tree.digest,
    }])
    .unwrap();
    assert!(repository
        .freeze_completion(&current, &selection, 103)
        .await
        .unwrap());
    let page = recovery.recover_page(context, None, 104).await.unwrap();
    assert!(page.failures.is_empty(), "{:?}", page.failures);
    assert_eq!(page.progressed, 1);
    let current = repository.load(context, initial.upload).await.unwrap().unwrap();
    assert_eq!(current.completion.as_ref().unwrap().progress.completed_bytes, 7);
    client.shutdown_small_writes().await.unwrap();
    drop(recovery);
    drop(repository);
    drop(blocks);
    drop(client);
    stack.chunk_kv.restart().await;
    verify_resumed(stack, &initial).await;
}

async fn verify_resumed(stack: &TestIcebergStack, initial: &MultipartSession) {
    let client = crate::chunks(stack).await;
    let blocks = Arc::new(NativeFileBlocks::new(client.clone()));
    let repository = MultipartRepository::new(stack.store().await);
    let recovered = repository
        .load(initial.context, initial.upload)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.completion.as_ref().unwrap().progress.completed_bytes, 7);
    let recovery = MultipartRecovery::new(stack.store().await, blocks.clone(), 7, 8).unwrap();
    for _ in 0..3 {
        let page = recovery.recover_page(initial.context, None, 105).await.unwrap();
        assert!(page.failures.is_empty(), "{:?}", page.failures);
        assert_eq!(page.progressed, 1);
    }
    let page = recovery.recover_page(initial.context, None, 105).await.unwrap();
    assert_eq!(page.awaiting_seal, vec![initial.upload]);
    let current = repository
        .load(initial.context, initial.upload)
        .await
        .unwrap()
        .unwrap();
    let tree = repository
        .assembled_tree(&current, blocks.clone(), 8)
        .await
        .unwrap();
    let reader = FileReader::from_tree(blocks, initial.owner, tree, None, 4096).unwrap();
    assert_eq!(crate::read_all(reader).await, b"durable multipart bytes");
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
