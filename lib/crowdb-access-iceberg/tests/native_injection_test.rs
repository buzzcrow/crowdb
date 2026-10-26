#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::{
    file::{DirectFileLocations, FileIdentity, FileRepository, FileTree, FileTreeWriter, NativeInjection},
    key::{FileId, NamespaceId, OperationId, StorageKey, TableStorageScope},
    operation::{RequestIdentity, RETRY_WINDOW_MS},
    record::StorageRecord,
};
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
};

async fn upload(fixture: &fixture::TestFile, blocks: Arc<blocks::TestBlocks>) -> (FileIdentity, FileTree) {
    let owner = FileIdentity {
        table: fixture.table,
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(blocks, owner, 128).unwrap();
    writer.push(&[7; 1024]).await.unwrap();
    (owner, writer.finish().await.unwrap())
}

#[tokio::test]
async fn complete_native_uploads_publish_only_file_resources_and_whole_retries_need_no_receipt() {
    let mut fixture = fixture::TestFile::new(common::TestStore::default()).await;
    fixture.table.storage_namespace = Some(NamespaceId::random());
    let blocks = Arc::new(blocks::TestBlocks::default());
    let signer = DirectFileLocations::new(1, BTreeMap::from([(1, [1; 32])])).unwrap();
    let (owner, tree) = upload(&fixture, blocks.clone()).await;
    fixture.store.file_record_reply_loss.store(true, Ordering::SeqCst);
    let reads = fixture.store.reads.load(Ordering::SeqCst);
    let writes = fixture.store.cas_attempts.load(Ordering::SeqCst);
    assert!(FileRepository::new(fixture.store.clone())
        .finalize_native_parquet(fixture.context, owner, "part-0001", tree.clone(), &signer)
        .await
        .is_err());
    assert_eq!(fixture.store.reads.load(Ordering::SeqCst) - reads, 1);
    assert_eq!(fixture.store.cas_attempts.load(Ordering::SeqCst) - writes, 1);
    let first = signer
        .parquet(
            owner.table,
            "part-0001",
            owner.file,
            tree.length,
            tree.digest,
            tree.root.unwrap(),
        )
        .unwrap();
    let first = FileRepository::new(fixture.store.clone())
        .load(fixture.context, &first.location)
        .await
        .unwrap()
        .unwrap();
    let (next_owner, next_tree) = upload(&fixture, blocks).await;
    let reads = fixture.store.reads.load(Ordering::SeqCst);
    let writes = fixture.store.cas_attempts.load(Ordering::SeqCst);
    let restarted = FileRepository::new(fixture.store.clone());
    let next = restarted
        .finalize_native_parquet(fixture.context, next_owner, "part-0001", next_tree, &signer)
        .await
        .unwrap();
    assert_eq!(fixture.store.reads.load(Ordering::SeqCst) - reads, 1);
    assert_eq!(fixture.store.cas_attempts.load(Ordering::SeqCst) - writes, 1);
    assert_ne!(next.location, first.location);
    assert_ne!(next.file, first.file);
    assert_eq!((next.length, next.digest), (first.length, first.digest));
    let rotated = DirectFileLocations::new(2, BTreeMap::from([(1, [1; 32]), (2, [2; 32])])).unwrap();
    assert_eq!(rotated.resolve(&first.location).unwrap(), Some(first.clone()));
    assert_eq!(rotated.resolve(&next.location).unwrap(), Some(next.clone()));
    let records = fixture.store.values.load_full();
    assert_eq!(records.keys().filter(|key| matches!(StorageKey::decode(key), Ok(StorageKey::NativeTable(key)) if key.scope == TableStorageScope::File)).count(), 2);
    assert_eq!(records.keys().filter(|key| matches!(StorageKey::decode(key), Ok(StorageKey::NativeTable(key)) if key.scope == TableStorageScope::Operation)).count(), 0);
    let deleted = restarted
        .mark_deleted(fixture.context, &first, 105)
        .await
        .unwrap();
    assert!(restarted.publish(fixture.context, &first).await.is_err());
    assert_eq!(
        restarted.deleted(fixture.context, &first.location).await.unwrap(),
        Some(deleted)
    );
    assert_eq!(
        restarted.load(fixture.context, &next.location).await.unwrap(),
        Some(next)
    );
}

#[tokio::test]
async fn historical_injection_receipts_remain_decodable_without_a_forward_driver() {
    let mut fixture = fixture::TestFile::new(common::TestStore::default()).await;
    fixture.table.storage_namespace = Some(NamespaceId::random());
    let (owner, tree) = upload(&fixture, Arc::new(blocks::TestBlocks::default())).await;
    let signer = DirectFileLocations::new(1, BTreeMap::from([(1, [1; 32])])).unwrap();
    let file = signer
        .parquet(
            owner.table,
            "part-0001",
            owner.file,
            tree.length,
            tree.digest,
            tree.root.unwrap(),
        )
        .unwrap();
    let receipt = NativeInjection {
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        activation_epoch: fixture.context.activation_epoch,
        principal: [3; 32],
        stem: "part-0001".into(),
        file,
    };
    let key = receipt.key().unwrap();
    let record = StorageRecord::NativeInjection(Box::new(receipt.clone()));
    assert_eq!(
        StorageRecord::decode_storage(&key, &record.encode().unwrap()).unwrap(),
        record
    );
    assert!(receipt.retained(100 + RETRY_WINDOW_MS));
    assert!(!receipt.retained(101 + RETRY_WINDOW_MS));
    let mut corrupt = receipt;
    corrupt.file.location = fixture.table.file("data/part-0001.parquet").unwrap();
    assert!(corrupt.validate().is_err());
}
