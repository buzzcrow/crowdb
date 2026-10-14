#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::{
    file::{
        DirectFileLocations, FileIdentity, FileRepository, FileTreeWriter, NativeInjectionRepository,
        NativeInjectionRequest,
    },
    key::{FileId, NamespaceId, OperationId, StorageKey, TableStorageScope},
    operation::{RequestIdentity, RETRY_WINDOW_MS},
};
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
};

#[tokio::test]
async fn durable_injection_selection_survives_both_lost_replies_and_key_rotation() {
    for selection_loss in [true, false] {
        let mut fixture = fixture::TestFile::new(common::TestStore::default()).await;
        fixture.table.storage_namespace = Some(NamespaceId::random());
        let blocks = Arc::new(blocks::TestBlocks::default());
        let owner = FileIdentity {
            table: fixture.table,
            file: FileId::random(),
        };
        let mut writer = FileTreeWriter::new(blocks, owner, 128).unwrap();
        writer.push(&[7; 1024]).await.unwrap();
        let tree = writer.finish().await.unwrap();
        let request = NativeInjectionRequest {
            context: fixture.context,
            table: fixture.table,
            identity: RequestIdentity {
                operation: OperationId::random(),
                issued_ms: 100,
            },
            principal: [3; 32],
            stem: "part-0001".into(),
            digest: tree.digest,
        };
        let signer = DirectFileLocations::new(1, BTreeMap::from([(1, [1; 32])])).unwrap();
        let repository = NativeInjectionRepository::new(fixture.store.clone());
        assert!(repository.replay(&request, 101).await.unwrap().is_none());
        if selection_loss {
            fixture
                .store
                .native_injection_reply_loss
                .store(true, Ordering::SeqCst);
            repository
                .finish(&request, owner, tree, &signer, 101)
                .await
                .unwrap();
        } else {
            fixture.store.file_record_reply_loss.store(true, Ordering::SeqCst);
            assert!(repository
                .finish(&request, owner, tree, &signer, 101)
                .await
                .is_err());
        }
        let restarted = NativeInjectionRepository::new(fixture.store.clone());
        let file = restarted.replay(&request, 102).await.unwrap().unwrap();
        assert_eq!(file.file, owner.file);
        let rotated = DirectFileLocations::new(2, BTreeMap::from([(1, [1; 32]), (2, [2; 32])])).unwrap();
        assert_eq!(rotated.resolve(&file.location).unwrap(), Some(file.clone()));
        assert_eq!(restarted.replay(&request, 103).await.unwrap(), Some(file.clone()));
        let records = fixture.store.values.load_full();
        assert_eq!(records.keys().filter(|key| matches!(StorageKey::decode(key), Ok(StorageKey::NativeTable(key)) if key.scope == TableStorageScope::File)).count(), 1);
        assert_eq!(records.keys().filter(|key| matches!(StorageKey::decode(key), Ok(StorageKey::NativeTable(key)) if key.scope == TableStorageScope::Operation)).count(), 1);
        let mut changed = request.clone();
        changed.digest[0] ^= 1;
        assert!(restarted.replay(&changed, 104).await.is_err());
        changed = request.clone();
        changed.principal[0] ^= 1;
        assert!(restarted.replay(&changed, 104).await.is_err());
        changed = request.clone();
        changed.table.storage_namespace = Some(NamespaceId::random());
        assert!(restarted.replay(&changed, 104).await.unwrap().is_none());
        assert!(restarted.replay(&request, 100 + RETRY_WINDOW_MS).await.is_err());
        FileRepository::new(fixture.store.clone())
            .mark_deleted(fixture.context, &file, 105)
            .await
            .unwrap();
        assert!(restarted.replay(&request, 106).await.is_err());
    }
}
