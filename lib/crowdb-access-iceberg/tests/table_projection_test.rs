#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/table_read.rs"]
mod fixture;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace.rs"]
mod namespaces;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    key::{CatalogScope, IcebergKey},
    table::{SnapshotLoadingMode, TableLoad, TableLoader},
};
use fixture::TestTable;

async fn load(fixture: &TestTable, loader: &TableLoader, mode: SnapshotLoadingMode) -> Vec<u8> {
    let TableLoad::Loaded { metadata, .. } = loader
        .load(
            fixture.fixture.context,
            &fixture.parent.identifier,
            &fixture.head.name,
            mode,
            None,
        )
        .await
        .unwrap()
    else {
        panic!("expected loaded metadata")
    };
    metadata
}

fn projection(key: &[u8]) -> bool {
    matches!(
        IcebergKey::decode(key),
        Ok(IcebergKey::Catalog {
            scope: CatalogScope::MetadataProjection,
            ..
        })
    )
}

#[tokio::test]
async fn cold_repeated_and_concurrent_loads_never_write_or_read_projection_state() {
    let fixture = Arc::new(TestTable::new().await);
    let loader = Arc::new(fixture.loader());
    let store = &fixture.fixture.store;
    let writes = store.writes.load(Ordering::SeqCst);
    let attempts = store.cas_attempts.load(Ordering::SeqCst);
    let before = store.values.load_full();
    let expected = load(&fixture, &loader, SnapshotLoadingMode::Refs).await;
    let canonical_reads = fixture.blocks.reads.load(Ordering::SeqCst);
    let mut readers = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let fixture = fixture.clone();
        let loader = loader.clone();
        readers.spawn(async move { load(&fixture, &loader, SnapshotLoadingMode::Refs).await });
    }
    while let Some(result) = readers.join_next().await {
        assert_eq!(result.unwrap(), expected);
    }
    assert!(fixture.blocks.reads.load(Ordering::SeqCst) > canonical_reads);
    assert_eq!(
        load(&fixture, &loader, SnapshotLoadingMode::All).await,
        fixture.bytes
    );
    assert!(matches!(
        loader
            .load(
                fixture.fixture.context,
                &fixture.parent.identifier,
                &fixture.head.name,
                SnapshotLoadingMode::Refs,
                Some("*")
            )
            .await
            .unwrap(),
        TableLoad::NotModified { .. }
    ));
    assert_eq!(store.writes.load(Ordering::SeqCst), writes);
    assert_eq!(store.cas_attempts.load(Ordering::SeqCst), attempts);
    assert_eq!(store.projection_reads.load(Ordering::SeqCst), 0);
    assert!(Arc::ptr_eq(&before, &store.values.load_full()));
    fixture.blocks.corrupt_reads.store(true, Ordering::SeqCst);
    assert!(loader
        .load(
            fixture.fixture.context,
            &fixture.parent.identifier,
            &fixture.head.name,
            SnapshotLoadingMode::Refs,
            Some("*")
        )
        .await
        .is_err());
}

#[tokio::test]
async fn existing_or_corrupt_projection_pages_are_ignored_without_repair() {
    use crowdb_access_iceberg::{file::FileRepository, metadata_projection::ProjectionStore};
    let fixture = TestTable::new().await;
    let loader = fixture.loader();
    let expected = load(&fixture, &loader, SnapshotLoadingMode::Refs).await;
    let record = FileRepository::new(fixture.fixture.store.clone())
        .load(fixture.fixture.context, &fixture.head.metadata_location)
        .await
        .unwrap()
        .unwrap();
    assert!(
        ProjectionStore::new(fixture.fixture.store.clone(), fixture.blocks.clone())
            .put(&record, fixture.head.generation, &fixture.bytes)
            .await
    );
    let store = &fixture.fixture.store;
    let original = store.values.load_full();
    let keys: Vec<_> = original.keys().filter(|key| projection(key)).cloned().collect();
    assert!(!keys.is_empty());
    let attempts = store.cas_attempts.load(Ordering::SeqCst);
    assert_eq!(load(&fixture, &loader, SnapshotLoadingMode::Refs).await, expected);
    for key in keys {
        for remove in [false, true] {
            let mut damaged = (*original).clone();
            if remove {
                damaged.remove(&key);
            } else {
                damaged.get_mut(&key).unwrap().bytes[0] ^= 1;
            }
            store.values.store(Arc::new(damaged));
            assert_eq!(load(&fixture, &loader, SnapshotLoadingMode::Refs).await, expected);
        }
    }
    assert_eq!(store.cas_attempts.load(Ordering::SeqCst), attempts);
    assert_eq!(store.projection_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn loads_preserve_parser_limits_and_selected_head_binding() {
    let mut fixture = TestTable::new().await;
    load(&fixture, &fixture.loader(), SnapshotLoadingMode::Refs).await;
    for limit in 0..5 {
        let mut limits = metadata::limits();
        match limit {
            0 => limits.bytes = fixture.bytes.len() - 1,
            1 => limits.values = 1,
            2 => limits.depth = 1,
            3 => limits.string_bytes = 1,
            _ => limits.collection_entries = 1,
        }
        let loader = TableLoader::new(fixture.fixture.store.clone(), fixture.blocks.clone(), limits);
        assert!(loader
            .load(
                fixture.fixture.context,
                &fixture.parent.identifier,
                &fixture.head.name,
                SnapshotLoadingMode::Refs,
                None
            )
            .await
            .is_err());
    }
    fixture.head.table_uuid = Some(uuid::Uuid::new_v4());
    fixture.publish_head().await;
    let loader = fixture.loader();
    assert!(loader
        .load(
            fixture.fixture.context,
            &fixture.parent.identifier,
            &fixture.head.name,
            SnapshotLoadingMode::Refs,
            None
        )
        .await
        .is_err());
}

#[tokio::test]
async fn loads_reject_head_or_namespace_changes_during_reads() {
    for change_head in [false, true] {
        let fixture = TestTable::new().await;
        let loader = fixture.loader();
        load(&fixture, &loader, SnapshotLoadingMode::Refs).await;
        fixture.blocks.pause_reads.store(true, Ordering::SeqCst);
        let read = loader.load(
            fixture.fixture.context,
            &fixture.parent.identifier,
            &fixture.head.name,
            SnapshotLoadingMode::Refs,
            Some("*"),
        );
        tokio::pin!(read);
        tokio::select! {
            result = &mut read => panic!("unexpected completion {result:?}"),
            () = fixture.blocks.read_entered.notified() => {}
        }
        if change_head {
            let mut next = fixture.head.clone();
            next.generation += 1;
            fixture
                .fixture
                .put(
                    crowdb_access_iceberg::table::head_key(next.catalog, next.table),
                    crowdb_access_iceberg::record::StorageRecord::TableHead(Box::new(next)),
                )
                .await;
        } else {
            let replacement = fixture.fixture.authority(None, &["analytics"]);
            fixture.fixture.publish(&replacement).await;
        }
        fixture.blocks.pause_reads.store(false, Ordering::SeqCst);
        fixture.blocks.read_release.notify_one();
        assert!(matches!(
            read.await,
            Err(crowdb_access_iceberg::table::TableLoadError::Catalog(
                crowdb_access_iceberg::catalog::CatalogError::Conflict
            ))
        ));
    }
}
