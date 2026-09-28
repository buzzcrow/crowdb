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

use crowdb_access_iceberg::{
    catalog::CatalogError,
    table::{SnapshotLoadingMode, TableLoad, TableLoadError},
};
use fixture::TestTable;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn loads_preserve_canonical_bytes_and_use_representation_specific_etags() {
    let fixture = TestTable::new().await;
    let loader = fixture.loader();
    let context = fixture.fixture.context;
    let namespace = &fixture.parent.identifier;
    let name = &fixture.head.name;
    assert!(loader.exists(context, namespace, name).await.unwrap());
    let TableLoad::Loaded {
        metadata,
        etag: all_tag,
        ..
    } = loader
        .load(context, namespace, name, SnapshotLoadingMode::All, None)
        .await
        .unwrap()
    else {
        panic!("expected metadata");
    };
    assert_eq!(metadata, fixture.bytes);
    let TableLoad::Loaded {
        metadata,
        etag: refs_tag,
        ..
    } = loader
        .load(
            context,
            namespace,
            name,
            SnapshotLoadingMode::Refs,
            Some(&all_tag),
        )
        .await
        .unwrap()
    else {
        panic!("expected refs metadata");
    };
    assert_ne!(all_tag, refs_tag);
    let value: serde_json::Value = serde_json::from_slice(&metadata).unwrap();
    let ids: Vec<_> = value["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value["snapshot-id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, [20, 30]);
    assert!(matches!(
        loader
            .load(
                context,
                namespace,
                name,
                SnapshotLoadingMode::Refs,
                Some(&format!("W/{refs_tag}"))
            )
            .await
            .unwrap(),
        TableLoad::NotModified { .. }
    ));
    assert!(matches!(
        loader
            .load(context, namespace, "missing", SnapshotLoadingMode::All, Some("*"))
            .await
            .unwrap(),
        TableLoad::Missing
    ));
}

#[tokio::test]
async fn canonical_corruption_never_becomes_not_modified() {
    let fixture = TestTable::new().await;
    fixture.blocks.corrupt_reads.store(true, Ordering::SeqCst);
    assert!(fixture
        .loader()
        .load(
            fixture.fixture.context,
            &fixture.parent.identifier,
            &fixture.head.name,
            SnapshotLoadingMode::All,
            Some("*")
        )
        .await
        .is_err());
}

#[tokio::test]
async fn generation_changes_during_canonical_reads_fail_without_mixing() {
    let fixture = TestTable::new().await;
    let loader = fixture.loader();
    fixture.blocks.pause_reads.store(true, Ordering::SeqCst);
    let read = loader.load(
        fixture.fixture.context,
        &fixture.parent.identifier,
        &fixture.head.name,
        SnapshotLoadingMode::All,
        None,
    );
    tokio::pin!(read);
    tokio::select! {
        result = &mut read => panic!("unexpected completion {result:?}"),
        () = fixture.blocks.read_entered.notified() => {}
    }
    let mut head = fixture.head.clone();
    head.generation += 1;
    fixture
        .fixture
        .put(
            crowdb_access_iceberg::table::head_key(head.catalog, head.table),
            crowdb_access_iceberg::record::StorageRecord::TableHead(Box::new(head)),
        )
        .await;
    fixture.blocks.pause_reads.store(false, Ordering::SeqCst);
    fixture.blocks.read_release.notify_one();
    assert!(matches!(
        read.await,
        Err(TableLoadError::Catalog(CatalogError::Conflict))
    ));
}

#[tokio::test]
async fn namespace_recreation_does_not_expose_old_table_identity() {
    let fixture = TestTable::new().await;
    let replacement = fixture.fixture.authority(None, &["analytics"]);
    fixture.fixture.publish(&replacement).await;
    assert!(!fixture
        .loader()
        .exists(
            fixture.fixture.context,
            &replacement.identifier,
            &fixture.head.name
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn namespace_replacement_during_load_fails_before_response() {
    let fixture = TestTable::new().await;
    let loader = fixture.loader();
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
    let replacement = fixture.fixture.authority(None, &["analytics"]);
    fixture.fixture.publish(&replacement).await;
    fixture.blocks.pause_reads.store(false, Ordering::SeqCst);
    fixture.blocks.read_release.notify_one();
    assert!(matches!(
        read.await,
        Err(TableLoadError::Catalog(CatalogError::Conflict))
    ));
}

#[tokio::test]
async fn unchanged_bytes_in_a_new_generation_have_a_new_etag() {
    let mut fixture = TestTable::new().await;
    let loader = fixture.loader();
    let TableLoad::Loaded { etag: original, .. } = loader
        .load(
            fixture.fixture.context,
            &fixture.parent.identifier,
            &fixture.head.name,
            SnapshotLoadingMode::All,
            None,
        )
        .await
        .unwrap()
    else {
        panic!("expected metadata");
    };
    fixture.head.generation += 1;
    fixture.publish_head().await;
    let TableLoad::Loaded {
        etag: updated,
        metadata,
        ..
    } = loader
        .load(
            fixture.fixture.context,
            &fixture.parent.identifier,
            &fixture.head.name,
            SnapshotLoadingMode::All,
            Some(&original),
        )
        .await
        .unwrap()
    else {
        panic!("expected new generation");
    };
    assert_ne!(original, updated);
    assert_eq!(metadata, fixture.bytes);
}
