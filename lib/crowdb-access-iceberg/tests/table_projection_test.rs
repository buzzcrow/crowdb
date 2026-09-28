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
use sha2::{Digest, Sha256};

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
async fn validated_refs_reuse_pages_but_still_verify_canonical_storage() {
    let fixture = TestTable::new().await;
    let loader = fixture.loader();
    let expected = load(&fixture, &loader, SnapshotLoadingMode::Refs).await;
    assert_eq!(loader.projection_hits_for_tests(), 0);
    let reads = fixture.blocks.reads.load(Ordering::SeqCst);
    assert_eq!(load(&fixture, &loader, SnapshotLoadingMode::Refs).await, expected);
    assert_eq!(loader.projection_hits_for_tests(), 1);
    assert!(fixture.blocks.reads.load(Ordering::SeqCst) > reads);
    assert_eq!(
        load(&fixture, &loader, SnapshotLoadingMode::All).await,
        fixture.bytes
    );
    assert_eq!(loader.projection_hits_for_tests(), 1);
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
    assert_eq!(loader.projection_hits_for_tests(), 1);
}

#[tokio::test]
async fn every_missing_or_corrupt_projection_page_and_receipt_falls_back_exactly() {
    let fixture = TestTable::new().await;
    let loader = fixture.loader();
    let expected = load(&fixture, &loader, SnapshotLoadingMode::Refs).await;
    let original = fixture.fixture.store.values.load_full();
    let keys: Vec<_> = original.keys().filter(|key| projection(key)).cloned().collect();
    assert!(keys.len() > 2);
    for key in &keys {
        for remove in [false, true] {
            let mut damaged = (*original).clone();
            if remove {
                damaged.remove(key);
            } else {
                damaged.get_mut(key).unwrap().bytes[0] ^= 1;
            }
            fixture.fixture.store.values.store(Arc::new(damaged));
            assert_eq!(load(&fixture, &loader, SnapshotLoadingMode::Refs).await, expected);
            assert_eq!(loader.projection_hits_for_tests(), 0);
            assert_eq!(
                load(&fixture, &loader, SnapshotLoadingMode::All).await,
                fixture.bytes
            );
        }
    }
}

#[tokio::test]
async fn projection_identity_and_version_cannot_replace_validation_receipt() {
    let fixture = TestTable::new().await;
    let loader = fixture.loader();
    let expected = load(&fixture, &loader, SnapshotLoadingMode::Refs).await;
    let original = fixture.fixture.store.values.load_full();
    let root_key = original
        .keys()
        .find(|key| projection(key) && key.ends_with(&[0; 4]))
        .unwrap();
    for field in ["version", "generation", "digest", "table", "length"] {
        let mut damaged = (*original).clone();
        let root = damaged.get_mut(root_key).unwrap();
        let mut body: serde_json::Value = serde_json::from_slice(&root.bytes[32..]).unwrap();
        if matches!(field, "digest" | "table") {
            body[field][0] = serde_json::json!(body[field][0].as_u64().unwrap() ^ 1);
        } else {
            body[field] = serde_json::json!(999);
        }
        let body = serde_json::to_vec(&body).unwrap();
        root.bytes = Sha256::digest(&body).to_vec();
        root.bytes.extend(body);
        fixture.fixture.store.values.store(Arc::new(damaged));
        assert_eq!(load(&fixture, &loader, SnapshotLoadingMode::Refs).await, expected);
        assert_eq!(loader.projection_hits_for_tests(), 0);
    }
}

#[tokio::test]
async fn receipt_never_relaxes_parser_limits_or_selected_head_binding() {
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
        assert_eq!(loader.projection_hits_for_tests(), 0);
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
    assert_eq!(loader.projection_hits_for_tests(), 0);
}

#[tokio::test]
async fn optional_projection_write_failures_do_not_fail_canonical_loads() {
    let fixture = TestTable::new().await;
    let original = fixture.fixture.store.values.load_full();
    let loader = fixture.loader();
    let expected = load(&fixture, &loader, SnapshotLoadingMode::Refs).await;
    let count = fixture
        .fixture
        .store
        .values
        .load()
        .keys()
        .filter(|key| projection(key))
        .count();
    for boundary in 1..=count {
        fixture.fixture.store.values.store(original.clone());
        let writes = fixture.fixture.store.writes.load(Ordering::SeqCst);
        fixture
            .fixture
            .store
            .fail_after
            .store(writes + boundary, Ordering::SeqCst);
        assert_eq!(
            load(&fixture, &fixture.loader(), SnapshotLoadingMode::Refs).await,
            expected
        );
        fixture.fixture.store.fail_after.store(0, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn warm_projections_cannot_hide_head_or_namespace_changes_during_reads() {
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
        assert_eq!(loader.projection_hits_for_tests(), 1);
    }
}
