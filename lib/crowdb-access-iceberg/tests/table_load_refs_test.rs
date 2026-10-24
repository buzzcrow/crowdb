// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
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
    file::{ContentFormat, FileContent, FileIdentity, FileKind, FileRecord, FileRepository, FileTreeWriter},
    key::FileId,
    table::{SnapshotLoadingMode, TableLoad, TableLoadError, TableMetadataError},
};
use fixture::TestTable;
use serde_json::value::RawValue;
use std::{collections::BTreeMap, sync::atomic::Ordering};

type TestFields = BTreeMap<String, Box<RawValue>>;

fn raw(value: &str) -> Box<RawValue> {
    RawValue::from_string(value.into()).unwrap()
}
async fn replace(fixture: &mut TestTable, fields: &TestFields) {
    fixture.bytes = serde_json::to_vec(fields).unwrap();
    fixture.head.metadata_file = FileId::random();
    fixture.head.generation += 1;
    fixture.head.metadata_location = fixture
        .head
        .metadata_location
        .table()
        .file(&format!("metadata/{}.json", fixture.head.metadata_file))
        .unwrap();
    let mut writer = FileTreeWriter::new(
        fixture.blocks.clone(),
        FileIdentity {
            table: fixture.head.metadata_location.table(),
            file: fixture.head.metadata_file,
        },
        1024,
    )
    .unwrap();
    writer.push(&fixture.bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    fixture.head.metadata_digest = tree.digest;
    FileRepository::new(fixture.fixture.store.clone())
        .publish(
            fixture.fixture.context,
            &FileRecord {
                file: fixture.head.metadata_file,
                location: fixture.head.metadata_location.clone(),
                kind: FileKind::Metadata,
                format: ContentFormat::Json,
                length: tree.length,
                digest: tree.digest,
                content: FileContent::Chunks { root: tree.root },
                hint: None,
            },
        )
        .await
        .unwrap();
    fixture.publish_head().await;
}
async fn refs(fixture: &TestTable) -> Vec<u8> {
    let TableLoad::Loaded { metadata, .. } = fixture
        .loader()
        .load(
            fixture.fixture.context,
            &fixture.parent.identifier,
            &fixture.head.name,
            SnapshotLoadingMode::Refs,
            None,
        )
        .await
        .unwrap()
    else {
        panic!("expected refs");
    };
    metadata
}

#[tokio::test]
async fn refs_keep_original_snapshot_order_and_exact_raw_values_without_writes() {
    let mut fixture = TestTable::new().await;
    let mut fields: TestFields = serde_json::from_slice(&fixture.bytes).unwrap();
    let snapshots: Vec<Box<RawValue>> = serde_json::from_str(fields["snapshots"].get()).unwrap();
    fields.insert(
        "snapshots".into(),
        raw(&format!(
            "[{}, {}, {}]",
            snapshots[2].get(),
            snapshots[0].get(),
            snapshots[1].get()
        )),
    );
    fields.insert(
        "extension".into(),
        raw(r#"{ "escaped": "\u4e2d", "number": 1e0, "array": [ 1,  2 ] }"#),
    );
    fields.insert(
        "refs".into(),
        raw(r#"{ "main" : { "type":"branch", "snapshot-id":20 }, "t\u0061g" : {"type":"tag","snapshot-id":30} }"#),
    );
    replace(&mut fixture, &fields).await;
    let writes = fixture.fixture.store.writes.load(Ordering::Relaxed);
    let reads = fixture.fixture.store.reads.load(Ordering::Relaxed);
    let bytes = refs(&fixture).await;
    assert_eq!(fixture.fixture.store.reads.load(Ordering::Relaxed) - reads, 10);
    let actual: TestFields = serde_json::from_slice(&bytes).unwrap();
    let selected: Vec<&RawValue> = serde_json::from_str(actual["snapshots"].get()).unwrap();
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].get(), snapshots[2].get());
    assert_eq!(selected[1].get(), snapshots[1].get());
    for (name, value) in &fields {
        if name != "snapshots" {
            assert_eq!(actual[name].get(), value.get(), "{name}");
        }
    }
    assert_eq!(fixture.fixture.store.writes.load(Ordering::Relaxed), writes);
    assert_eq!(fixture.fixture.store.projection_reads.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn refs_use_current_snapshot_when_refs_are_absent_or_null() {
    let mut fixture = TestTable::new().await;
    let mut fields: TestFields = serde_json::from_slice(&fixture.bytes).unwrap();
    for refs_field in [None, Some(raw("null"))] {
        if let Some(value) = refs_field {
            fields.insert("refs".into(), value);
        } else {
            fields.remove("refs");
        }
        replace(&mut fixture, &fields).await;
        let value: serde_json::Value = serde_json::from_slice(&refs(&fixture).await).unwrap();
        let snapshots = value["snapshots"].as_array().unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0]["snapshot-id"], 20);
    }
}

#[tokio::test]
async fn refs_preserve_absent_snapshot_array_and_reject_null_before_representation() {
    let mut fixture = TestTable::new().await;
    let mut fields: TestFields = serde_json::from_slice(&fixture.bytes).unwrap();
    fields.insert("current-snapshot-id".into(), raw("-1"));
    fields.insert("refs".into(), raw("null"));
    fields.remove("snapshots");
    replace(&mut fixture, &fields).await;
    assert_eq!(refs(&fixture).await, fixture.bytes);
    fields.insert("snapshots".into(), raw("null"));
    replace(&mut fixture, &fields).await;
    assert!(matches!(
        fixture
            .loader()
            .load(
                fixture.fixture.context,
                &fixture.parent.identifier,
                &fixture.head.name,
                SnapshotLoadingMode::Refs,
                None,
            )
            .await,
        Err(TableLoadError::Metadata(TableMetadataError::Field("snapshots")))
    ));
}
