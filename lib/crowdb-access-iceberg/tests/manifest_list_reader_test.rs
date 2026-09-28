#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/manifest_list.rs"]
mod fixture;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::file::{
    AvroDatumLimits, AvroLimits, ContentFormat, FileContent, FileIdentity, FileKind, FileRecord,
    FileTreeWriter,
};
use crowdb_access_iceberg::key::FileId;
use crowdb_access_iceberg::manifest::{
    ManifestListProjection, ManifestListReader, ManifestListSelection, ManifestVersion,
};

fn long(value: usize, output: &mut Vec<u8>) {
    let mut encoded = u64::try_from(value).unwrap() << 1;
    while encoded > 127 {
        output.push((encoded & 127) as u8 | 128);
        encoded >>= 7;
    }
    output.push(u8::try_from(encoded).unwrap());
}

async fn stored(corrupt: bool, empty: bool) -> (Arc<blocks::TestBlocks>, FileRecord) {
    stored_metadata(corrupt, empty, &[]).await
}

async fn stored_metadata(
    corrupt: bool,
    empty: bool,
    metadata: &[(&str, &str)],
) -> (Arc<blocks::TestBlocks>, FileRecord) {
    stored_layout(corrupt, empty, metadata, fixture::TestManifestList::new()).await
}

async fn stored_layout(
    corrupt: bool,
    empty: bool,
    metadata: &[(&str, &str)],
    mut fixture: fixture::TestManifestList,
) -> (Arc<blocks::TestBlocks>, FileRecord) {
    let schema = fixture.schema_bytes();
    let mut bytes = b"Obj\x01".to_vec();
    long(1 + metadata.len(), &mut bytes);
    long(11, &mut bytes);
    bytes.extend(b"avro.schema");
    long(schema.len(), &mut bytes);
    bytes.extend(schema);
    for (key, value) in metadata {
        long(key.len(), &mut bytes);
        bytes.extend(key.as_bytes());
        long(value.len(), &mut bytes);
        bytes.extend(value.as_bytes());
    }
    bytes.push(0);
    bytes.extend([42; 16]);
    if !empty {
        for index in 0..2 {
            if corrupt && index == 1 {
                fixture.set(501, serde_json::json!(-1));
            }
            let payload = fixture.bytes().repeat(2);
            long(2, &mut bytes);
            long(payload.len(), &mut bytes);
            bytes.extend(payload);
            bytes.extend([42; 16]);
        }
    }
    let store = Arc::new(blocks::TestBlocks::default());
    let file = FileId::random();
    let owner = FileIdentity {
        table: fixture::table(),
        file,
    };
    let mut writer = FileTreeWriter::new(store.clone(), owner, 97).unwrap();
    writer.push(&bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    let record = FileRecord {
        file,
        location: fixture::table().file("metadata/list.avro").unwrap(),
        kind: FileKind::Unbound,
        format: ContentFormat::Avro,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
    (store, record)
}

async fn open(
    store: Arc<blocks::TestBlocks>,
    record: FileRecord,
    version: ManifestVersion,
) -> ManifestListReader {
    ManifestListReader::open(
        store,
        record.clone(),
        (record.location, version),
        AvroLimits {
            header_bytes: 8192,
            metadata_entries: 8,
            block_bytes: 4096,
            records_per_block: 8,
        },
        AvroDatumLimits {
            depth: 64,
            values: 1000,
            value_bytes: 1024,
        },
        4096,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn selected_unbound_lists_stream_records_and_blocks_until_verified_eof() {
    for version in [ManifestVersion::V1, ManifestVersion::V2, ManifestVersion::V3] {
        let fixture = fixture::TestManifestList::new();
        let schema = fixture.schema();
        let projection = ManifestListProjection::new(&schema, version, fixture::table()).unwrap();
        let bytes = fixture.bytes();
        let expected = projection
            .records(
                &bytes,
                1,
                AvroDatumLimits {
                    depth: 64,
                    values: 1000,
                    value_bytes: 1024,
                },
            )
            .unwrap()
            .next_entry()
            .unwrap()
            .unwrap();
        for empty in [false, true] {
            let (store, record) = stored(false, empty).await;
            let original = record.clone();
            let mut reader = open(store, record.clone(), version).await;
            for _ in 0..if empty { 0 } else { 4 } {
                assert!(!reader.is_complete());
                let entry = reader.next_entry().await.unwrap().unwrap();
                assert_eq!(entry, expected);
                assert_eq!(entry.length, 42);
                assert_eq!(entry.sequence, if version == ManifestVersion::V1 { 0 } else { 8 });
            }
            assert!(reader.next_entry().await.unwrap().is_none());
            assert!(reader.is_complete());
            assert!(reader.next_entry().await.unwrap().is_none());
            assert_eq!(record, original);
        }
    }
}

#[tokio::test]
async fn later_semantic_failure_never_marks_list_complete() {
    let (store, record) = stored(true, false).await;
    let mut reader = open(store, record, ManifestVersion::V3).await;
    for _ in 0..2 {
        assert!(reader.next_entry().await.unwrap().is_some());
    }
    assert!(reader.next_entry().await.is_err());
    assert!(!reader.is_complete());
    assert!(reader.next_entry().await.is_err());
}

#[tokio::test]
async fn cancelled_list_requires_a_fresh_reader() {
    let (store, record) = stored(false, false).await;
    let mut reader = open(store.clone(), record.clone(), ManifestVersion::V3).await;
    store.pause_reads.store(true, Ordering::SeqCst);
    tokio::select! {
        () = store.read_entered.notified() => {},
        result = reader.next_entry() => panic!("unexpected completion: {result:?}"),
    }
    assert!(!reader.is_complete());
    assert!(reader.next_entry().await.is_err());
    store.pause_reads.store(false, Ordering::SeqCst);
    let mut fresh = open(store, record, ManifestVersion::V3).await;
    assert!(fresh.next_entry().await.unwrap().is_some());
}

#[tokio::test]
async fn wrong_selection_kind_and_format_fail_before_storage_reads() {
    let (store, record) = stored(false, false).await;
    for invalid in 0..3 {
        let mut candidate = record.clone();
        let mut selected = record.location.clone();
        match invalid {
            0 => selected = fixture::table().file("metadata/other.avro").unwrap(),
            1 => candidate.kind = FileKind::Manifest,
            _ => candidate.format = ContentFormat::Parquet,
        }
        let before = store.reads.load(Ordering::SeqCst);
        let result = ManifestListReader::open(
            store.clone(),
            candidate,
            (selected, ManifestVersion::V3),
            AvroLimits {
                header_bytes: 8192,
                metadata_entries: 8,
                block_bytes: 4096,
                records_per_block: 8,
            },
            AvroDatumLimits {
                depth: 64,
                values: 1000,
                value_bytes: 1024,
            },
            4096,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(store.reads.load(Ordering::SeqCst), before);
    }
}

#[tokio::test]
async fn canonical_storage_corruption_poisoning_cannot_be_retried_in_place() {
    let (store, record) = stored(false, false).await;
    let mut reader = open(store.clone(), record, ManifestVersion::V3).await;
    store.corrupt_reads.store(true, Ordering::SeqCst);
    assert!(reader.next_entry().await.is_err());
    assert!(!reader.is_complete());
    store.corrupt_reads.store(false, Ordering::SeqCst);
    assert!(reader.next_entry().await.is_err());
}

fn selection(record: &FileRecord, version: ManifestVersion) -> ManifestListSelection {
    ManifestListSelection {
        location: record.location.clone(),
        table_version: version,
        snapshot_id: 99,
        parent_snapshot_id: None,
        sequence: if version == ManifestVersion::V1 { 0 } else { 8 },
        first_row_id: (version == ManifestVersion::V3).then_some(100),
        added_rows: (version == ManifestVersion::V3).then_some(30),
    }
}

async fn selected(
    store: Arc<blocks::TestBlocks>,
    record: FileRecord,
    selection: ManifestListSelection,
) -> Result<ManifestListReader, crowdb_access_iceberg::manifest::ManifestListError> {
    ManifestListReader::open_selected(
        store,
        record,
        selection,
        AvroLimits {
            header_bytes: 8192,
            metadata_entries: 8,
            block_bytes: 4096,
            records_per_block: 8,
        },
        AvroDatumLimits {
            depth: 64,
            values: 1000,
            value_bytes: 1024,
        },
        4096,
    )
    .await
}

#[tokio::test]
async fn selected_snapshot_accepts_official_writer_headers_and_optional_absence() {
    for (version, label) in [
        (ManifestVersion::V1, "1"),
        (ManifestVersion::V2, "2"),
        (ManifestVersion::V3, "3"),
    ] {
        let mut metadata = vec![
            ("format-version", label),
            ("snapshot-id", "99"),
            ("parent-snapshot-id", "null"),
        ];
        if version != ManifestVersion::V1 {
            metadata.push(("sequence-number", "8"));
        }
        if version == ManifestVersion::V3 {
            metadata.push(("first-row-id", "100"));
        }
        for headers in [metadata.as_slice(), &[]] {
            let (store, record) = stored_metadata(false, false, headers).await;
            let scope = selection(&record, version);
            let mut reader = selected(store, record, scope).await.unwrap();
            for _ in 0..4 {
                assert!(reader.next_entry().await.unwrap().is_some());
                assert!(!reader.is_complete());
            }
            assert!(reader.next_entry().await.unwrap().is_none());
            assert!(reader.is_complete());
        }
    }
}

#[tokio::test]
async fn selected_snapshot_rejects_header_mismatch_even_for_empty_lists() {
    for metadata in [
        ("format-version", "2"),
        ("format-version", "4"),
        ("snapshot-id", "98"),
        ("snapshot-id", "bad"),
        ("parent-snapshot-id", "12"),
        ("parent-snapshot-id", ""),
        ("sequence-number", "9"),
        ("first-row-id", "101"),
        ("sequence-number", "9223372036854775808"),
    ] {
        let (store, record) = stored_metadata(false, true, &[metadata]).await;
        let scope = selection(&record, ManifestVersion::V3);
        assert!(selected(store, record, scope).await.is_err(), "{metadata:?}");
    }
    let (store, record) = stored_metadata(false, true, &[("parent-snapshot-id", "42")]).await;
    let mut scope = selection(&record, ManifestVersion::V3);
    scope.parent_snapshot_id = Some(42);
    assert!(selected(store, record, scope).await.is_ok());
}

#[tokio::test]
async fn invalid_snapshot_scope_is_rejected_before_canonical_reads() {
    let (store, record) = stored(false, true).await;
    for invalid in 0..8 {
        let mut scope = selection(&record, ManifestVersion::V3);
        match invalid {
            0 => scope.sequence = -1,
            1 => scope.parent_snapshot_id = Some(scope.snapshot_id),
            2 => scope.first_row_id = None,
            3 => scope.added_rows = None,
            4 => scope.first_row_id = Some(-1),
            5 => scope.added_rows = Some(-1),
            6 => scope.first_row_id = Some(i64::MAX),
            _ => scope.table_version = ManifestVersion::V1,
        }
        let before = store.reads.load(Ordering::SeqCst);
        assert!(selected(store.clone(), record.clone(), scope).await.is_err());
        assert_eq!(store.reads.load(Ordering::SeqCst), before);
    }
}

#[tokio::test]
async fn snapshot_sequence_checks_distinguish_new_and_reused_manifests() {
    for (snapshot, sequence, accepted) in [(99, 7, false), (99, 9, false), (100, 7, false), (100, 9, true)] {
        let (store, record) = stored(false, false).await;
        let mut scope = selection(&record, ManifestVersion::V3);
        scope.snapshot_id = snapshot;
        scope.sequence = sequence;
        let mut reader = selected(store, record, scope).await.unwrap();
        assert_eq!(reader.next_entry().await.is_ok(), accepted);
        if !accepted {
            assert!(!reader.is_complete());
            assert!(reader.next_entry().await.is_err());
        }
    }
}

#[tokio::test]
async fn upgraded_snapshot_reads_old_canonical_lists_with_or_without_writer_headers() {
    for writer in [ManifestVersion::V1, ManifestVersion::V2] {
        for table in [ManifestVersion::V2, ManifestVersion::V3] {
            let label = if writer == ManifestVersion::V1 { "1" } else { "2" };
            let metadata = [("format-version", label), ("snapshot-id", "99")];
            for headers in [metadata.as_slice(), &[]] {
                let mut fixture = fixture::TestManifestList::new();
                if writer == ManifestVersion::V1 {
                    fixture.fields.truncate(4);
                } else {
                    fixture.fields.retain(|(id, _, _)| *id != 520);
                }
                let (store, record) = stored_layout(false, false, headers, fixture).await;
                let mut scope = selection(&record, writer);
                scope.table_version = table;
                let mut reader = selected(store, record, scope).await.unwrap();
                for _ in 0..4 {
                    let entry = reader.next_entry().await.unwrap().unwrap();
                    assert_eq!(entry.sequence, if writer == ManifestVersion::V1 { 0 } else { 8 });
                    assert_eq!(entry.first_row_id, None);
                    if writer == ManifestVersion::V1 {
                        assert_eq!(entry.file_counts, [None; 3]);
                    }
                }
                assert!(reader.next_entry().await.unwrap().is_none());
                assert!(reader.is_complete());
            }
        }
    }
}
