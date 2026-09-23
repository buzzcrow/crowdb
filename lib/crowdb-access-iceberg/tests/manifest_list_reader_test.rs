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
use crowdb_access_iceberg::manifest::{ManifestListProjection, ManifestListReader, ManifestVersion};

fn long(value: usize, output: &mut Vec<u8>) {
    let mut encoded = u64::try_from(value).unwrap() << 1;
    while encoded > 127 {
        output.push((encoded & 127) as u8 | 128);
        encoded >>= 7;
    }
    output.push(u8::try_from(encoded).unwrap());
}

async fn stored(corrupt: bool, empty: bool) -> (Arc<blocks::TestBlocks>, FileRecord) {
    let mut fixture = fixture::TestManifestList::new();
    let schema = fixture.schema_bytes();
    let mut bytes = b"Obj\x01".to_vec();
    long(1, &mut bytes);
    long(11, &mut bytes);
    bytes.extend(b"avro.schema");
    long(schema.len(), &mut bytes);
    bytes.extend(schema);
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
