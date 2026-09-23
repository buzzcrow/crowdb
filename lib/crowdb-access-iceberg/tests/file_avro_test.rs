#[path = "common/file_blocks.rs"]
mod blocks;

use std::sync::{atomic::Ordering, Arc};

use blocks::TestBlocks;
use crowdb_access_iceberg::file::{
    AvroBlocks, AvroContainerError, AvroDatumLimits, AvroLimits, AvroRecords, ContentFormat, FileContent,
    FileIdentity, FileKind, FileRecord, FileTreeWriter, TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};

const SYNC: [u8; 16] = [42; 16];

fn datum_limits() -> AvroDatumLimits {
    AvroDatumLimits {
        depth: 16,
        values: 100,
        value_bytes: 1024,
    }
}

fn limits() -> AvroLimits {
    AvroLimits {
        header_bytes: 1024,
        metadata_entries: 8,
        block_bytes: 1024,
        records_per_block: 100,
    }
}

fn long(bytes: &mut Vec<u8>, value: i64) {
    let mut value =
        (u64::from_ne_bytes(value.to_ne_bytes()) << 1) ^ u64::from_ne_bytes((value >> 63).to_ne_bytes());
    while value >= 128 {
        bytes.push(u8::try_from(value & 127).unwrap() | 128);
        value >>= 7;
    }
    bytes.push(u8::try_from(value).unwrap());
}

fn sized(bytes: &mut Vec<u8>, value: &[u8]) {
    long(bytes, i64::try_from(value.len()).unwrap());
    bytes.extend_from_slice(value);
}

fn header(negative: bool) -> Vec<u8> {
    let mut entries = Vec::new();
    sized(&mut entries, b"avro.schema");
    sized(&mut entries, br#""long""#);
    sized(&mut entries, b"avro.codec");
    sized(&mut entries, b"null");
    let mut bytes = b"Obj\x01".to_vec();
    long(&mut bytes, if negative { -2 } else { 2 });
    if negative {
        long(&mut bytes, i64::try_from(entries.len()).unwrap());
    }
    bytes.extend(entries);
    long(&mut bytes, 0);
    bytes.extend(SYNC);
    bytes
}

fn block(bytes: &mut Vec<u8>, count: i64, encoded: &[u8]) {
    long(bytes, count);
    sized(bytes, encoded);
    bytes.extend(SYNC);
}

async fn record(store: Arc<TestBlocks>, bytes: &[u8]) -> FileRecord {
    let owner = FileIdentity {
        table: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(store, owner, 7).unwrap();
    writer.push(bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    FileRecord {
        file: owner.file,
        location: owner.table.file("manifest.avro").unwrap(),
        kind: FileKind::Manifest,
        format: ContentFormat::Avro,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    }
}

#[tokio::test]
async fn avro_blocks_stream_positive_and_negative_metadata_maps_without_retaining_entries() {
    let store = Arc::new(TestBlocks::default());
    for negative in [false, true] {
        let mut bytes = header(negative);
        let header_length = bytes.len();
        for value in 0..300 {
            let mut encoded = Vec::new();
            long(&mut encoded, value);
            block(&mut bytes, 1, &encoded);
        }
        let record = record(store.clone(), &bytes).await;
        let mut reader = AvroBlocks::open(store.clone(), record, limits()).await.unwrap();
        assert_eq!(reader.codec(), "null");
        assert_eq!(reader.metadata()["avro.schema"], br#""long""#);
        assert_eq!(reader.header_hint().length, header_length as u64);
        let reads = store.reads.load(Ordering::SeqCst);
        tokio::task::yield_now().await;
        assert_eq!(reads, store.reads.load(Ordering::SeqCst));
        for value in 0..300 {
            let actual = reader.next().await.unwrap().unwrap();
            let mut expected = Vec::new();
            long(&mut expected, value);
            assert_eq!(actual.records, 1);
            assert_eq!(actual.encoded, expected);
            assert_eq!(
                &bytes[usize::try_from(actual.payload.offset).unwrap()..]
                    [..usize::try_from(actual.payload.length).unwrap()],
                expected
            );
        }
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn avro_container_bounds_are_independent_and_checked_before_payload_allocation() {
    let store = Arc::new(TestBlocks::default());
    let mut bytes = header(false);
    block(&mut bytes, 2, &[2, 4]);
    let record = record(store.clone(), &bytes).await;
    for bounds in [
        AvroLimits {
            header_bytes: 20,
            ..limits()
        },
        AvroLimits {
            metadata_entries: 1,
            ..limits()
        },
        AvroLimits {
            block_bytes: 0,
            ..limits()
        },
        AvroLimits {
            records_per_block: 1_000_001,
            ..limits()
        },
    ] {
        assert!(AvroBlocks::open(store.clone(), record.clone(), bounds)
            .await
            .is_err());
    }
    for bounds in [
        AvroLimits {
            block_bytes: 1,
            ..limits()
        },
        AvroLimits {
            records_per_block: 1,
            ..limits()
        },
    ] {
        let mut reader = AvroBlocks::open(store.clone(), record.clone(), bounds)
            .await
            .unwrap();
        assert!(matches!(reader.next().await, Err(AvroContainerError::Bounds)));
        assert!(matches!(reader.next().await, Err(AvroContainerError::Failed)));
    }
}

#[tokio::test]
async fn avro_container_rejects_corrupt_headers_sync_truncation_and_overflow() {
    let store = Arc::new(TestBlocks::default());
    let mut wrong_map_size = header(true);
    wrong_map_size[5] += 2;
    for bytes in [
        b"Obj\x02".to_vec(),
        b"Obj\x01\0".to_vec(),
        wrong_map_size,
        [b"Obj\x01".as_slice(), &[255; 10]].concat(),
    ] {
        let record = record(store.clone(), &bytes).await;
        assert!(AvroBlocks::open(store.clone(), record, limits()).await.is_err());
    }
    for suffix in [
        vec![128],
        vec![255; 10],
        vec![1, 0],
        vec![2, 127],
        vec![2, 0, 0],
        [vec![2, 0], vec![43; 16]].concat(),
    ] {
        let mut bytes = header(false);
        bytes.extend(suffix);
        let record = record(store.clone(), &bytes).await;
        let mut reader = AvroBlocks::open(store.clone(), record, limits()).await.unwrap();
        assert!(reader.next().await.is_err());
        assert!(matches!(reader.next().await, Err(AvroContainerError::Failed)));
    }
}

#[tokio::test]
async fn avro_container_accepts_empty_files_and_zero_byte_null_blocks() {
    let store = Arc::new(TestBlocks::default());
    let mut bytes = header(false);
    let schema = bytes.windows(4).position(|bytes| bytes == b"long").unwrap();
    bytes[schema..schema + 4].copy_from_slice(b"null");
    let record_empty = record(store.clone(), &bytes).await;
    let mut reader = AvroBlocks::open(store.clone(), record_empty, limits())
        .await
        .unwrap();
    assert!(reader.next().await.unwrap().is_none());
    block(&mut bytes, 100, &[]);
    let record = record(store.clone(), &bytes).await;
    let mut reader = AvroBlocks::open(store, record, limits()).await.unwrap();
    let block = reader.next().await.unwrap().unwrap();
    assert_eq!(block.records, 100);
    assert!(block.encoded.is_empty());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn cancelled_avro_block_read_cannot_resume_at_a_partial_record_boundary() {
    let store = Arc::new(TestBlocks::default());
    let mut bytes = header(false);
    block(&mut bytes, 1, &[2; 100]);
    let record = record(store.clone(), &bytes).await;
    let mut reader = AvroBlocks::open(store.clone(), record, limits()).await.unwrap();
    store.pause_reads.store(true, Ordering::SeqCst);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        tokio::select! {
            result = reader.next() => panic!("read unexpectedly completed: {result:?}"),
            () = store.read_entered.notified() => {}
        }
    })
    .await
    .unwrap();
    assert!(matches!(reader.next().await, Err(AvroContainerError::Failed)));
}

#[tokio::test]
async fn container_record_reader_validates_its_own_writer_schema_and_stops_after_corrupt_data() {
    let store = Arc::new(TestBlocks::default());
    let mut bytes = header(false);
    block(&mut bytes, 2, &[2, 4]);
    block(&mut bytes, 1, &[2, 4]);
    let record = record(store.clone(), &bytes).await;
    let mut reader = AvroRecords::open(store.clone(), record, limits(), datum_limits(), 1024)
        .await
        .unwrap();
    assert_eq!(reader.metadata()["avro.schema"], br#""long""#);
    assert_eq!(reader.header_hint().offset, 0);
    let first = reader.next().await.unwrap().unwrap();
    assert_eq!(first.records, 2);
    assert_eq!(first.bytes, [2, 4]);
    let reads = store.reads.load(Ordering::SeqCst);
    tokio::task::yield_now().await;
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    assert!(matches!(reader.next().await, Err(AvroContainerError::Schema)));
    assert!(matches!(reader.next().await, Err(AvroContainerError::Failed)));
}

#[tokio::test]
async fn container_record_reader_rejects_bad_schema_limits_and_cancelled_partial_blocks() {
    let store = Arc::new(TestBlocks::default());
    let mut invalid = header(false);
    let schema = invalid.windows(4).position(|bytes| bytes == b"long").unwrap();
    invalid[schema..schema + 4].copy_from_slice(b"oops");
    let invalid = record(store.clone(), &invalid).await;
    assert!(matches!(
        AvroRecords::open(store.clone(), invalid, limits(), datum_limits(), 1024).await,
        Err(AvroContainerError::Schema)
    ));
    let mut bytes = header(false);
    block(&mut bytes, 100, &[2; 100]);
    let record = record(store.clone(), &bytes).await;
    assert!(matches!(
        AvroRecords::open(store.clone(), record.clone(), limits(), datum_limits(), 0).await,
        Err(AvroContainerError::Bounds)
    ));
    let mut reader = AvroRecords::open(store.clone(), record, limits(), datum_limits(), 1024)
        .await
        .unwrap();
    store.pause_reads.store(true, Ordering::SeqCst);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        tokio::select! {
            result = reader.next() => panic!("read unexpectedly completed: {result:?}"),
            () = store.read_entered.notified() => {}
        }
    })
    .await
    .unwrap();
    assert!(matches!(reader.next().await, Err(AvroContainerError::Failed)));
}
