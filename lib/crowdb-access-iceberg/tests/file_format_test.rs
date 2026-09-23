#[path = "common/file_blocks.rs"]
mod blocks;

use std::sync::{atomic::Ordering, Arc};

use blocks::TestBlocks;
use crowdb_access_iceberg::file::{
    probe_parquet_footer, probe_puffin_footer, ContentFormat, FileContent, FileIdentity, FileKind,
    FileRecord, FileTreeWriter, FormatHint, TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};

async fn record(store: Arc<TestBlocks>, bytes: &[u8], block_size: usize) -> FileRecord {
    let owner = FileIdentity {
        table: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(store, owner, block_size).unwrap();
    writer.push(bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    FileRecord {
        file: owner.file,
        location: owner.table.file("data.parquet").unwrap(),
        kind: FileKind::Data,
        format: ContentFormat::Parquet,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    }
}

#[tokio::test]
async fn puffin_probe_checks_both_footer_magics_flags_and_signed_lengths() {
    let store = Arc::new(TestBlocks::default());
    let original = b"PFA1blobPFA1{}\x02\0\0\0\0\0\0\0PFA1".to_vec();
    for compressed in [false, true] {
        let mut bytes = original.clone();
        bytes[18] = u8::from(compressed);
        let mut record = record(store.clone(), &bytes, 3).await;
        record.kind = FileKind::Statistics;
        record.format = ContentFormat::Puffin;
        let footer = probe_puffin_footer(store.clone(), &record).await.unwrap();
        assert_eq!(
            footer.payload,
            FormatHint {
                offset: 12,
                length: 2
            }
        );
        assert_eq!(footer.compressed, compressed);
    }
    for (offset, replacement) in [
        (0, b'x'),
        (8, b'x'),
        (22, b'x'),
        (14, 0),
        (14, 255),
        (17, 128),
        (18, 2),
        (19, 1),
        (20, 1),
        (21, 1),
    ] {
        let mut bytes = original.clone();
        bytes[offset] = replacement;
        let mut record = record(store.clone(), &bytes, 3).await;
        record.kind = FileKind::Statistics;
        record.format = ContentFormat::Puffin;
        assert!(probe_puffin_footer(store.clone(), &record).await.is_err());
    }
}

#[tokio::test]
async fn parquet_probe_uses_canonical_framing_across_leaf_boundaries() {
    let store = Arc::new(TestBlocks::default());
    let bytes = b"PAR1payloadfooter\x06\0\0\0PAR1";
    let mut record = record(store.clone(), bytes, 3).await;
    let expected = FormatHint {
        offset: 11,
        length: 6,
    };
    for hint in [
        None,
        Some(expected),
        Some(FormatHint { offset: 4, length: 1 }),
        Some(FormatHint {
            offset: u64::MAX,
            length: u64::MAX,
        }),
    ] {
        record.hint = hint;
        assert_eq!(
            probe_parquet_footer(store.clone(), &record).await.unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn parquet_probe_rejects_bad_magic_lengths_and_truncated_containers() {
    let store = Arc::new(TestBlocks::default());
    for bytes in [
        b"PAR1".as_slice(),
        b"PAR1x\0\0\0\0PAR1",
        b"PAR1x\xff\xff\xff\xffPAR1",
        b"PAR1x\x02\0\0\0PAR1",
        b"PAREf\x01\0\0\0PARE",
        b"PAR1f\x01\0\0\0nope",
        b"nopef\x01\0\0\0PAR1",
    ] {
        let record = record(store.clone(), bytes, 5).await;
        assert!(probe_parquet_footer(store.clone(), &record).await.is_err());
    }
}

#[tokio::test]
async fn parquet_probe_does_not_read_or_allocate_the_advertised_footer() {
    let store = Arc::new(TestBlocks::default());
    let mut bytes = vec![0; 1024 * 1024];
    bytes[..4].copy_from_slice(b"PAR1");
    let length = bytes.len();
    bytes[length - 8..length - 4].copy_from_slice(&900_000_u32.to_le_bytes());
    bytes[length - 4..].copy_from_slice(b"PAR1");
    let record = record(store.clone(), &bytes, 4096).await;
    store.reads.store(0, Ordering::SeqCst);
    assert_eq!(
        probe_parquet_footer(store.clone(), &record).await.unwrap(),
        FormatHint {
            offset: length as u64 - 8 - 900_000,
            length: 900_000
        }
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), 4);
}
