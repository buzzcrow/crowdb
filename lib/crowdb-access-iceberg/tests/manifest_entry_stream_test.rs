#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/manifest_entry.rs"]
mod fixture;

use std::io::Write;
use std::sync::Arc;

use crowdb_access_iceberg::file::{
    AvroDatumLimits, AvroLimits, AvroRecords, ContentFormat, FileContent, FileIdentity, FileKind, FileRecord,
    FileTreeWriter,
};
use crowdb_access_iceberg::key::FileId;
use crowdb_access_iceberg::manifest::{
    ManifestContent, ManifestEntryProjection, ManifestEntryState, ManifestVersion,
};
use fixture::{table, TestManifestEntry};

fn limits() -> AvroDatumLimits {
    AvroDatumLimits {
        depth: 64,
        values: 1000,
        value_bytes: 1024,
    }
}

fn long(value: i64, bytes: &mut Vec<u8>) {
    let mut encoded = (value.unsigned_abs() << 1).wrapping_sub(u64::from(value < 0));
    while encoded > 127 {
        bytes.push(u8::try_from(encoded & 127).unwrap() | 128);
        encoded >>= 7;
    }
    bytes.push(u8::try_from(encoded).unwrap());
}

fn sized(value: &[u8], bytes: &mut Vec<u8>) {
    long(i64::try_from(value.len()).unwrap(), bytes);
    bytes.extend_from_slice(value);
}

async fn reader(version: ManifestVersion, deflate: bool, corrupt: bool) -> AvroRecords {
    let mut fixture = TestManifestEntry::new(version);
    ManifestEntryProjection::new(&fixture.schema(), version, table()).unwrap();
    let mut bytes = b"Obj\x01".to_vec();
    long(2, &mut bytes);
    sized(b"avro.schema", &mut bytes);
    sized(&fixture.schema_bytes(), &mut bytes);
    sized(b"avro.codec", &mut bytes);
    sized(if deflate { b"deflate" } else { b"null" }, &mut bytes);
    long(0, &mut bytes);
    bytes.extend([42; 16]);
    for index in 0..2 {
        if corrupt && index == 1 {
            fixture.set(103, serde_json::json!(-1));
        }
        let mut encoded = fixture.bytes();
        if deflate {
            let mut compressor =
                flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
            compressor.write_all(&encoded).unwrap();
            encoded = compressor.finish().unwrap();
        }
        long(1, &mut bytes);
        sized(&encoded, &mut bytes);
        bytes.extend([42; 16]);
    }
    let store = Arc::new(blocks::TestBlocks::default());
    let owner = FileIdentity {
        table: table(),
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(store.clone(), owner, 64).unwrap();
    writer.push(&bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    let record = FileRecord {
        file: owner.file,
        location: table().file("metadata/manifest.avro").unwrap(),
        kind: FileKind::Manifest,
        format: ContentFormat::Avro,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
    AvroRecords::open(
        store,
        record,
        AvroLimits {
            header_bytes: 8192,
            metadata_entries: 8,
            block_bytes: 4096,
            records_per_block: 8,
        },
        limits(),
        4096,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn manifest_inheritance_survives_leaf_and_avro_block_boundaries_for_each_writer_version() {
    for version in [ManifestVersion::V1, ManifestVersion::V2, ManifestVersion::V3] {
        for deflate in [false, true] {
            let mut reader = reader(version, deflate, false).await;
            let mut state =
                ManifestEntryState::new(version, table(), ManifestContent::Data, 50, 9, Some(100)).unwrap();
            for first in [100, 110] {
                let block = reader.next().await.unwrap().unwrap();
                let projection = ManifestEntryProjection::new(reader.schema(), version, table()).unwrap();
                let mut records = projection
                    .records(&block.bytes, block.records, limits(), &mut state)
                    .unwrap();
                assert_eq!(
                    records.next_entry().unwrap().unwrap().inherited.first_row_id,
                    Some(first)
                );
                assert!(records.next_entry().unwrap().is_none());
            }
            assert!(reader.next().await.unwrap().is_none());
            assert_eq!(state.next_row_id(), Some(120));
        }
    }
}

#[tokio::test]
async fn a_later_binary_valid_but_semantically_bad_block_never_consumes_row_ids() {
    for deflate in [false, true] {
        let version = ManifestVersion::V3;
        let mut reader = reader(version, deflate, true).await;
        let mut state =
            ManifestEntryState::new(version, table(), ManifestContent::Data, 50, 9, Some(100)).unwrap();
        for valid in [true, false] {
            let block = reader.next().await.unwrap().unwrap();
            let projection = ManifestEntryProjection::new(reader.schema(), version, table()).unwrap();
            let mut records = projection
                .records(&block.bytes, block.records, limits(), &mut state)
                .unwrap();
            assert_eq!(records.next_entry().is_ok(), valid);
            if !valid {
                assert!(records.next_entry().is_err());
            }
        }
        assert_eq!(state.next_row_id(), Some(110));
    }
}
