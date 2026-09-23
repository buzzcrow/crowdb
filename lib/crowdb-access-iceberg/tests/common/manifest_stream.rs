use super::{
    blocks::TestBlocks,
    fixture::{table, TestManifestEntry},
};
use crowdb_access_iceberg::file::{
    ContentFormat, FileContent, FileIdentity, FileKind, FileRecord, FileTreeWriter,
};
use crowdb_access_iceberg::key::FileId;
use crowdb_access_iceberg::manifest::{ManifestContent, ManifestContext, ManifestListEntry, ManifestVersion};
use std::{io::Write, sync::Arc};

pub fn context(version: ManifestVersion) -> ManifestContext {
    ManifestContext::parse(
        version,
        0,
        0,
        br#"{"type":"struct","schema-id":0,"fields":[{"id":3,"name":"v","required":false,"type":"long"}]}"#,
        b"[]",
    )
    .unwrap()
}

pub fn list(record: &FileRecord) -> ManifestListEntry {
    ManifestListEntry {
        partitions: None,
        location: record.location.clone(),
        length: record.length,
        partition_spec_id: 0,
        added_snapshot_id: 99,
        content: ManifestContent::Data,
        sequence: 9,
        min_sequence: 9,
        file_counts: [Some(2), Some(0), Some(0)],
        row_counts: [Some(20), Some(0), Some(0)],
        first_row_id: Some(100),
    }
}

pub async fn stored(version: ManifestVersion, deflate: bool, corrupt: bool) -> (Arc<TestBlocks>, FileRecord) {
    stored_with_count(version, deflate, corrupt, 1).await
}

pub async fn stored_with_count(
    version: ManifestVersion,
    deflate: bool,
    corrupt: bool,
    records_per_block: usize,
) -> (Arc<TestBlocks>, FileRecord) {
    stored_impl(version, deflate, corrupt, records_per_block, None).await
}

pub async fn stored_with_partitions(
    version: ManifestVersion,
    deflate: bool,
    values: [Option<f64>; 2],
) -> (Arc<TestBlocks>, FileRecord) {
    stored_impl(version, deflate, false, 1, Some(values)).await
}

async fn stored_impl(
    version: ManifestVersion,
    deflate: bool,
    corrupt: bool,
    records_per_block: usize,
    partitions: Option<[Option<f64>; 2]>,
) -> (Arc<TestBlocks>, FileRecord) {
    let mut fixture = TestManifestEntry::new(version);
    crowdb_access_iceberg::manifest::ManifestEntryProjection::new(&fixture.schema(), version, table())
        .unwrap();
    fixture.file.push((109, "long-map", serde_json::json!([[3, 10]])));
    if partitions.is_some() {
        fixture
            .partition_fields
            .push(serde_json::json!({"name":"p","field-id":1000,"type":["null","double"]}));
    }
    let mut bytes = b"Obj\x01".to_vec();
    let mut metadata=vec![
        ("avro.schema",fixture.schema_bytes()),
        ("avro.codec",if deflate {b"deflate".to_vec()} else {b"null".to_vec()}),
        ("schema",br#"{"type":"struct","schema-id":0,"fields":[{"id":3,"name":"v","required":false,"type":"long"}]}"#.to_vec()),
        ("partition-spec",b"[]".to_vec()),
    ];
    if partitions.is_some() {
        metadata[2].1 = br#"{"type":"struct","schema-id":0,"fields":[{"id":3,"name":"v","required":false,"type":"double"}]}"#.to_vec();
        metadata[3].1 = br#"[{"source-id":3,"field-id":1000,"name":"p","transform":"identity"}]"#.to_vec();
    }
    if version != ManifestVersion::V1 {
        metadata.extend([
            (
                "format-version",
                if version == ManifestVersion::V2 {
                    b"2".to_vec()
                } else {
                    b"3".to_vec()
                },
            ),
            ("schema-id", b"0".to_vec()),
            ("partition-spec-id", b"0".to_vec()),
            ("content", b"data".to_vec()),
        ]);
    }
    long(i64::try_from(metadata.len()).unwrap(), &mut bytes);
    for (key, value) in metadata {
        sized(key.as_bytes(), &mut bytes);
        sized(&value, &mut bytes);
    }
    bytes.push(0);
    bytes.extend([42; 16]);
    for index in 0..2 {
        if let Some(values) = partitions {
            fixture.partition_bytes = match values[index] {
                None => vec![0],
                Some(value) => {
                    let mut bytes = vec![2];
                    bytes.extend(value.to_le_bytes());
                    bytes
                }
            };
        }
        if corrupt && index == 1 {
            fixture.set(109, serde_json::json!([[4, 10]]));
        }
        let mut payload = fixture.bytes().repeat(records_per_block);
        if deflate {
            let mut encoder = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(&payload).unwrap();
            payload = encoder.finish().unwrap();
        }
        long(i64::try_from(records_per_block).unwrap(), &mut bytes);
        sized(&payload, &mut bytes);
        bytes.extend([42; 16]);
    }
    let store = Arc::new(TestBlocks::default());
    let owner = FileIdentity {
        table: table(),
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(store.clone(), owner, 64).unwrap();
    writer.push(&bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    let record = FileRecord {
        file: owner.file,
        location: table().file("metadata/m.avro").unwrap(),
        kind: FileKind::Manifest,
        format: ContentFormat::Avro,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
    (store, record)
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
