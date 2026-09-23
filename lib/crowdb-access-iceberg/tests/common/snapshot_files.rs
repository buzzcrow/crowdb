use super::{
    blocks::TestBlocks,
    fixture::{table, TestManifestEntry},
    list_fixture::TestManifestList,
    parquet,
};
use async_trait::async_trait;
use crowdb_access_iceberg::{catalog::CatalogContext, file::*, key::FileId, manifest::*};
use serde_json::json;
use std::sync::Arc;

pub struct TestSource {
    pub manifests: Vec<FileRecord>,
    pub files: Vec<FileRecord>,
}

#[async_trait]
impl SnapshotManifestSource for TestSource {
    async fn resolve(
        &self,
        location: &FileLocation,
    ) -> Result<(FileRecord, ManifestContext), SnapshotManifestError> {
        let record = self
            .manifests
            .iter()
            .find(|record| &record.location == location)
            .ok_or(SnapshotManifestError::Unavailable)?
            .clone();
        Ok((
            record,
            ManifestContext::parse(ManifestVersion::V3, 0, 0, schema(), b"[]").unwrap(),
        ))
    }
}

#[async_trait]
impl SnapshotFileSource for TestSource {
    async fn resolve(&self, location: &FileLocation) -> Result<FileRecord, SnapshotValidationError> {
        self.files
            .iter()
            .find(|record| &record.location == location)
            .cloned()
            .ok_or(SnapshotValidationError::Unavailable)
    }
}

fn schema() -> &'static [u8] {
    br#"{"type":"struct","schema-id":0,"fields":[{"id":3,"name":"value","required":false,"type":"long"}]}"#
}

pub fn limits() -> SnapshotFileLimits {
    SnapshotFileLimits {
        manifests: SnapshotManifestLimits {
            framing: AvroLimits {
                header_bytes: 8192,
                metadata_entries: 10,
                block_bytes: 8192,
                records_per_block: 10,
            },
            datum: AvroDatumLimits {
                depth: 64,
                values: 2000,
                value_bytes: 2048,
            },
            decoded_bytes: 8192,
            manifests: 10,
            entries: 20,
            manifest_bytes: 100_000,
            identity: SnapshotIdentityLimits {
                keys: 100,
                key_bytes: 16_384,
            },
        },
        data_files: 10,
        index_bytes: 1024 * 1024,
        position_deletes: PositionDeleteLimits {
            metadata: parquet::limits(),
            page: ParquetPageLimits {
                bytes: 1024 * 1024,
                values: 1024,
                pages: 100,
            },
            rows: 1000,
        },
        vectors: SnapshotDvLimits {
            vectors: 10,
            blob_bytes: 100_000,
            vector: DeletionVectorLimits {
                blob_bytes: 100_000,
                bitmaps: 100,
            },
        },
        delete_rows: 1000,
    }
}

pub async fn store(store: Arc<TestBlocks>, path: &str, format: ContentFormat, bytes: &[u8]) -> FileRecord {
    let owner = FileIdentity {
        table: table(),
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(store, owner, 97).unwrap();
    writer.push(bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    FileRecord {
        file: owner.file,
        location: table().file(path).unwrap(),
        kind: FileKind::Unbound,
        format,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    }
}

pub async fn data(store: Arc<TestBlocks>, path: &str) -> FileRecord {
    let footer = parquet::structure(&parquet::footer());
    let mut bytes = b"PAR1".to_vec();
    bytes.resize(32, 0);
    bytes.extend(&footer);
    bytes.extend(u32::try_from(footer.len()).unwrap().to_le_bytes());
    bytes.extend(b"PAR1");
    store_file(store, path, &bytes).await
}

async fn store_file(blocks: Arc<TestBlocks>, path: &str, bytes: &[u8]) -> FileRecord {
    store(blocks, path, ContentFormat::Parquet, bytes).await
}

pub fn entry(record: &FileRecord, content: i64, rows: i64) -> TestManifestEntry {
    let mut entry = TestManifestEntry::new(ManifestVersion::V2);
    entry.set(100, json!(record.location.to_string()));
    entry.set(
        101,
        json!(match record.format {
            ContentFormat::Orc => "ORC",
            ContentFormat::Puffin => "PUFFIN",
            _ => "PARQUET",
        }),
    );
    entry.set(104, json!(record.length));
    entry.set(103, json!(rows));
    entry.set(134, json!(content));
    entry
}

pub async fn input(
    blocks: Arc<TestBlocks>,
    groups: Vec<Vec<TestManifestEntry>>,
    files: Vec<FileRecord>,
) -> SnapshotValidationInput {
    let mut manifests = Vec::new();
    let mut references = Vec::new();
    let list_schema = TestManifestList::new().schema_bytes();
    for (index, entries) in groups.iter().enumerate() {
        let (manifest, reference) = manifest(blocks.clone(), index, entries).await;
        references.push(reference);
        manifests.push(manifest);
    }
    finish_input(blocks, files, manifests, list_schema, references).await
}

async fn manifest(
    blocks: Arc<TestBlocks>,
    index: usize,
    entries: &[TestManifestEntry],
) -> (FileRecord, Vec<u8>) {
    let content = entries[0]
        .file
        .iter()
        .find(|field| field.0 == 134)
        .unwrap()
        .2
        .as_i64()
        .unwrap();
    let rows: i64 = entries
        .iter()
        .map(|entry| {
            entry
                .file
                .iter()
                .find(|field| field.0 == 103)
                .unwrap()
                .2
                .as_i64()
                .unwrap()
        })
        .sum();
    let bytes = ocf(
        vec![
            ("avro.schema", entries[0].schema_bytes()),
            ("schema", schema().to_vec()),
            ("partition-spec", b"[]".to_vec()),
            ("schema-id", b"0".to_vec()),
            ("partition-spec-id", b"0".to_vec()),
            (
                "format-version",
                if entries[0]
                    .file
                    .iter()
                    .any(|field| field.0 == 101 && field.2 == "PUFFIN")
                {
                    b"3".to_vec()
                } else {
                    b"2".to_vec()
                },
            ),
            (
                "content",
                if content == 0 {
                    b"data".to_vec()
                } else {
                    b"deletes".to_vec()
                },
            ),
        ],
        &entries.iter().map(TestManifestEntry::bytes).collect::<Vec<_>>(),
    );
    let manifest = store(
        blocks.clone(),
        &format!("metadata/{index}.avro"),
        ContentFormat::Avro,
        &bytes,
    )
    .await;
    let mut reference = TestManifestList::new();
    for (id, value) in [
        (501, i64::try_from(manifest.length).unwrap()),
        (517, i64::from(content != 0)),
        (515, 9),
        (
            516,
            entries
                .iter()
                .filter_map(|entry| {
                    entry
                        .root
                        .iter()
                        .find(|field| field.0 == 3)
                        .and_then(|field| field.2.as_i64())
                })
                .min()
                .unwrap_or(9),
        ),
        (504, i64::try_from(entries.len()).unwrap()),
        (505, 0),
        (506, 0),
        (512, rows),
        (513, 0),
        (514, 0),
    ] {
        reference.set(id, json!(value));
    }
    reference.set(500, json!(manifest.location.to_string()));
    reference.set(520, json!(null));
    (manifest, reference.bytes())
}

async fn finish_input(
    blocks: Arc<TestBlocks>,
    files: Vec<FileRecord>,
    manifests: Vec<FileRecord>,
    list_schema: Vec<u8>,
    references: Vec<Vec<u8>>,
) -> SnapshotValidationInput {
    let list = store(
        blocks,
        "metadata/list.avro",
        ContentFormat::Avro,
        &ocf(vec![("avro.schema", list_schema)], &references),
    )
    .await;
    let selection = ManifestListSelection {
        location: list.location.clone(),
        table_version: if files.iter().any(|file| file.format == ContentFormat::Puffin) {
            ManifestVersion::V3
        } else {
            ManifestVersion::V2
        },
        snapshot_id: 99,
        parent_snapshot_id: None,
        sequence: 9,
        first_row_id: None,
        added_rows: None,
    };
    let scope = SnapshotDvScope {
        context: CatalogContext {
            catalog: table().catalog,
            activation_epoch: 1,
        },
        table: table(),
        snapshot_id: 99,
        sequence: 9,
        manifest_list: list.file,
    };
    let source = Arc::new(TestSource { manifests, files });
    SnapshotValidationInput {
        scope,
        list,
        selection,
        manifests: source.clone(),
        files: source,
        mapping: None,
    }
}

fn ocf(metadata: Vec<(&str, Vec<u8>)>, records: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = b"Obj\x01".to_vec();
    long(i64::try_from(metadata.len()).unwrap(), &mut bytes);
    for (key, value) in metadata {
        sized(key.as_bytes(), &mut bytes);
        sized(&value, &mut bytes);
    }
    bytes.push(0);
    bytes.extend([42; 16]);
    for record in records {
        long(1, &mut bytes);
        sized(record, &mut bytes);
        bytes.extend([42; 16]);
    }
    bytes
}

fn long(value: i64, bytes: &mut Vec<u8>) {
    bytes.extend(parquet::number(value));
}
fn sized(value: &[u8], bytes: &mut Vec<u8>) {
    long(i64::try_from(value.len()).unwrap(), bytes);
    bytes.extend(value);
}
