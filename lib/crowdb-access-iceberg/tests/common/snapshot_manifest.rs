use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::file::{
    AvroDatumLimits, AvroLimits, ContentFormat, FileContent, FileIdentity, FileKind, FileLocation,
    FileReader, FileRecord, FileTreeWriter,
};
use crowdb_access_iceberg::key::FileId;
use crowdb_access_iceberg::manifest::{
    ManifestContext, ManifestListSelection, ManifestVersion, SnapshotManifestError, SnapshotManifestLimits,
    SnapshotManifestSource,
};

use super::{blocks::TestBlocks, list_fixture::TestManifestList, stream};

pub struct TestSource {
    pub records: Vec<FileRecord>,
    pub calls: AtomicUsize,
    pub unavailable: AtomicBool,
    pub pause: AtomicBool,
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[async_trait]
impl SnapshotManifestSource for TestSource {
    async fn resolve(
        &self,
        location: &FileLocation,
    ) -> Result<(FileRecord, ManifestContext), SnapshotManifestError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.pause.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(SnapshotManifestError::Unavailable);
        }
        let record = self
            .records
            .iter()
            .find(|record| &record.location == location)
            .ok_or(SnapshotManifestError::Unavailable)?
            .clone();
        Ok((record, stream::context(ManifestVersion::V3)))
    }
}

pub fn limits() -> SnapshotManifestLimits {
    SnapshotManifestLimits {
        framing: AvroLimits {
            header_bytes: 8192,
            metadata_entries: 8,
            block_bytes: 4096,
            records_per_block: 8,
        },
        datum: AvroDatumLimits {
            depth: 64,
            values: 1000,
            value_bytes: 1024,
        },
        decoded_bytes: 4096,
        manifests: 2,
        entries: 4,
        manifest_bytes: 32 * 1024,
    }
}

pub async fn stored(
    count: usize,
    corrupt: bool,
    wrong_totals: bool,
) -> (
    Arc<TestBlocks>,
    Arc<TestSource>,
    FileRecord,
    ManifestListSelection,
) {
    stored_impl(count, corrupt, wrong_totals, None, 99).await
}

pub async fn stored_with_lineage(
    first_rows: &[Option<i64>],
    added_snapshot_id: i64,
) -> (
    Arc<TestBlocks>,
    Arc<TestSource>,
    FileRecord,
    ManifestListSelection,
) {
    stored_impl(
        first_rows.len(),
        false,
        false,
        Some(first_rows),
        added_snapshot_id,
    )
    .await
}

async fn stored_impl(
    count: usize,
    corrupt: bool,
    wrong_totals: bool,
    first_rows: Option<&[Option<i64>]>,
    added_snapshot_id: i64,
) -> (
    Arc<TestBlocks>,
    Arc<TestSource>,
    FileRecord,
    ManifestListSelection,
) {
    let (store, mut record) = stream::stored(ManifestVersion::V3, false, corrupt).await;
    record.kind = FileKind::Unbound;
    let mut fixture = TestManifestList::new();
    fixture.set(503, serde_json::json!(added_snapshot_id));
    for (id, value) in [
        (501, i64::try_from(record.length).unwrap()),
        (515, 9),
        (516, 9),
        (504, 2),
        (505, 0),
        (506, 0),
        (512, 20),
        (513, 0),
        (514, 0),
    ] {
        fixture.set(id, serde_json::json!(value));
    }
    let schema = fixture.schema_bytes();
    let mut bytes = b"Obj\x01".to_vec();
    long(1, &mut bytes);
    sized(b"avro.schema", &mut bytes);
    sized(&schema, &mut bytes);
    bytes.push(0);
    bytes.extend([42; 16]);
    let mut records = Vec::new();
    for index in 0..count {
        let mut candidate = copy_record(store.clone(), record.clone()).await;
        candidate.location = candidate
            .location
            .table()
            .file(&format!("metadata/{index}.avro"))
            .unwrap();
        fixture.set(500, serde_json::json!(candidate.location.to_string()));
        fixture.set(
            520,
            serde_json::json!(
                first_rows.map_or(Some(100 + i64::try_from(index).unwrap() * 20), |rows| rows[index])
            ),
        );
        if wrong_totals && index + 1 == count {
            fixture.set(504, serde_json::json!(3));
            fixture.set(512, serde_json::json!(30));
        }
        long(1, &mut bytes);
        sized(&fixture.bytes(), &mut bytes);
        bytes.extend([42; 16]);
        records.push(candidate);
    }
    let owner = FileIdentity {
        table: record.location.table(),
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(store.clone(), owner, 97).unwrap();
    writer.push(&bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    let list = FileRecord {
        file: owner.file,
        location: owner.table.file("metadata/list.avro").unwrap(),
        kind: FileKind::Unbound,
        format: ContentFormat::Avro,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
    let selection = ManifestListSelection {
        location: list.location.clone(),
        table_version: ManifestVersion::V3,
        snapshot_id: 99,
        parent_snapshot_id: None,
        sequence: 9,
        first_row_id: Some(100),
        added_rows: Some(40),
    };
    let source = Arc::new(TestSource {
        records,
        calls: AtomicUsize::new(0),
        unavailable: AtomicBool::new(false),
        pause: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    (store, source, list, selection)
}

async fn copy_record(store: Arc<TestBlocks>, record: FileRecord) -> FileRecord {
    let mut reader = FileReader::new(store.clone(), record.clone(), None, 256).unwrap();
    let mut candidate = record;
    candidate.file = FileId::random();
    let owner = FileIdentity {
        table: candidate.location.table(),
        file: candidate.file,
    };
    let mut writer = FileTreeWriter::new(store, owner, 64).unwrap();
    while let Some(bytes) = reader.next().await.unwrap() {
        writer.push(&bytes).await.unwrap();
    }
    let tree = writer.finish().await.unwrap();
    candidate.content = FileContent::Chunks { root: tree.root };
    candidate
}

fn long(value: usize, bytes: &mut Vec<u8>) {
    let mut encoded = u64::try_from(value).unwrap() << 1;
    while encoded > 127 {
        bytes.push(u8::try_from(encoded & 127).unwrap() | 128);
        encoded >>= 7;
    }
    bytes.push(u8::try_from(encoded).unwrap());
}

fn sized(value: &[u8], bytes: &mut Vec<u8>) {
    long(value.len(), bytes);
    bytes.extend_from_slice(value);
}
