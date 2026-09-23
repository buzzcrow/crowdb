use super::{blocks::TestBlocks, fixtures};
#[path = "manifest_entry.rs"]
mod entry_fixture;
use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    AvroDatumLimits, ContentFormat, DeletionVectorLimits, DeletionVectorReference, FileContent, FileIdentity,
    FileKind, FileRecord, FileTreeWriter,
};
use crowdb_access_iceberg::key::FileId;
use crowdb_access_iceberg::manifest::{
    ManifestContent, ManifestContext, ManifestEntryProjection, ManifestEntryState, ManifestScalarEntry,
    ManifestVersion, SnapshotDvLimits, SnapshotDvScope, SnapshotFile,
};
use std::sync::Arc;

pub struct TestDvPair {
    pub vector: FileRecord,
    pub data: FileRecord,
    pub vector_entry: ManifestScalarEntry,
    pub data_entry: ManifestScalarEntry,
    pub vector_context: ManifestContext,
    pub data_context: ManifestContext,
    pub scope: SnapshotDvScope,
}

impl TestDvPair {
    pub fn vector(&self) -> SnapshotFile<'_> {
        SnapshotFile {
            entry: &self.vector_entry,
            record: &self.vector,
            context: &self.vector_context,
        }
    }
    pub fn data(&self) -> SnapshotFile<'_> {
        SnapshotFile {
            entry: &self.data_entry,
            record: &self.data,
            context: &self.data_context,
        }
    }
}

pub fn limits() -> SnapshotDvLimits {
    SnapshotDvLimits {
        vectors: 100,
        blob_bytes: 1024 * 1024,
        vector: DeletionVectorLimits {
            blob_bytes: 1024 * 1024,
            bitmaps: 10,
        },
    }
}

pub async fn pair(store: Arc<TestBlocks>, blob: &[u8], count: u64, rows: i64) -> TestDvPair {
    let (record, reference) = fixtures::record(store.clone(), blob, count).await;
    from_reference(store, record, reference, rows).await
}

pub async fn from_reference(
    store: Arc<TestBlocks>,
    vector: FileRecord,
    reference: DeletionVectorReference,
    rows: i64,
) -> TestDvPair {
    let table = vector.location.table();
    let owner = FileIdentity {
        table,
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(store, owner, 64).unwrap();
    writer.push(b"canonical data bytes").await.unwrap();
    let tree = writer.finish().await.unwrap();
    let data = FileRecord {
        file: owner.file,
        location: reference.referenced.clone(),
        kind: FileKind::Data,
        format: ContentFormat::Parquet,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
    let context = ManifestContext::parse(
        ManifestVersion::V3,
        0,
        0,
        br#"{"type":"struct","schema-id":0,"fields":[]}"#,
        b"[]",
    )
    .unwrap();
    let vector_entry = entry(
        &vector,
        Some(&reference),
        i64::try_from(reference.cardinality).unwrap(),
        &context,
    );
    let data_entry = entry(&data, None, rows, &context);
    TestDvPair {
        vector,
        data,
        vector_entry,
        data_entry,
        vector_context: context.clone(),
        data_context: context,
        scope: SnapshotDvScope {
            context: CatalogContext {
                catalog: table.catalog,
                activation_epoch: 1,
            },
            table,
            snapshot_id: 99,
            sequence: 9,
            manifest_list: FileId::random(),
        },
    }
}

fn entry(
    record: &FileRecord,
    reference: Option<&DeletionVectorReference>,
    record_count: i64,
    context: &ManifestContext,
) -> ManifestScalarEntry {
    use serde_json::json;
    let mut fixture = entry_fixture::TestManifestEntry::new(ManifestVersion::V3);
    fixture.set(3, json!(9));
    fixture.set(4, json!(9));
    fixture.set(100, json!(record.location.to_string()));
    fixture.set(101, json!(if reference.is_some() { "PUFFIN" } else { "PARQUET" }));
    fixture.set(103, json!(record_count));
    fixture.set(104, json!(record.length));
    if let Some(reference) = reference {
        fixture.set(134, json!(1));
        fixture.set(143, json!(reference.referenced.to_string()));
        fixture.set(144, json!(reference.span.offset));
        fixture.set(145, json!(reference.span.length));
    }
    let schema = fixture.schema();
    let projection =
        ManifestEntryProjection::with_context(&schema, ManifestVersion::V3, record.location.table(), context)
            .unwrap();
    let mut state = ManifestEntryState::new(
        ManifestVersion::V3,
        record.location.table(),
        if reference.is_some() {
            ManifestContent::Deletes
        } else {
            ManifestContent::Data
        },
        99,
        9,
        None,
    )
    .unwrap();
    projection
        .records(
            &fixture.bytes(),
            1,
            AvroDatumLimits {
                depth: 64,
                values: 1000,
                value_bytes: 4096,
            },
            &mut state,
        )
        .unwrap()
        .next_entry()
        .unwrap()
        .unwrap()
}
