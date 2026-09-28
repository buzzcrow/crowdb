use super::{blocks::TestBlocks, fixture::TestManifestEntry, list_fixture::TestManifestList, snapshot};
use async_trait::async_trait;
use crowdb_access_iceberg::{
    file::{ContentFormat, FileLocation, FileRecord},
    manifest::{
        ManifestContext, ManifestListSelection, ManifestVersion, SnapshotManifestError,
        SnapshotManifestReader, SnapshotManifestSource,
    },
    table::TableMetadataDocument,
};
use serde_json::json;
use std::sync::Arc;

struct TestSource {
    record: FileRecord,
    context: ManifestContext,
}

#[async_trait]
impl SnapshotManifestSource for TestSource {
    async fn resolve(
        &self,
        location: &FileLocation,
    ) -> Result<(FileRecord, ManifestContext), SnapshotManifestError> {
        if location != &self.record.location {
            return Err(SnapshotManifestError::Unavailable);
        }
        Ok((self.record.clone(), self.context.clone()))
    }
}

pub async fn reader(
    store: Arc<TestBlocks>,
    document: &TableMetadataDocument,
    entries: &[TestManifestEntry],
) -> SnapshotManifestReader {
    let schema = serde_json::to_vec(&document.fields()["schemas"][0]).unwrap();
    let partition = serde_json::to_vec(&document.fields()["partition-specs"][0]["fields"]).unwrap();
    let context = ManifestContext::parse(ManifestVersion::V2, 0, 0, &schema, &partition).unwrap();
    let bytes = snapshot::ocf(
        vec![
            ("avro.schema", entries[0].schema_bytes()),
            ("schema", schema),
            ("partition-spec", partition),
            ("schema-id", b"0".to_vec()),
            ("partition-spec-id", b"0".to_vec()),
            ("format-version", b"2".to_vec()),
            ("content", b"data".to_vec()),
        ],
        &entries.iter().map(TestManifestEntry::bytes).collect::<Vec<_>>(),
    );
    let record = snapshot::store(
        store.clone(),
        "metadata/manifest.avro",
        ContentFormat::Avro,
        &bytes,
    )
    .await;
    let mut reference = TestManifestList::new();
    reference.set(500, json!(record.location.to_string()));
    let count = i64::try_from(entries.len()).unwrap();
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
    for (id, value) in [
        (501, i64::try_from(record.length).unwrap()),
        (517, 0),
        (515, 9),
        (516, 9),
        (504, count),
        (505, 0),
        (506, 0),
        (512, rows),
        (513, 0),
        (514, 0),
    ] {
        reference.set(id, json!(value));
    }
    reference.set(520, json!(null));
    let bytes = snapshot::ocf(
        vec![("avro.schema", reference.schema_bytes())],
        &[reference.bytes()],
    );
    let list = snapshot::store(store.clone(), "metadata/list.avro", ContentFormat::Avro, &bytes).await;
    let selection = ManifestListSelection {
        location: list.location.clone(),
        table_version: ManifestVersion::V2,
        snapshot_id: 99,
        parent_snapshot_id: None,
        sequence: 9,
        first_row_id: None,
        added_rows: None,
    };
    SnapshotManifestReader::open(
        store,
        Arc::new(TestSource { record, context }),
        list,
        selection,
        snapshot::limits().manifests,
    )
    .await
    .unwrap()
}

pub fn entry(index: usize, partition: i64, records: i64) -> TestManifestEntry {
    let mut entry = TestManifestEntry::new(ManifestVersion::V2);
    entry.set(
        100,
        json!(super::fixture::table()
            .file(&format!("data/{index}.parquet"))
            .unwrap()
            .to_string()),
    );
    entry.set(103, json!(records));
    entry.set(104, json!(100));
    entry.partition_fields = vec![json!({"name":"part","field-id":1000,"type":["null","long"]})];
    entry.partition_bytes = vec![2];
    entry.partition_bytes.extend(super::parquet::number(partition));
    entry
}
