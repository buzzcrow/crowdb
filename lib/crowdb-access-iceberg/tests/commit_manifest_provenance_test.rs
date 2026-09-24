#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/manifest_entry.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/manifest_list.rs"]
#[allow(dead_code)]
mod list_fixture;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
#[path = "common/namespace.rs"]
#[allow(dead_code)]
mod namespaces;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod parquet;
#[path = "common/snapshot_files.rs"]
mod snapshot;

use crowdb_access_iceberg::{
    catalog::{CatalogContext, RootState},
    commit::{PriorManifestLimits, PriorManifestSource},
    file::{ContentFormat, FileContent, FileKind, FileRecord, FileRepository},
    manifest::{SnapshotManifestSource, SnapshotValidationInput},
    record::StorageRecord,
    table::{head_key, SelectedTable, TableMetadataDocument},
};
use serde_json::json;
use std::sync::{atomic::Ordering, Arc};

struct TestPrior {
    namespace: namespaces::TestNamespace,
    blocks: Arc<blocks::TestBlocks>,
    selected: SelectedTable,
    document: TableMetadataDocument,
    input: SnapshotValidationInput,
    manifest: FileRecord,
}

impl TestPrior {
    async fn new() -> Self {
        let namespace = namespaces::TestNamespace {
            store: Arc::new(common::TestStore::default()),
            context: CatalogContext {
                catalog: fixture::table().catalog,
                activation_epoch: 1,
            },
        };
        namespace.root(namespace.context, RootState::Ready).await;
        let blocks = Arc::new(blocks::TestBlocks::default());
        let data = snapshot::data(blocks.clone(), "data/first.parquet").await;
        let input = snapshot::input(
            blocks.clone(),
            vec![vec![snapshot::entry(&data, 0, 10)]],
            vec![data],
        )
        .await;
        let (manifest, _) = input
            .manifests
            .resolve(&fixture::table().file("metadata/0.avro").unwrap())
            .await
            .unwrap();
        let files = FileRepository::new(namespace.store.clone());
        files.publish(namespace.context, &input.list).await.unwrap();
        files.publish(namespace.context, &manifest).await.unwrap();
        let mut value = metadata::metadata(2);
        value["schemas"] = json!([{"type":"struct","schema-id":1,"fields":[{"id":4,"name":"new","type":"string","required":false}]}]);
        value["current-schema-id"] = json!(1);
        value["last-column-id"] = json!(4);
        value["last-sequence-number"] = json!(9);
        let mut snapshot = metadata::snapshot(99, 9);
        snapshot["schema-id"] = json!(1);
        snapshot["manifest-list"] = json!(input.list.location.to_string());
        value["snapshots"] = json!([snapshot]);
        let bytes = serde_json::to_vec(&value).unwrap();
        let head = metadata::head(
            &bytes,
            2,
            Some(uuid::Uuid::parse_str(value["table-uuid"].as_str().unwrap()).unwrap()),
        );
        let document = TableMetadataDocument::parse(bytes, &head, metadata::limits()).unwrap();
        let record = FileRecord {
            file: head.metadata_file,
            location: head.metadata_location.clone(),
            kind: FileKind::Metadata,
            format: ContentFormat::Json,
            length: document.canonical().len() as u64,
            digest: head.metadata_digest,
            content: FileContent::select_inline(FileKind::Metadata, document.canonical()).unwrap(),
            hint: None,
        };
        files.publish(namespace.context, &record).await.unwrap();
        namespace
            .put(
                head_key(head.catalog, head.table),
                StorageRecord::TableHead(Box::new(head.clone())),
            )
            .await;
        Self {
            namespace,
            blocks,
            selected: SelectedTable {
                head,
                metadata: record,
            },
            document,
            input,
            manifest,
        }
    }

    async fn build(
        &self,
        limits: PriorManifestLimits,
    ) -> Result<PriorManifestSource, crowdb_access_iceberg::manifest::SnapshotManifestError> {
        PriorManifestSource::build(
            self.namespace.store.clone(),
            self.blocks.clone(),
            self.namespace.context,
            &self.selected,
            &self.document,
            limits,
        )
        .await
    }
}

fn limits() -> PriorManifestLimits {
    PriorManifestLimits {
        snapshots: 10,
        references: 10,
        index_bytes: 100_000,
        manifests: snapshot::limits().manifests,
    }
}

#[tokio::test]
async fn a_head_change_during_canonical_manifest_reads_discards_the_recovered_context() {
    let fixture = TestPrior::new().await;
    let source = fixture.build(limits()).await.unwrap();
    fixture.blocks.pause_reads.store(true, Ordering::SeqCst);
    let mut resolution = Box::pin(source.resolve(&fixture.manifest.location));
    tokio::select! {
        result = &mut resolution => panic!("read did not pause: {result:?}"),
        () = fixture.blocks.read_entered.notified() => {}
    }
    let mut head = fixture.selected.head.clone();
    head.operation_fence += 1;
    fixture
        .namespace
        .put(
            head_key(head.catalog, head.table),
            StorageRecord::TableHead(Box::new(head)),
        )
        .await;
    fixture.blocks.pause_reads.store(false, Ordering::SeqCst);
    fixture.blocks.read_release.notify_one();
    assert!(resolution.await.is_err());
}

#[tokio::test]
async fn reachable_manifest_recovers_expired_schema_but_uploads_cannot_self_authorize() {
    let fixture = TestPrior::new().await;
    assert!(fixture.document.manifest_context(0, 0, &[], 1000).is_err());
    let source = fixture.build(limits()).await.unwrap();
    assert_eq!(source.selected(), &fixture.selected);
    let (record, context) = source.resolve(&fixture.manifest.location).await.unwrap();
    assert_eq!(record.file, fixture.manifest.file);
    assert_eq!(context.schema_id(), 0);
    assert!(context.field(3).is_some());
    assert!(context.field(4).is_none());
    let mut reader = crowdb_access_iceberg::file::FileReader::new(
        fixture.blocks.clone(),
        fixture.manifest.clone(),
        None,
        8192,
    )
    .unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = reader.next().await.unwrap() {
        bytes.extend(chunk);
    }
    let upload = snapshot::store(
        fixture.blocks.clone(),
        "metadata/unselected.avro",
        ContentFormat::Avro,
        &bytes,
    )
    .await;
    FileRepository::new(fixture.namespace.store.clone())
        .publish(fixture.namespace.context, &upload)
        .await
        .unwrap();
    assert!(source.resolve(&upload.location).await.is_err());
    let mut reader = crowdb_access_iceberg::manifest::SnapshotManifestReader::open(
        fixture.blocks.clone(),
        Arc::new(source),
        fixture.input.list,
        fixture.input.selection,
        snapshot::limits().manifests,
    )
    .await
    .unwrap();
    assert!(reader.next_entry().await.unwrap().is_some());
    assert!(reader.next_entry().await.unwrap().is_none());
    assert_eq!(reader.finish().unwrap().entries, 1);
}

#[tokio::test]
async fn changed_head_and_retired_catalog_invalidate_prepared_provenance() {
    let fixture = TestPrior::new().await;
    let source = fixture.build(limits()).await.unwrap();
    let mut head = fixture.selected.head.clone();
    head.operation_fence += 1;
    fixture
        .namespace
        .put(
            head_key(head.catalog, head.table),
            StorageRecord::TableHead(Box::new(head)),
        )
        .await;
    assert!(source.resolve(&fixture.manifest.location).await.is_err());
    assert!(fixture.build(limits()).await.is_err());
    let fixture = TestPrior::new().await;
    let source = fixture.build(limits()).await.unwrap();
    let mut context = fixture.namespace.context;
    context.activation_epoch += 1;
    fixture.namespace.root(context, RootState::Ready).await;
    assert!(source.resolve(&fixture.manifest.location).await.is_err());
}

#[tokio::test]
async fn provenance_bounds_and_canonical_corruption_never_return_partial_authority() {
    let fixture = TestPrior::new().await;
    for limit in [
        PriorManifestLimits {
            snapshots: 0,
            ..limits()
        },
        PriorManifestLimits {
            references: 0,
            ..limits()
        },
        PriorManifestLimits {
            index_bytes: 1,
            ..limits()
        },
    ] {
        assert!(fixture.build(limit).await.is_err());
    }
    let mut limit = limits();
    limit.manifests.manifest_bytes = fixture.input.list.length;
    assert!(fixture.build(limit).await.is_err());
    let source = fixture.build(limits()).await.unwrap();
    fixture.blocks.corrupt_reads.store(true, Ordering::SeqCst);
    assert!(source.resolve(&fixture.manifest.location).await.is_err());
    assert!(fixture.build(limits()).await.is_err());
}
