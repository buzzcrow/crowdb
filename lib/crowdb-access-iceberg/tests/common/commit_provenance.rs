use crate::{blocks, common, fixture, metadata, namespaces, snapshot};
use crowdb_access_iceberg::{
    catalog::{CatalogContext, RootState},
    commit::{PriorManifestLimits, PriorManifestSource},
    file::{ContentFormat, FileContent, FileKind, FileRecord, FileRepository},
    manifest::SnapshotValidationInput,
    record::StorageRecord,
    table::{head_key, SelectedTable, TableMetadataDocument},
};
use serde_json::json;
use std::sync::Arc;

pub struct TestPrior {
    pub namespace: namespaces::TestNamespace,
    pub blocks: Arc<blocks::TestBlocks>,
    pub selected: SelectedTable,
    pub document: TableMetadataDocument,
    pub input: SnapshotValidationInput,
    pub manifest: FileRecord,
}

impl TestPrior {
    pub async fn legacy() -> Self {
        let mut fixture = Self::new().await;
        let (blocks, manifest) =
            crate::stream::stored(crowdb_access_iceberg::manifest::ManifestVersion::V1, false, false).await;
        fixture.blocks = blocks;
        let files = FileRepository::new(fixture.namespace.store.clone());
        files.publish(fixture.namespace.context, &manifest).await.unwrap();
        let mut value = serde_json::Value::Object(fixture.document.fields().clone());
        value["last-sequence-number"] = json!(0);
        value["snapshots"][0]["sequence-number"] = json!(0);
        value["snapshots"][0]
            .as_object_mut()
            .unwrap()
            .remove("manifest-list");
        value["snapshots"][0]["manifests"] = json!([manifest.location.to_string()]);
        let bytes = serde_json::to_vec(&value).unwrap();
        let mut head = metadata::head(&bytes, 2, fixture.selected.head.table_uuid);
        head.metadata_location = fixture::table().file("metadata/legacy.json").unwrap();
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
        files.publish(fixture.namespace.context, &record).await.unwrap();
        fixture
            .namespace
            .put(
                head_key(head.catalog, head.table),
                StorageRecord::TableHead(Box::new(head.clone())),
            )
            .await;
        fixture.selected = SelectedTable {
            head,
            metadata: record,
        };
        fixture.document = document;
        fixture.manifest = manifest;
        fixture
    }

    pub fn candidate(&self, restore_writer_schema: bool) -> Arc<TableMetadataDocument> {
        let mut value = serde_json::Value::Object(self.document.fields().clone());
        if restore_writer_schema {
            value["schemas"].as_array_mut().unwrap().push(json!({"type":"struct","schema-id":0,"fields":[{"id":3,"name":"value","required":false,"type":"long"}]}));
        }
        self.candidate_value(&value)
    }

    pub fn candidate_value(&self, value: &serde_json::Value) -> Arc<TableMetadataDocument> {
        use sha2::{Digest, Sha256};
        let bytes = serde_json::to_vec(value).unwrap();
        let mut head = self.selected.head.clone();
        head.generation += 1;
        head.metadata_file = crowdb_access_iceberg::key::FileId::random();
        head.metadata_location = fixture::table().file("metadata/two.json").unwrap();
        head.metadata_digest = Sha256::digest(&bytes).into();
        Arc::new(TableMetadataDocument::parse(bytes, &head, metadata::limits()).unwrap())
    }

    pub async fn copy_manifest(&self, path: &str) -> FileRecord {
        let mut reader = crowdb_access_iceberg::file::FileReader::new(
            self.blocks.clone(),
            self.manifest.clone(),
            None,
            8192,
        )
        .unwrap();
        let mut bytes = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            bytes.extend(chunk);
        }
        let record = snapshot::store(self.blocks.clone(), path, ContentFormat::Avro, &bytes).await;
        FileRepository::new(self.namespace.store.clone())
            .publish(self.namespace.context, &record)
            .await
            .unwrap();
        record
    }

    pub async fn new() -> Self {
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
        let data = input
            .files
            .resolve(&fixture::table().file("data/first.parquet").unwrap())
            .await
            .unwrap();
        files.publish(namespace.context, &data).await.unwrap();
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

    pub async fn build(
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

pub fn limits() -> PriorManifestLimits {
    PriorManifestLimits {
        snapshots: 10,
        references: 10,
        index_bytes: 100_000,
        manifests: snapshot::limits().manifests,
    }
}
