use std::sync::Arc;

use crowdb_access_iceberg::{
    file::{
        ContentFormat, FileContent, FileIdentity, FileKind, FileRecord, FileRepository, FileTreeWriter,
        TableLocation,
    },
    key::OperationId,
    namespace::NamespaceAuthority,
    record::StorageRecord,
    table::{head_key, name_key, TableHead, TableLoader, TableMapping, TableMappingState},
};
use serde_json::json;

use crate::{blocks::TestBlocks, metadata, namespaces::TestNamespace};

pub struct TestTable {
    pub fixture: TestNamespace,
    pub parent: NamespaceAuthority,
    pub head: TableHead,
    pub mapping: TableMapping,
    pub bytes: Vec<u8>,
    pub blocks: Arc<TestBlocks>,
}

impl TestTable {
    pub async fn new() -> Self {
        let fixture = TestNamespace::new().await;
        let parent = fixture.authority(None, &["analytics"]);
        fixture.publish(&parent).await;
        let mut value = metadata::metadata(3);
        let mut head = metadata::head(
            b"",
            3,
            Some(uuid::Uuid::parse_str(value["table-uuid"].as_str().unwrap()).unwrap()),
        );
        head.catalog = fixture.context.catalog;
        head.namespace = parent.namespace;
        let location = TableLocation {
            catalog: head.catalog,
            table: head.table,
        };
        head.metadata_location = location.file("metadata/one.json").unwrap();
        value["location"] = json!(location.to_string());
        value["last-sequence-number"] = json!(3);
        value["current-snapshot-id"] = json!(20);
        value["refs"] =
            json!({"main":{"type":"branch","snapshot-id":20},"tag":{"type":"tag","snapshot-id":30}});
        let mut snapshots = Vec::new();
        for (snapshot, sequence) in [(10, 1), (20, 2), (30, 3)] {
            let mut entry = metadata::snapshot(snapshot, sequence);
            entry["manifest-list"] = json!(location
                .file(&format!("metadata/{snapshot}.avro"))
                .unwrap()
                .to_string());
            snapshots.push(entry);
        }
        value["snapshots"] = json!(snapshots);
        let bytes = serde_json::to_vec_pretty(&value).unwrap();
        let blocks = Arc::new(TestBlocks::default());
        let mut writer = FileTreeWriter::new(
            blocks.clone(),
            FileIdentity {
                table: location,
                file: head.metadata_file,
            },
            1024,
        )
        .unwrap();
        writer.push(&bytes).await.unwrap();
        let tree = writer.finish().await.unwrap();
        head.metadata_digest = tree.digest;
        let record = FileRecord {
            file: head.metadata_file,
            location: head.metadata_location.clone(),
            kind: FileKind::Metadata,
            format: ContentFormat::Json,
            length: tree.length,
            digest: tree.digest,
            content: FileContent::Chunks { root: tree.root },
            hint: None,
        };
        FileRepository::new(fixture.store.clone())
            .publish(fixture.context, &record)
            .await
            .unwrap();
        let mapping = TableMapping {
            catalog: head.catalog,
            namespace: head.namespace,
            name: head.name.clone(),
            table: head.table,
            name_epoch: head.name_epoch,
            operation: OperationId::random(),
            state: TableMappingState::Published,
        };
        let result = Self {
            fixture,
            parent,
            head,
            mapping,
            bytes,
            blocks,
        };
        result.publish_head().await;
        result.publish_mapping().await;
        result
    }

    pub async fn publish_head(&self) {
        self.fixture
            .put(
                head_key(self.head.catalog, self.head.table),
                StorageRecord::TableHead(Box::new(self.head.clone())),
            )
            .await;
    }

    pub async fn publish_mapping(&self) {
        self.fixture
            .put(
                name_key(self.mapping.catalog, self.mapping.namespace, &self.mapping.name).unwrap(),
                StorageRecord::TableMapping(self.mapping.clone()),
            )
            .await;
    }

    pub fn loader(&self) -> TableLoader {
        TableLoader::new(
            self.fixture.store.clone(),
            self.blocks.clone(),
            metadata::limits(),
        )
    }
}
