use std::sync::Arc;

use crowdb_access_iceberg::file::{
    ContentFormat, FileContent, FileIdentity, FileKind, FileRecord, FileTreeWriter, TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};
use crowdb_access_iceberg::metadata_projection::{MetadataRead, ProjectionStore};

use crate::{blocks::TestBlocks, store::TestStore};

pub struct TestUnavailableProjectionStore;

#[async_trait::async_trait]
impl crowdb_access_iceberg::catalog::CatalogStore for TestUnavailableProjectionStore {
    async fn get(
        &self,
        _key: &[u8],
    ) -> Result<Option<crowdb_access_iceberg::catalog::StoredValue>, crowdb_access_iceberg::catalog::StoreError>
    {
        Err(crowdb_access_iceberg::catalog::StoreError::Response)
    }

    async fn compare_exchange(
        &self,
        _key: &[u8],
        _expected: Option<&[u8]>,
        _value: &[u8],
        _identity: crowdb_protocol::chunk_kv::ClientRequestId,
    ) -> Result<crowdb_access_iceberg::catalog::CasOutcome, crowdb_access_iceberg::catalog::StoreError> {
        Err(crowdb_access_iceberg::catalog::StoreError::Response)
    }
}

pub struct TestProjection {
    pub store: Arc<TestStore>,
    pub blocks: Arc<TestBlocks>,
    pub record: FileRecord,
    pub projection: ProjectionStore,
}

impl TestProjection {
    pub async fn new(input: &[u8]) -> Self {
        let store = Arc::new(TestStore::default());
        let blocks = Arc::new(TestBlocks::default());
        let owner = FileIdentity {
            table: TableLocation {
                catalog: CatalogId::random(),
                table: TableId::random(),
            },
            file: FileId::random(),
        };
        let mut writer = FileTreeWriter::new(blocks.clone(), owner, 4096).unwrap();
        writer.push(input).await.unwrap();
        let tree = writer.finish().await.unwrap();
        let record = FileRecord {
            file: owner.file,
            location: owner.table.file("metadata/test.json").unwrap(),
            kind: FileKind::Metadata,
            format: ContentFormat::Json,
            length: tree.length,
            digest: tree.digest,
            content: FileContent::Chunks { root: tree.root },
            hint: None,
        };
        Self {
            projection: ProjectionStore::new(store.clone(), blocks.clone()),
            store,
            blocks,
            record,
        }
    }

    pub async fn fallback(&self, generation: u64, names: &[&str], expected: &[u8]) {
        let MetadataRead::Canonical(mut reader) = self
            .projection
            .select(self.record.clone(), generation, names)
            .await
            .unwrap()
        else {
            panic!("expected canonical fallback");
        };
        let mut actual = Vec::new();
        while let Some(bytes) = reader.next().await.unwrap() {
            actual.extend(bytes);
        }
        assert_eq!(actual, expected);
    }
}
