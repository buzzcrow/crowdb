use std::sync::Arc;

use crowdb_access_iceberg::catalog::{ActiveCatalogRecord, CatalogContext, CatalogStore, RootState};
use crowdb_access_iceberg::file::{ContentFormat, FileContent, FileKind, FileRecord, TableLocation};
use crowdb_access_iceberg::key::{CatalogId, FileId, IcebergKey, OperationId, SystemScope, TableId};
use crowdb_access_iceberg::operation::mutation_identity;
use crowdb_access_iceberg::record::StorageRecord;
use sha2::{Digest, Sha256};

use crate::common::TestStore;

pub struct TestFile {
    pub store: Arc<TestStore>,
    pub context: CatalogContext,
    pub table: TableLocation,
}

impl TestFile {
    pub async fn new(store: TestStore) -> Self {
        let catalog = CatalogId::random();
        let fixture = Self {
            store: Arc::new(store),
            context: CatalogContext {
                catalog,
                activation_epoch: 1,
            },
            table: TableLocation {
                catalog,
                table: TableId::random(),
            },
        };
        fixture.root(fixture.context, RootState::Ready).await;
        fixture
    }

    pub async fn root(&self, context: CatalogContext, state: RootState) {
        let key = IcebergKey::System {
            scope: SystemScope::ActiveRoot,
            suffix: Vec::new(),
        }
        .encode()
        .unwrap();
        let bytes = StorageRecord::Active(ActiveCatalogRecord {
            context,
            operation: OperationId::random(),
            state,
        })
        .encode()
        .unwrap();
        let previous = self.store.get(&key).await.unwrap();
        let expected = previous.as_ref().map(|value| value.bytes.as_slice());
        self.store
            .compare_exchange(&key, expected, &bytes, mutation_identity(&key, expected, &bytes))
            .await
            .unwrap();
    }

    pub fn record(&self, key: &str, input: &[u8]) -> FileRecord {
        FileRecord {
            file: FileId::random(),
            location: self.table.file(key).unwrap(),
            kind: FileKind::Metadata,
            format: ContentFormat::Json,
            length: input.len() as u64,
            digest: Sha256::digest(input).into(),
            content: FileContent::select_inline(FileKind::Metadata, input).unwrap(),
            hint: None,
        }
    }
}
