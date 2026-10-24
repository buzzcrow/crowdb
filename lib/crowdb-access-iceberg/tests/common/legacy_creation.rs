use crowdb_access_iceberg::{
    commit::{
        CommitPreparationLimits, CommitProofLimits, CommitRequestLimits, PriorManifestLimits,
        TableCreateOperation, TableCreatePhase, TableCreateStage, TableRecovery,
    },
    file::{
        ContentFormat, FileContent, FileIdentity, FileKind, FileRecord, FileRepository, FileTreeWriter,
        TableLocation,
    },
    key::{CatalogId, CatalogScope, IcebergKey, OperationId},
    operation::{PayloadStore, RequestIdentity},
    record::StorageRecord,
    table::storage_head_key,
};

use crate::{metadata, snapshot, staging::TestStaged};

pub struct TestLegacyCreation {
    pub staged: TestStaged,
    pub operation: TableCreateOperation,
}

impl TestLegacyCreation {
    pub async fn new() -> Self {
        Self::with_layout(false).await
    }

    pub async fn with_layout(native: bool) -> Self {
        let staged = TestStaged::new().await;
        if native {
            staged
                .namespace
                .put(
                    crowdb_access_iceberg::key::IcebergKey::Catalog {
                        catalog: staged.namespace.context.catalog,
                        scope: crowdb_access_iceberg::key::CatalogScope::Authority,
                        suffix: Vec::new(),
                    },
                    StorageRecord::Authority(
                        crowdb_access_iceberg::catalog::CatalogAuthority::new_native(
                            staged.namespace.context.catalog,
                            "native".into(),
                        )
                        .unwrap(),
                    ),
                )
                .await;
        }
        let output = staged.stage().await;
        let response = TestStaged::response(&output);
        let value = &response["metadata"];
        let table: TableLocation = format!("{}/", value["location"].as_str().unwrap())
            .parse()
            .unwrap();
        let identity = RequestIdentity {
            operation: OperationId::from_bytes(table.table.as_bytes()).unwrap(),
            issued_ms: 100,
        };
        let bytes = serde_json::to_vec(value).unwrap();
        let mut head = metadata::head(
            &bytes,
            2,
            Some(uuid::Uuid::parse_str(value["table-uuid"].as_str().unwrap()).unwrap()),
        );
        head.catalog = table.catalog;
        head.table = table.table;
        head.namespace = staged.parent.namespace;
        head.metadata_location = table
            .file(&format!("metadata/1-{}.metadata.json", head.metadata_file))
            .unwrap();
        head.pending_operation = Some(identity.operation);
        let payload = PayloadStore::new(staged.namespace.store.clone())
            .put(table.catalog, identity.operation, &bytes)
            .await
            .unwrap();
        let operation = TableCreateOperation {
            context: staged.namespace.context,
            identity,
            principal: "legacy writer".into(),
            namespace: staged.parent.identifier.clone(),
            revision: 1,
            timestamp_ms: 1000,
            phase: TableCreatePhase::Staged,
            input: payload.clone(),
            document: payload.clone(),
            response: payload.clone(),
            candidate: head,
            admission: None,
            outcome: None,
            stage: Some(TableCreateStage {
                created_ms: 1000,
                expires_ms: 2000,
                response: payload,
                binding: None,
            }),
        };
        let test = Self { staged, operation };
        test.persist().await;
        test
    }

    pub async fn persist(&self) {
        self.staged
            .namespace
            .put(
                self.operation.key(),
                StorageRecord::TableCreateOperation(Box::new(self.operation.clone())),
            )
            .await;
    }

    pub fn head(&self) -> Option<StorageRecord> {
        let key = storage_head_key(self.operation.candidate.storage_location());
        self.staged
            .namespace
            .store
            .values
            .load()
            .get(&key.encode().unwrap())
            .map(|value| StorageRecord::decode(&key, &value.bytes).unwrap())
    }

    pub fn recovery(&self) -> TableRecovery {
        let staged = crate::staging::limits();
        TableRecovery::new(
            self.staged.namespace.store.clone(),
            self.staged.blocks.clone(),
            CommitProofLimits {
                preparation: CommitPreparationLimits {
                    request: CommitRequestLimits {
                        json: metadata::limits(),
                        requirements: 100,
                        updates: 100,
                    },
                    evaluation: staged.evaluation,
                },
                prior: PriorManifestLimits {
                    snapshots: 10,
                    references: 10,
                    index_bytes: 100_000,
                    manifests: snapshot::limits().manifests,
                },
                snapshots: staged.snapshots,
                auxiliary: staged.auxiliary,
            },
        )
    }

    pub async fn publish_chunked_metadata(
        &self,
        blocks: std::sync::Arc<crate::gc_blocks::TestReclaimBlocks>,
    ) {
        let bytes = PayloadStore::new(self.staged.namespace.store.clone())
            .get(&self.operation.document)
            .await
            .unwrap();
        let head = &self.operation.candidate;
        let mut writer = FileTreeWriter::new(
            blocks,
            FileIdentity {
                table: head.storage_location(),
                file: head.metadata_file,
            },
            128,
        )
        .unwrap();
        writer.push(&bytes).await.unwrap();
        let tree = writer.finish().await.unwrap();
        FileRepository::new(self.staged.namespace.store.clone())
            .publish(
                self.operation.context,
                &FileRecord {
                    file: head.metadata_file,
                    location: head.metadata_location.clone(),
                    kind: FileKind::Metadata,
                    format: ContentFormat::Json,
                    length: tree.length,
                    digest: head.metadata_digest,
                    content: FileContent::Chunks { root: tree.root },
                    hint: None,
                },
            )
            .await
            .unwrap();
    }

    pub async fn retire_catalog(&self) {
        use crowdb_access_iceberg::catalog::{CatalogLifecycle, CatalogStore, RootState};
        let authority = IcebergKey::Catalog {
            catalog: self.operation.context.catalog,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        };
        let value = self
            .staged
            .namespace
            .store
            .get(&authority.encode().unwrap())
            .await
            .unwrap()
            .unwrap();
        let StorageRecord::Authority(mut retired) = StorageRecord::decode(&authority, &value.bytes).unwrap()
        else {
            panic!()
        };
        retired.lifecycle = CatalogLifecycle::Retired;
        self.staged
            .namespace
            .put(authority, StorageRecord::Authority(retired))
            .await;
        self.staged
            .namespace
            .root(
                self.operation.context.replacement(CatalogId::random()).unwrap(),
                RootState::Ready,
            )
            .await;
    }
}
