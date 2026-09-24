use std::sync::Arc;

use crowdb_access_iceberg::{
    commit::{CandidateFileSource, CandidateSnapshotLimits, TableCreateOperation, TableCreatePhase},
    key::OperationId,
    namespace::NamespaceIdentifier,
    operation::{PayloadStore, RequestIdentity},
    record::StorageRecord,
    table::{head_key, name_key, TableMappingState, TableMetadataDocument},
};

use crate::{metadata, provenance::TestPrior, snapshot};

pub struct TestInitial {
    pub fixture: TestPrior,
    pub operation: TableCreateOperation,
    pub document: Arc<TableMetadataDocument>,
}

impl TestInitial {
    pub async fn new(restore_schema: bool, parent: Option<i64>) -> Self {
        let fixture = TestPrior::new().await;
        let candidate = fixture.candidate(restore_schema);
        let mut root = serde_json::Value::Object(candidate.fields().clone());
        if let Some(parent) = parent {
            root["snapshots"][0]["parent-snapshot-id"] = serde_json::json!(parent);
        }
        let bytes = serde_json::to_vec(&root).unwrap();
        let identity = RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        };
        let mut head = metadata::head(&bytes, 2, fixture.selected.head.table_uuid);
        head.pending_operation = Some(identity.operation);
        let document =
            Arc::new(TableMetadataDocument::parse(bytes.clone(), &head, metadata::limits()).unwrap());
        let payloads = PayloadStore::new(fixture.namespace.store.clone());
        let payload = payloads
            .put(head.catalog, identity.operation, &bytes)
            .await
            .unwrap();
        let operation = TableCreateOperation {
            context: fixture.namespace.context,
            identity,
            principal: "writer".into(),
            namespace: NamespaceIdentifier::new(vec!["parent".into()]).unwrap(),
            revision: 2,
            timestamp_ms: 1000,
            phase: TableCreatePhase::Reserved,
            input: payload.clone(),
            document: payload.clone(),
            response: payload,
            candidate: head.clone(),
            admission: None,
            outcome: None,
        };
        fixture.namespace.store.values.rcu(|current| {
            let mut next = (**current).clone();
            next.remove(&head_key(head.catalog, head.table).encode().unwrap());
            Arc::new(next)
        });
        let mapping = operation.mapping(TableMappingState::Reserved);
        fixture
            .namespace
            .put(
                name_key(mapping.catalog, mapping.namespace, &mapping.name).unwrap(),
                StorageRecord::TableMapping(mapping),
            )
            .await;
        let test = Self {
            fixture,
            operation,
            document,
        };
        test.persist().await;
        test
    }

    pub async fn persist(&self) {
        self.fixture
            .namespace
            .put(
                self.operation.key(),
                StorageRecord::TableCreateOperation(Box::new(self.operation.clone())),
            )
            .await;
    }

    pub fn source(&self) -> Arc<CandidateFileSource> {
        Arc::new(
            CandidateFileSource::for_creation(
                self.fixture.namespace.store.clone(),
                self.fixture.blocks.clone(),
                &self.operation,
                self.document.clone(),
                snapshot::limits().manifests.framing,
            )
            .unwrap(),
        )
    }
}

pub fn limits() -> CandidateSnapshotLimits {
    CandidateSnapshotLimits {
        snapshots: 10,
        entries: 100,
        manifest_bytes: 1_000_000,
        ranges: 100,
        files: snapshot::limits(),
    }
}
