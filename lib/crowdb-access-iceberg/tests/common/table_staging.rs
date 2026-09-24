use std::sync::Arc;

use crowdb_access_iceberg::{
    commit::{
        CandidateAuxiliaryLimits, CandidateSnapshotLimits, EvaluationLimits, RequirementLimits,
        StagedCommitLimits, StagedCommitRequest, TableCommitOutcome, TableCreateJournal,
        TableCreateOperation, TableCreationRequest, TableCreator,
    },
    key::OperationId,
    namespace::{NamespaceAuthority, NamespaceDropRequest},
    operation::{PayloadStore, RequestIdentity},
};
use serde_json::{json, Value};

use crate::{blocks::TestBlocks, common::TestStore, metadata, namespaces::TestNamespace, snapshot};

pub struct TestStaged {
    pub namespace: TestNamespace,
    pub parent: NamespaceAuthority,
    pub blocks: Arc<TestBlocks>,
    pub request: TableCreationRequest,
}

impl TestStaged {
    pub async fn new() -> Self {
        Self::with_store(Arc::new(TestStore::default())).await
    }

    pub async fn with_store(store: Arc<TestStore>) -> Self {
        let namespace = TestNamespace {
            store,
            context: crowdb_access_iceberg::catalog::CatalogContext {
                catalog: crate::fixture::table().catalog,
                activation_epoch: 1,
            },
        };
        namespace
            .root(
                namespace.context,
                crowdb_access_iceberg::catalog::RootState::Ready,
            )
            .await;
        let parent = namespace.authority(None, &["parent"]);
        namespace.publish(&parent).await;
        let request = TableCreationRequest {
            context: namespace.context,
            identity: RequestIdentity {
                operation: OperationId::from_bytes(crate::fixture::table().table.as_bytes()).unwrap(),
                issued_ms: 100,
            },
            principal: "writer".into(),
            namespace: parent.identifier.clone(),
            timestamp_ms: 1000,
            body: serde_json::to_vec(&json!({"name":"events","stage-create":true,
                "schema":{"type":"struct","schema-id":0,"fields":[
                    {"id":91,"name":"id","required":true,"type":"long"}]}}))
            .unwrap(),
        };
        Self {
            namespace,
            parent,
            blocks: Arc::new(TestBlocks::default()),
            request,
        }
    }

    pub fn creator(&self) -> TableCreator {
        TableCreator::new(self.namespace.store.clone(), self.blocks.clone()).with_staged_limits(limits())
    }

    pub async fn stage(&self) -> TableCommitOutcome {
        self.creator().stage(&self.request, 2000).await.unwrap()
    }

    pub async fn operation(&self) -> TableCreateOperation {
        TableCreateJournal::new(self.namespace.store.clone())
            .load(self.namespace.context, self.request.identity.operation)
            .await
            .unwrap()
            .unwrap()
    }

    pub async fn response(&self, outcome: &TableCommitOutcome) -> Value {
        serde_json::from_slice(
            &PayloadStore::new(self.namespace.store.clone())
                .get(&outcome.body)
                .await
                .unwrap(),
        )
        .unwrap()
    }

    pub async fn commit_request(&self) -> StagedCommitRequest {
        let staged = self.stage().await;
        let response = self.response(&staged).await;
        let value = &response["metadata"];
        let body = json!({"requirements":[{"type":"assert-create"}],"updates":[
            {"action":"assign-uuid","uuid":value["table-uuid"]},
            {"action":"upgrade-format-version","format-version":value["format-version"]},
            {"action":"add-schema","schema":value["schemas"][0]},
            {"action":"set-current-schema","schema-id":-1},
            {"action":"add-spec","spec":value["partition-specs"][0]},
            {"action":"set-default-spec","spec-id":-1},
            {"action":"add-sort-order","sort-order":value["sort-orders"][0]},
            {"action":"set-default-sort-order","sort-order-id":-1},
            {"action":"set-location","location":value["location"]},
            {"action":"set-properties","updates":value["properties"]},
            {"action":"set-properties","updates":{"transaction":"committed"}}
        ]});
        StagedCommitRequest {
            context: self.namespace.context,
            identity: RequestIdentity {
                operation: OperationId::random(),
                issued_ms: 101,
            },
            principal: "writer".into(),
            namespace: self.parent.identifier.clone(),
            name: "events".into(),
            body: serde_json::to_vec(&body).unwrap(),
            timestamp_ms: 1001,
        }
    }

    pub fn drop_request(&self) -> NamespaceDropRequest {
        NamespaceDropRequest {
            context: self.namespace.context,
            identity: RequestIdentity {
                operation: OperationId::random(),
                issued_ms: 100,
            },
            principal: "writer".into(),
            identifier: self.parent.identifier.clone(),
        }
    }
}

pub fn limits() -> StagedCommitLimits {
    StagedCommitLimits {
        evaluation: EvaluationLimits {
            metadata: metadata::limits(),
            requirements: RequirementLimits {
                count: 100,
                text_bytes: 4096,
            },
            updates: 100,
            work_bytes: 8 * 1024 * 1024,
        },
        snapshots: CandidateSnapshotLimits {
            snapshots: 10,
            entries: 100,
            manifest_bytes: 1_000_000,
            ranges: 100,
            files: snapshot::limits(),
        },
        auxiliary: CandidateAuxiliaryLimits {
            files: 10,
            bytes: 1_000_000,
            work: 1000,
            puffin_encoded_bytes: 100_000,
            puffin_decoded_bytes: 100_000,
            parquet: snapshot::limits().position_deletes.metadata,
            manifests: snapshot::limits().manifests,
            partition_rows: crowdb_access_iceberg::manifest::PartitionStatisticsRowLimits {
                page: snapshot::limits().position_deletes.page,
                rows: 1000,
                buffered_bytes: 64 * 1024 * 1024,
            },
        },
    }
}
