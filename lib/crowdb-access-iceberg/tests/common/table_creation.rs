use std::sync::Arc;

use crowdb_access_iceberg::{
    commit::{TableCreationRequest, TableCreator},
    key::OperationId,
    namespace::{NamespaceAuthority, NamespaceDropRequest},
    operation::RequestIdentity,
};
use serde_json::json;

use crate::{blocks::TestBlocks, fixture::TestNamespace};

pub struct TestCreation {
    pub fixture: TestNamespace,
    pub parent: NamespaceAuthority,
    pub blocks: Arc<TestBlocks>,
    pub request: TableCreationRequest,
}

impl TestCreation {
    pub async fn new() -> Self {
        Self::with_fixture(TestNamespace::new().await).await
    }

    pub async fn with_fixture(fixture: TestNamespace) -> Self {
        let parent = fixture.authority(None, &["parent"]);
        fixture.publish(&parent).await;
        let request = TableCreationRequest {
            context: fixture.context,
            identity: RequestIdentity {
                operation: OperationId::random(),
                issued_ms: 100,
            },
            principal: "writer".into(),
            namespace: parent.identifier.clone(),
            timestamp_ms: 1000,
            body: serde_json::to_vec(&json!({"name":"events","schema":{"type":"struct","schema-id":0,
                "fields":[{"id":91,"name":"id","type":"long","required":true}]}}))
            .unwrap(),
        };
        Self {
            fixture,
            parent,
            blocks: Arc::new(TestBlocks::default()),
            request,
        }
    }

    pub fn creator(&self) -> TableCreator {
        TableCreator::new(self.fixture.store.clone(), self.blocks.clone())
    }

    pub fn drop_request(&self) -> NamespaceDropRequest {
        NamespaceDropRequest {
            context: self.fixture.context,
            identity: RequestIdentity {
                operation: OperationId::random(),
                issued_ms: 100,
            },
            principal: "writer".into(),
            identifier: self.parent.identifier.clone(),
        }
    }
}
