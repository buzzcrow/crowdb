use std::sync::Arc;

use crowdb_access_iceberg::catalog::{ActiveCatalogRecord, CatalogContext, CatalogStore, RootState};
use crowdb_access_iceberg::key::{CatalogId, IcebergKey, NamespaceId, OperationId, SystemScope};
use crowdb_access_iceberg::namespace::{
    authority_key, name_key, NamespaceAuthority, NamespaceIdentifier, NamespaceLifecycle, NamespaceMapping,
    NamespaceMappingState, NamespaceProperties,
};
use crowdb_access_iceberg::operation::mutation_identity;
use crowdb_access_iceberg::record::StorageRecord;

use crate::common::TestStore;

pub struct TestNamespace {
    pub store: Arc<TestStore>,
    pub context: CatalogContext,
}

impl TestNamespace {
    pub async fn new() -> Self {
        let fixture = Self {
            store: Arc::new(TestStore::default()),
            context: CatalogContext {
                catalog: CatalogId::random(),
                activation_epoch: 1,
            },
        };
        fixture.root(fixture.context, RootState::Ready).await;
        fixture
    }

    pub async fn root(&self, context: CatalogContext, state: RootState) {
        self.put(
            IcebergKey::System {
                scope: SystemScope::ActiveRoot,
                suffix: Vec::new(),
            },
            StorageRecord::Active(ActiveCatalogRecord {
                context,
                operation: OperationId::random(),
                state,
            }),
        )
        .await;
    }

    pub async fn put(&self, key: IcebergKey, record: StorageRecord) {
        self.bytes(key, &record.encode().unwrap()).await;
    }

    pub async fn bytes(&self, key: IcebergKey, bytes: &[u8]) {
        let key = key.encode().unwrap();
        let previous = self.store.get(&key).await.unwrap();
        let expected = previous.as_ref().map(|value| value.bytes.as_slice());
        self.store
            .compare_exchange(&key, expected, bytes, mutation_identity(&key, expected, bytes))
            .await
            .unwrap();
    }

    pub fn authority(&self, parent: Option<NamespaceId>, names: &[&str]) -> NamespaceAuthority {
        NamespaceAuthority {
            catalog: self.context.catalog,
            namespace: NamespaceId::random(),
            parent,
            identifier: NamespaceIdentifier::new(names.iter().map(|name| (*name).into()).collect()).unwrap(),
            name_epoch: 1,
            property_revision: 1,
            admission_fence: 1,
            mutation_revision: 1,
            lifecycle: NamespaceLifecycle::Ready,
            pending_operation: None,
            properties: NamespaceProperties::default(),
        }
    }

    pub async fn publish(&self, authority: &NamespaceAuthority) -> NamespaceMapping {
        self.put(
            authority_key(authority.catalog, authority.namespace),
            StorageRecord::NamespaceAuthority(Box::new(authority.clone())),
        )
        .await;
        let mapping = NamespaceMapping {
            catalog: authority.catalog,
            parent: authority.parent,
            name: authority.identifier.name().into(),
            namespace: authority.namespace,
            name_epoch: authority.name_epoch,
            operation: OperationId::random(),
            state: NamespaceMappingState::Published,
        };
        self.put(
            name_key(mapping.catalog, mapping.parent, &mapping.name).unwrap(),
            StorageRecord::NamespaceMapping(mapping.clone()),
        )
        .await;
        mapping
    }
}
