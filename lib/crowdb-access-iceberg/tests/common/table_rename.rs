use crate::creation::TestCreation;
use crowdb_access_iceberg::{
    catalog::CatalogStore,
    key::OperationId,
    namespace::{NamespaceIdentifier, NamespaceRecovery, NamespaceRepository},
    operation::RequestIdentity,
    record::StorageRecord,
    table::{TableHead, TableLifecycleAction, TableLifecycleRequest, TableRepository},
};

pub fn request(test: &TestCreation, namespace: NamespaceIdentifier) -> TableLifecycleRequest {
    TableLifecycleRequest {
        context: test.fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        namespace: test.parent.identifier.clone(),
        name: "events".into(),
        action: TableLifecycleAction::Rename {
            namespace,
            name: "renamed".into(),
        },
    }
}

pub async fn setup(cross: bool) -> (TestCreation, TableLifecycleRequest, TableHead) {
    let test = TestCreation::new().await;
    test.creator().create(&test.request).await.unwrap();
    let target = if cross {
        let parent = test.fixture.authority(None, &["destination"]);
        test.fixture.publish(&parent).await;
        parent.identifier
    } else {
        test.parent.identifier.clone()
    };
    let before = TableRepository::new(test.fixture.store.clone())
        .select(test.fixture.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .unwrap()
        .head;
    let request = request(&test, target);
    (test, request, before)
}

pub async fn repair(test: &TestCreation) {
    let recovery = NamespaceRecovery::new(test.fixture.store.clone());
    let mut cursor = None;
    loop {
        let page = recovery.repair_page(test.fixture.context, cursor).await.unwrap();
        assert!(page.failures.is_empty(), "{:?}", page.failures);
        assert_eq!(page.deferred, 0);
        cursor = page.continuation;
        if cursor.is_none() {
            break;
        }
    }
}

pub async fn head(test: &TestCreation, before: &TableHead) -> TableHead {
    let key = crowdb_access_iceberg::table::storage_head_key(before.storage_location());
    let value = test
        .fixture
        .store
        .get(&key.encode().unwrap())
        .await
        .unwrap()
        .unwrap();
    let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
        panic!()
    };
    *head
}

pub async fn visible(test: &TestCreation, request: &TableLifecycleRequest) -> (bool, bool) {
    let TableLifecycleAction::Rename { namespace, name } = &request.action else {
        panic!()
    };
    let parent = NamespaceRepository::new(test.fixture.store.clone())
        .load(request.context, namespace)
        .await
        .unwrap();
    let tables = TableRepository::new(test.fixture.store.clone());
    let old = tables
        .select(request.context, test.parent.namespace, &request.name)
        .await
        .unwrap()
        .is_some();
    let new = if let Some(parent) = parent {
        tables
            .select(request.context, parent.namespace, name)
            .await
            .unwrap()
            .is_some()
    } else {
        false
    };
    assert!(!(old && new), "one head must not resolve both names");
    (old, new)
}
