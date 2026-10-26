#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/table_creation.rs"]
#[allow(dead_code)]
mod creation;
#[path = "common/namespace.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/namespace_recovery_store.rs"]
mod namespace_recovery_store;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/table_rename.rs"]
#[allow(dead_code)]
mod rename;

use crowdb_access_iceberg::{
    catalog::CatalogStore,
    namespace::NamespaceRepository,
    operation::PayloadStore,
    record::StorageRecord,
    table::{name_key, TableLifecycleAction, TableLifecycleOperation, TableLifecyclePhase, TableLifecycles},
};

#[tokio::test]
async fn existing_rename_journals_remain_readable_and_can_finish() {
    let (test, request, before) = rename::setup(true).await;
    let TableLifecycleAction::Rename { namespace, name } = &request.action else {
        panic!()
    };
    let target = NamespaceRepository::new(test.fixture.store.clone())
        .load(request.context, namespace)
        .await
        .unwrap()
        .unwrap();
    let key = name_key(before.catalog, before.namespace, &before.name).unwrap();
    let value = test
        .fixture
        .store
        .get(&key.encode().unwrap())
        .await
        .unwrap()
        .unwrap();
    let StorageRecord::TableMapping(source) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
        panic!()
    };
    let mut candidate = before.clone();
    candidate.namespace = target.namespace;
    candidate.name.clone_from(name);
    candidate.name_epoch += 1;
    candidate.operation_fence += 1;
    candidate.pending_operation = Some(request.identity.operation);
    let input = serde_json::to_vec(&serde_json::json!({"namespace": request.namespace.components(),
        "name": request.name, "action": {"rename": {"namespace": namespace.components(), "name": name}}}))
    .unwrap();
    let input = PayloadStore::new(test.fixture.store.clone())
        .put(request.context.catalog, request.identity.operation, &input)
        .await
        .unwrap();
    let operation = TableLifecycleOperation {
        context: request.context,
        identity: request.identity,
        principal: request.principal.clone(),
        revision: 1,
        phase: TableLifecyclePhase::Prepared,
        input,
        source,
        before,
        candidate,
        destination_namespace: Some(namespace.clone()),
        purge_requested: false,
        admission: None,
        outcome: None,
    };
    test.fixture
        .put(
            operation.key(),
            StorageRecord::TableLifecycleOperation(Box::new(operation)),
        )
        .await;
    let manager = TableLifecycles::new(test.fixture.store.clone());
    assert_eq!(
        manager
            .resume(request.context, request.identity.operation)
            .await
            .unwrap()
            .status,
        204
    );
    assert_eq!(rename::visible(&test, &request).await, (false, true));
    assert_eq!(
        manager
            .load(request.context, request.identity.operation)
            .await
            .unwrap()
            .unwrap()
            .phase,
        TableLifecyclePhase::Complete
    );
}
