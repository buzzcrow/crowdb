#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
mod fixture;

use crowdb_access_iceberg::{
    catalog::{CatalogError, CatalogStore},
    file::{FileRecord, FileRepository},
    key::{IcebergKey, NamespaceId, OperationId, TableId},
    operation::mutation_identity,
    record::StorageRecord,
    table::{
        head_key, name_key, TableHead, TableLifecycle, TableMapping, TableMappingState, TableRepository,
    },
};

fn head(record: &FileRecord) -> TableHead {
    TableHead {
        catalog: record.location.table().catalog,
        table: record.location.table().table,
        namespace: NamespaceId::random(),
        name: "events".into(),
        name_epoch: 1,
        lifecycle: TableLifecycle::Ready,
        generation: 1,
        metadata_file: record.file,
        metadata_location: record.location.clone(),
        metadata_digest: record.digest,
        format_version: 2,
        table_uuid: Some(uuid::Uuid::new_v4()),
        operation_fence: 1,
        pending_operation: None,
    }
}

fn mapping(head: &TableHead) -> TableMapping {
    TableMapping {
        catalog: head.catalog,
        namespace: head.namespace,
        name: head.name.clone(),
        table: head.table,
        name_epoch: head.name_epoch,
        operation: OperationId::random(),
        state: TableMappingState::Published,
    }
}

async fn put(store: &common::TestStore, key: IcebergKey, record: StorageRecord) {
    let key = key.encode().unwrap();
    let bytes = record.encode().unwrap();
    let previous = store.get(&key).await.unwrap();
    let expected = previous.as_ref().map(|value| value.bytes.as_slice());
    store
        .compare_exchange(&key, expected, &bytes, mutation_identity(&key, expected, &bytes))
        .await
        .unwrap();
}

#[tokio::test]
async fn selection_pins_one_head_and_immutable_metadata_generation() {
    let fixture = fixture::TestFile::new(common::TestStore::default()).await;
    let file = fixture.record("metadata/one.json", b"{\"generation\":1}");
    FileRepository::new(fixture.store.clone())
        .publish(fixture.context, &file)
        .await
        .unwrap();
    let mut head = head(&file);
    let mapping = mapping(&head);
    put(
        &fixture.store,
        head_key(head.catalog, head.table),
        StorageRecord::TableHead(Box::new(head.clone())),
    )
    .await;
    put(
        &fixture.store,
        name_key(head.catalog, head.namespace, &head.name).unwrap(),
        StorageRecord::TableMapping(mapping),
    )
    .await;
    let repository = TableRepository::new(fixture.store.clone());
    let first = repository
        .select(fixture.context, head.namespace, &head.name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.metadata, file);
    repository.ensure_current(fixture.context, &first).await.unwrap();
    let second_file = fixture.record("metadata/two.json", b"{\"generation\":2}");
    FileRepository::new(fixture.store.clone())
        .publish(fixture.context, &second_file)
        .await
        .unwrap();
    head.generation += 1;
    head.metadata_file = second_file.file;
    head.metadata_location = second_file.location.clone();
    head.metadata_digest = second_file.digest;
    put(
        &fixture.store,
        head_key(head.catalog, head.table),
        StorageRecord::TableHead(Box::new(head.clone())),
    )
    .await;
    let second = repository
        .select(fixture.context, head.namespace, &head.name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.head.generation, 1);
    assert_eq!(first.metadata, file);
    assert_eq!(second.head.generation, 2);
    assert_eq!(second.metadata, second_file);
    assert!(matches!(
        repository.ensure_current(fixture.context, &first).await,
        Err(CatalogError::Conflict)
    ));
    repository.ensure_current(fixture.context, &second).await.unwrap();
}

#[tokio::test]
async fn reservations_stale_names_and_tombstones_do_not_resolve() {
    for case in 0..5 {
        let fixture = fixture::TestFile::new(common::TestStore::default()).await;
        let file = fixture.record("metadata/one.json", b"{}");
        FileRepository::new(fixture.store.clone())
            .publish(fixture.context, &file)
            .await
            .unwrap();
        let mut head = head(&file);
        let mut mapping = mapping(&head);
        match case {
            0 => mapping.state = TableMappingState::Reserved,
            1 => head.name_epoch += 1,
            2 => head.name = "renamed".into(),
            3 => head.namespace = NamespaceId::random(),
            _ => {
                head.lifecycle = TableLifecycle::Tombstone;
                head.pending_operation = Some(OperationId::random());
            }
        }
        put(
            &fixture.store,
            head_key(head.catalog, head.table),
            StorageRecord::TableHead(Box::new(head.clone())),
        )
        .await;
        put(
            &fixture.store,
            name_key(mapping.catalog, mapping.namespace, &mapping.name).unwrap(),
            StorageRecord::TableMapping(mapping.clone()),
        )
        .await;
        assert!(TableRepository::new(fixture.store)
            .select(fixture.context, mapping.namespace, &mapping.name)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn corrupt_selected_file_is_not_reported_as_table_absence() {
    let fixture = fixture::TestFile::new(common::TestStore::default()).await;
    let file = fixture.record("metadata/one.json", b"{}");
    let mut head = head(&file);
    let mapping = mapping(&head);
    put(
        &fixture.store,
        name_key(mapping.catalog, mapping.namespace, &mapping.name).unwrap(),
        StorageRecord::TableMapping(mapping),
    )
    .await;
    let repository = TableRepository::new(fixture.store.clone());
    for published in [false, true] {
        if published {
            FileRepository::new(fixture.store.clone())
                .publish(fixture.context, &file)
                .await
                .unwrap();
            head.metadata_digest[0] ^= 1;
        }
        put(
            &fixture.store,
            head_key(head.catalog, head.table),
            StorageRecord::TableHead(Box::new(head.clone())),
        )
        .await;
        assert!(repository
            .select(fixture.context, head.namespace, &head.name)
            .await
            .is_err());
    }
    let mut retired = fixture.context;
    retired.activation_epoch += 1;
    assert!(matches!(
        repository.select(retired, head.namespace, "missing").await,
        Err(CatalogError::Conflict)
    ));
}

#[tokio::test]
async fn bounded_table_records_round_trip_and_reject_wrong_keys_and_versions() {
    let fixture = fixture::TestFile::new(common::TestStore::default()).await;
    let mut head = head(&fixture.record("metadata/one.json", b"{}"));
    for version in 1..=3 {
        head.format_version = version;
        let record = StorageRecord::TableHead(Box::new(head.clone()));
        let bytes = record.encode().unwrap();
        assert!(bytes.len() < 4096);
        assert_eq!(
            StorageRecord::decode(&head_key(head.catalog, head.table), &bytes).unwrap(),
            record
        );
        assert!(StorageRecord::decode(&head_key(head.catalog, TableId::random()), &bytes).is_err());
    }
    let mapping = mapping(&head);
    let record = StorageRecord::TableMapping(mapping.clone());
    assert_eq!(
        StorageRecord::decode(
            &name_key(head.catalog, head.namespace, &head.name).unwrap(),
            &record.encode().unwrap()
        )
        .unwrap(),
        record
    );
    head.format_version = 4;
    assert!(StorageRecord::TableHead(Box::new(head.clone())).encode().is_err());
    head.format_version = 2;
    head.table_uuid = None;
    assert!(StorageRecord::TableHead(Box::new(head.clone())).encode().is_err());
    head.format_version = 1;
    assert!(StorageRecord::TableHead(Box::new(head.clone())).encode().is_ok());
    head.generation = 0;
    assert!(StorageRecord::TableHead(Box::new(head)).encode().is_err());
}
