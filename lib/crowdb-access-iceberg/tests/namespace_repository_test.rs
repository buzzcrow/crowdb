#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;

use std::sync::atomic::Ordering;

use crowdb_access_iceberg::catalog::{CatalogContext, CatalogError, RootState};
use crowdb_access_iceberg::key::{CatalogId, NamespaceId, OperationId};
use crowdb_access_iceberg::namespace::{
    authority_key, name_key, NamespaceIdentifier, NamespaceLifecycle, NamespaceMappingState,
    NamespaceRepository,
};
use crowdb_access_iceberg::record::StorageRecord;
use fixture::TestNamespace;

#[tokio::test]
async fn lookup_walks_stable_parents_without_writing_or_creating_ancestors() {
    let fixture = TestNamespace::new().await;
    let parent = fixture.authority(None, &["数据库"]);
    fixture.publish(&parent).await;
    let child = fixture.authority(Some(parent.namespace), &["数据库", "child"]);
    fixture.publish(&child).await;
    let repository = NamespaceRepository::new(fixture.store.clone());
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    assert_eq!(
        repository.load(fixture.context, &child.identifier).await.unwrap(),
        Some(child)
    );
    for names in [vec!["child"], vec!["absent", "child"], vec!["数据库", "absent"]] {
        let identifier = NamespaceIdentifier::new(names.into_iter().map(String::from).collect()).unwrap();
        assert!(!repository.exists(fixture.context, &identifier).await.unwrap());
    }
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
}

#[tokio::test]
async fn unpublished_missing_and_stale_authorities_are_not_visible() {
    let fixture = TestNamespace::new().await;
    let authority = fixture.authority(None, &["namespace"]);
    let original = fixture.publish(&authority).await;
    let repository = NamespaceRepository::new(fixture.store.clone());
    for variant in 0..3 {
        let mut mapping = original.clone();
        match variant {
            0 => mapping.state = NamespaceMappingState::Reserved,
            1 => mapping.namespace = NamespaceId::random(),
            _ => mapping.name_epoch += 1,
        }
        fixture
            .put(
                name_key(mapping.catalog, mapping.parent, &mapping.name).unwrap(),
                StorageRecord::NamespaceMapping(mapping),
            )
            .await;
        assert!(!repository
            .exists(fixture.context, &authority.identifier)
            .await
            .unwrap());
    }
}

#[tokio::test]
async fn property_and_fence_changes_preserve_reads_but_tombstones_hide_descendants() {
    let fixture = TestNamespace::new().await;
    let mut parent = fixture.authority(None, &["parent"]);
    fixture.publish(&parent).await;
    let child = fixture.authority(Some(parent.namespace), &["parent", "child"]);
    fixture.publish(&child).await;
    let repository = NamespaceRepository::new(fixture.store.clone());
    parent.property_revision += 1;
    parent.admission_fence += 1;
    parent.mutation_revision += 2;
    parent.pending_operation = Some(OperationId::random());
    for lifecycle in [
        NamespaceLifecycle::Ready,
        NamespaceLifecycle::Dropping,
        NamespaceLifecycle::Tombstone,
    ] {
        parent.lifecycle = lifecycle;
        fixture
            .put(
                authority_key(parent.catalog, parent.namespace),
                StorageRecord::NamespaceAuthority(Box::new(parent.clone())),
            )
            .await;
        assert_eq!(
            repository
                .exists(fixture.context, &child.identifier)
                .await
                .unwrap(),
            lifecycle != NamespaceLifecycle::Tombstone
        );
    }
}

#[tokio::test]
async fn recreated_parent_does_not_attach_children_from_the_previous_identity() {
    let fixture = TestNamespace::new().await;
    let parent = fixture.authority(None, &["parent"]);
    fixture.publish(&parent).await;
    let child = fixture.authority(Some(parent.namespace), &["parent", "child"]);
    fixture.publish(&child).await;
    fixture.publish(&fixture.authority(None, &["parent"])).await;
    assert!(!NamespaceRepository::new(fixture.store.clone())
        .exists(fixture.context, &child.identifier)
        .await
        .unwrap());
}

#[tokio::test]
async fn canonical_full_identifier_is_checked_not_only_leaf_name() {
    let fixture = TestNamespace::new().await;
    let parent = fixture.authority(None, &["parent"]);
    fixture.publish(&parent).await;
    let mut child = fixture.authority(Some(parent.namespace), &["parent", "child"]);
    fixture.publish(&child).await;
    let requested = child.identifier.clone();
    child.identifier = NamespaceIdentifier::new(vec!["other".into(), "child".into()]).unwrap();
    fixture
        .put(
            authority_key(child.catalog, child.namespace),
            StorageRecord::NamespaceAuthority(Box::new(child)),
        )
        .await;
    assert!(!NamespaceRepository::new(fixture.store.clone())
        .exists(fixture.context, &requested)
        .await
        .unwrap());
}

#[tokio::test]
async fn corrupt_mapping_or_authority_is_an_error_not_absence() {
    let fixture = TestNamespace::new().await;
    let authority = fixture.authority(None, &["namespace"]);
    let repository = NamespaceRepository::new(fixture.store.clone());
    for key in [
        authority_key(authority.catalog, authority.namespace),
        name_key(authority.catalog, None, authority.identifier.name()).unwrap(),
    ] {
        fixture.publish(&authority).await;
        fixture.bytes(key, b"corrupt").await;
        assert!(matches!(
            repository.exists(fixture.context, &authority.identifier).await,
            Err(CatalogError::Invalid(_))
        ));
    }
}

#[tokio::test]
async fn missing_names_do_not_hide_catalog_maintenance_or_retirement() {
    let fixture = TestNamespace::new().await;
    let repository = NamespaceRepository::new(fixture.store.clone());
    let identifier = NamespaceIdentifier::new(vec!["missing".into()]).unwrap();
    fixture.root(fixture.context, RootState::Fencing).await;
    assert!(matches!(
        repository.load(fixture.context, &identifier).await,
        Err(CatalogError::Busy)
    ));
    fixture
        .root(
            CatalogContext {
                catalog: CatalogId::random(),
                activation_epoch: fixture.context.activation_epoch + 1,
            },
            RootState::Ready,
        )
        .await;
    assert!(matches!(
        repository.load(fixture.context, &identifier).await,
        Err(CatalogError::Conflict)
    ));
}
