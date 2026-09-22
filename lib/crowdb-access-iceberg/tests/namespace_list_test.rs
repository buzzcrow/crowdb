#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;

use crowdb_access_iceberg::namespace::{name_key, NamespaceLister, NamespaceMappingState};
use crowdb_access_iceberg::record::StorageRecord;
use fixture::TestNamespace;

#[tokio::test]
async fn listing_uses_last_scanned_key_across_empty_stale_pages() {
    let fixture = TestNamespace::new().await;
    let parent = fixture.authority(None, &["parent"]);
    fixture.publish(&parent).await;
    let stale = fixture.authority(Some(parent.namespace), &["parent", "a"]);
    let mut mapping = fixture.publish(&stale).await;
    mapping.name_epoch += 1;
    fixture
        .put(
            name_key(mapping.catalog, mapping.parent, &mapping.name).unwrap(),
            StorageRecord::NamespaceMapping(mapping),
        )
        .await;
    let live = fixture.authority(Some(parent.namespace), &["parent", "b"]);
    fixture.publish(&live).await;
    let grandchild = fixture.authority(Some(live.namespace), &["parent", "b", "c"]);
    fixture.publish(&grandchild).await;
    let lister = NamespaceLister::new(fixture.store.clone(), &[7; 32]).unwrap();
    let first = lister
        .page(fixture.context, Some(&parent.identifier), 1, "")
        .await
        .unwrap()
        .unwrap();
    assert!(first.namespaces.is_empty());
    assert_eq!(first.scanned, 1);
    let token = first.next_page_token.unwrap();
    let second = lister
        .page(fixture.context, Some(&parent.identifier), 1, &token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.namespaces, vec![live.identifier]);
    assert!(second.next_page_token.is_none());
    let roots = lister
        .page(fixture.context, None, 100, "")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(roots.namespaces, vec![parent.identifier]);
}

#[tokio::test]
async fn tokens_bind_parameters_signing_key_and_recreated_parent_identity() {
    let fixture = TestNamespace::new().await;
    let parent = fixture.authority(None, &["parent"]);
    fixture.publish(&parent).await;
    for name in ["a", "b"] {
        fixture
            .publish(&fixture.authority(Some(parent.namespace), &["parent", name]))
            .await;
    }
    let lister = NamespaceLister::new(fixture.store.clone(), &[7; 32]).unwrap();
    let token = lister
        .page(fixture.context, Some(&parent.identifier), 1, "")
        .await
        .unwrap()
        .unwrap()
        .next_page_token
        .unwrap();
    assert!(lister.page(fixture.context, None, 1, &token).await.is_err());
    assert!(lister
        .page(fixture.context, Some(&parent.identifier), 2, &token)
        .await
        .is_err());
    assert!(NamespaceLister::new(fixture.store.clone(), &[8; 32])
        .unwrap()
        .page(fixture.context, Some(&parent.identifier), 1, &token)
        .await
        .is_err());
    let mut tampered = token.clone().into_bytes();
    tampered[2] = if tampered[2] == b'A' { b'B' } else { b'A' };
    assert!(lister
        .page(
            fixture.context,
            Some(&parent.identifier),
            1,
            std::str::from_utf8(&tampered).unwrap()
        )
        .await
        .is_err());
    let replacement = fixture.authority(None, &["parent"]);
    fixture.publish(&replacement).await;
    assert!(lister
        .page(fixture.context, Some(&parent.identifier), 1, &token)
        .await
        .is_err());
}

#[tokio::test]
async fn reserved_names_are_hidden_but_corruption_is_not_silently_filtered() {
    let fixture = TestNamespace::new().await;
    let authority = fixture.authority(None, &["reserved"]);
    let mut mapping = fixture.publish(&authority).await;
    mapping.state = NamespaceMappingState::Reserved;
    let key = name_key(mapping.catalog, mapping.parent, &mapping.name).unwrap();
    fixture
        .put(key.clone(), StorageRecord::NamespaceMapping(mapping))
        .await;
    let lister = NamespaceLister::new(fixture.store.clone(), &[7; 32]).unwrap();
    assert!(lister
        .page(fixture.context, None, 100, "")
        .await
        .unwrap()
        .unwrap()
        .namespaces
        .is_empty());
    fixture.bytes(key, b"corrupt").await;
    assert!(lister.page(fixture.context, None, 100, "").await.is_err());
    assert!(lister
        .page(
            fixture.context,
            Some(&fixture.authority(None, &["missing"]).identifier),
            100,
            ""
        )
        .await
        .unwrap()
        .is_none());
    assert!(lister.page(fixture.context, None, 0, "").await.is_err());
    assert!(lister.page(fixture.context, None, 101, "").await.is_err());
    assert!(lister
        .page(fixture.context, None, 1, &"a".repeat(8193))
        .await
        .is_err());
}
