// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_resource_conflict.rs"]
mod interleave;
#[path = "common/namespace_store.rs"]
mod namespace_store;

use crowdb_access_iceberg::{
    catalog::CatalogError,
    key::OperationId,
    namespace::{
        name_key, NamespaceCreateRequest, NamespaceCreator, NamespaceIdentifier, NamespaceMappingState,
        NamespaceProperties, NamespaceRepository,
    },
    operation::RequestIdentity,
    record::StorageRecord,
};
use fixture::TestNamespace;
use interleave::{TestConflictStore, TestInterleave};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

fn request(fixture: &TestNamespace) -> NamespaceCreateRequest {
    NamespaceCreateRequest {
        context: fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier: NamespaceIdentifier::new(vec!["parent".into(), "child".into()]).unwrap(),
        properties: NamespaceProperties::default(),
    }
}

#[tokio::test]
async fn parent_claim_and_release_reuse_cas_observations_without_reloading_the_parent() {
    for interleave in [TestInterleave::ClaimProperties, TestInterleave::ReleaseProperties] {
        let fixture = TestNamespace::new().await;
        let parent = fixture.authority(None, &["parent"]);
        fixture.publish(&parent).await;
        let claim = matches!(interleave, TestInterleave::ClaimProperties);
        let store = Arc::new(TestConflictStore {
            inner: fixture.store.clone(),
            interleave,
            armed: AtomicBool::new(true),
        });
        let reads = fixture.store.reads.load(Ordering::SeqCst);
        let attempts = fixture.store.cas_attempts.load(Ordering::SeqCst);
        let outcome = NamespaceCreator::new(store)
            .create(&request(&fixture))
            .await
            .unwrap();
        assert_eq!(outcome.status, 200);
        assert_eq!(fixture.store.reads.load(Ordering::SeqCst) - reads, 4);
        assert_eq!(
            fixture.store.cas_attempts.load(Ordering::SeqCst) - attempts,
            if claim { 7 } else { 6 }
        );
        let reader = NamespaceRepository::new(fixture.store.clone());
        let selected = reader
            .load(fixture.context, &parent.identifier)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(selected.creating_child, None);
        assert_eq!(selected.properties.entries()["rival"], "kept");
        assert_eq!(selected.property_revision, 2);
        assert!(reader
            .exists(fixture.context, &request(&fixture).identifier)
            .await
            .unwrap());
    }
}

#[tokio::test]
async fn observed_parent_epoch_change_is_rejected_without_retargeting_or_extra_get() {
    let fixture = TestNamespace::new().await;
    let parent = fixture.authority(None, &["parent"]);
    fixture.publish(&parent).await;
    let store = Arc::new(TestConflictStore {
        inner: fixture.store.clone(),
        interleave: TestInterleave::ClaimEpoch,
        armed: AtomicBool::new(true),
    });
    let reads = fixture.store.reads.load(Ordering::SeqCst);
    assert!(matches!(
        NamespaceCreator::new(store).create(&request(&fixture)).await,
        Err(CatalogError::Invalid(_))
    ));
    assert_eq!(fixture.store.reads.load(Ordering::SeqCst) - reads, 3);
    let key = name_key(fixture.context.catalog, Some(parent.namespace), "child").unwrap();
    let StorageRecord::NamespaceMapping(mapping) =
        StorageRecord::decode(&key, &fixture.store.values.load()[&key.encode().unwrap()].bytes).unwrap()
    else {
        panic!()
    };
    assert_eq!(mapping.state, NamespaceMappingState::ReservedResource);
}

#[tokio::test]
async fn catalog_replacement_between_publication_and_response_is_still_rejected() {
    let fixture = TestNamespace::new().await;
    let parent = fixture.authority(None, &["parent"]);
    fixture.publish(&parent).await;
    let store = Arc::new(TestConflictStore {
        inner: fixture.store.clone(),
        interleave: TestInterleave::Clear,
        armed: AtomicBool::new(true),
    });
    assert!(matches!(
        NamespaceCreator::new(store).create(&request(&fixture)).await,
        Err(CatalogError::Conflict)
    ));
    assert!(matches!(
        NamespaceRepository::new(fixture.store.clone())
            .load(fixture.context, &request(&fixture).identifier)
            .await,
        Err(CatalogError::Conflict)
    ));
}
