// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;

use crowdb_access_iceberg::{
    key::{CatalogScope, IcebergKey, OperationId},
    namespace::{NamespaceCreateRequest, NamespaceCreator, NamespaceIdentifier, NamespaceProperties},
    operation::RequestIdentity,
};
use fixture::TestNamespace;
use std::sync::atomic::Ordering;

fn request(fixture: &TestNamespace, nested: bool) -> NamespaceCreateRequest {
    NamespaceCreateRequest {
        context: fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier: NamespaceIdentifier::new(if nested {
            vec!["parent".into(), "child".into()]
        } else {
            vec!["child".into()]
        })
        .unwrap(),
        properties: NamespaceProperties::default(),
    }
}

#[tokio::test]
async fn active_creates_write_only_final_resources_and_bounded_parent_occupancy() {
    for nested in [false, true] {
        let fixture = TestNamespace::new().await;
        if nested {
            fixture.publish(&fixture.authority(None, &["parent"])).await;
        }
        let writes = fixture.store.writes.load(Ordering::Relaxed);
        let reads = fixture.store.reads.load(Ordering::Relaxed);
        let records = fixture.store.values.load().len();
        let result = NamespaceCreator::new(fixture.store.clone())
            .create(&request(&fixture, nested))
            .await
            .unwrap();
        assert_eq!(result.status, 200);
        assert_eq!(
            fixture.store.writes.load(Ordering::Relaxed) - writes,
            if nested { 5 } else { 3 }
        );
        assert_eq!(
            fixture.store.reads.load(Ordering::Relaxed) - reads,
            if nested { 4 } else { 2 }
        );
        assert_eq!(fixture.store.values.load().len() - records, 2);
        for key in fixture.store.values.load().keys() {
            assert!(!matches!(
                IcebergKey::decode(key).unwrap(),
                IcebergKey::Catalog {
                    scope: CatalogScope::NamespaceOperation | CatalogScope::OperationPayload,
                    ..
                }
            ));
        }
    }
}
