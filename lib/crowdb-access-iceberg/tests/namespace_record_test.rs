use std::collections::BTreeMap;

use crowdb_access_iceberg::error::ValidationError;
use crowdb_access_iceberg::key::{CatalogId, CatalogScope, NamespaceId, OperationId};
use crowdb_access_iceberg::namespace::{
    authority_key, child_range, name_key, NamespaceAuthority, NamespaceIdentifier, NamespaceLifecycle,
    NamespaceMapping, NamespaceMappingState, NamespaceProperties,
};
use crowdb_access_iceberg::record::{StorageRecord, MAX_RECORD_BYTES};

fn authority() -> NamespaceAuthority {
    NamespaceAuthority {
        catalog: CatalogId::random(),
        namespace: NamespaceId::random(),
        parent: Some(NamespaceId::random()),
        identifier: NamespaceIdentifier::new(vec!["parent".into(), "child".into()]).unwrap(),
        name_epoch: 1,
        property_revision: 1,
        admission_fence: 1,
        mutation_revision: 1,
        lifecycle: NamespaceLifecycle::Ready,
        pending_operation: None,
        properties: NamespaceProperties::new(BTreeMap::from([("owner".into(), "数据库".into())])).unwrap(),
    }
}

fn mapping(authority: &NamespaceAuthority) -> NamespaceMapping {
    NamespaceMapping {
        catalog: authority.catalog,
        parent: authority.parent,
        name: authority.identifier.name().into(),
        namespace: authority.namespace,
        name_epoch: authority.name_epoch,
        operation: OperationId::random(),
        state: NamespaceMappingState::Published,
    }
}

#[test]
fn namespace_records_round_trip_with_strict_key_binding() {
    let authority = authority();
    let mapping = mapping(&authority);
    for (key, record, wrong_key) in [
        (
            authority_key(authority.catalog, authority.namespace),
            StorageRecord::NamespaceAuthority(Box::new(authority.clone())),
            authority_key(authority.catalog, NamespaceId::random()),
        ),
        (
            name_key(mapping.catalog, mapping.parent, &mapping.name).unwrap(),
            StorageRecord::NamespaceMapping(mapping.clone()),
            name_key(mapping.catalog, None, &mapping.name).unwrap(),
        ),
    ] {
        let bytes = record.encode().unwrap();
        assert_eq!(StorageRecord::decode(&key, &bytes).unwrap(), record);
        assert_eq!(
            StorageRecord::decode(&wrong_key, &bytes),
            Err(ValidationError::IdentityMismatch)
        );
        assert!(StorageRecord::decode(&key, &bytes[..bytes.len() / 2]).is_err());
    }
}

#[test]
fn mapping_validation_uses_identity_and_name_epoch_not_property_revision_or_fence() {
    let mut authority = authority();
    let mut mapping = mapping(&authority);
    assert!(mapping.resolves(&authority));
    authority.property_revision += 1;
    authority.admission_fence += 1;
    authority.mutation_revision += 2;
    authority.lifecycle = NamespaceLifecycle::Dropping;
    authority.pending_operation = Some(OperationId::random());
    assert!(mapping.resolves(&authority));
    authority.lifecycle = NamespaceLifecycle::Ready;
    authority.admission_fence += 1;
    assert!(mapping.resolves(&authority));
    authority.lifecycle = NamespaceLifecycle::Tombstone;
    assert!(!mapping.resolves(&authority));
    authority.lifecycle = NamespaceLifecycle::Ready;
    authority.namespace = NamespaceId::random();
    assert!(!mapping.resolves(&authority));
    mapping.namespace = authority.namespace;
    mapping.state = NamespaceMappingState::Reserved;
    assert!(!mapping.resolves(&authority));
    mapping.state = NamespaceMappingState::Published;
    mapping.name_epoch += 1;
    assert!(!mapping.resolves(&authority));
}

#[test]
fn namespace_authority_bounds_include_flatbuffer_overhead() {
    let mut authority = authority();
    authority.properties = NamespaceProperties::new(
        (0..8)
            .map(|index| (index.to_string(), "v".repeat(8191)))
            .collect(),
    )
    .unwrap();
    assert_eq!(
        StorageRecord::NamespaceAuthority(Box::new(authority.clone())).encode(),
        Err(ValidationError::RecordTooLarge)
    );
    authority.properties = NamespaceProperties::new(
        (0..7)
            .map(|index| (index.to_string(), "v".repeat(8192)))
            .collect(),
    )
    .unwrap();
    let record = StorageRecord::NamespaceAuthority(Box::new(authority.clone()));
    let bytes = record.encode().unwrap();
    assert!(bytes.len() < MAX_RECORD_BYTES);
    assert_eq!(
        StorageRecord::decode(&authority_key(authority.catalog, authority.namespace), &bytes).unwrap(),
        record
    );
    authority.lifecycle = NamespaceLifecycle::Dropping;
    assert_eq!(authority.validate(), Err(ValidationError::Record));
    authority.pending_operation = Some(OperationId::random());
    assert!(authority.validate().is_ok());
}

#[test]
fn child_ranges_are_parent_scoped_and_separate_namespace_and_table_indexes() {
    let catalog = CatalogId::random();
    for parent in [
        None,
        Some(NamespaceId::random()),
        Some(NamespaceId::from_bytes(&[255; 16]).unwrap()),
    ] {
        let range = child_range(catalog, parent, CatalogScope::NamespaceName).unwrap();
        for name in ["a", "longer", "数据库"] {
            let key = name_key(catalog, parent, name).unwrap().encode().unwrap();
            assert!(range.contains(&key));
            assert!(
                !child_range(CatalogId::random(), parent, CatalogScope::NamespaceName)
                    .unwrap()
                    .contains(&key)
            );
            assert!(
                !child_range(catalog, Some(NamespaceId::random()), CatalogScope::NamespaceName)
                    .unwrap()
                    .contains(&key)
            );
            if parent.is_some() {
                assert!(!child_range(catalog, parent, CatalogScope::TableName)
                    .unwrap()
                    .contains(&key));
            }
        }
    }
    assert!(child_range(catalog, None, CatalogScope::TableName).is_err());
    assert!(child_range(catalog, None, CatalogScope::NamespaceAuthority).is_err());
}

#[test]
fn lifecycle_and_operation_markers_preserve_authority_encoding_capacity() {
    let mut authority = authority();
    let ready_bytes = StorageRecord::NamespaceAuthority(Box::new(authority.clone()))
        .encode()
        .unwrap();
    for lifecycle in [
        NamespaceLifecycle::Ready,
        NamespaceLifecycle::Dropping,
        NamespaceLifecycle::Tombstone,
    ] {
        authority.lifecycle = lifecycle;
        authority.pending_operation = Some(OperationId::random());
        authority.admission_fence += 1;
        authority.mutation_revision += 1;
        let record = StorageRecord::NamespaceAuthority(Box::new(authority.clone()));
        let bytes = record.encode().unwrap();
        assert_eq!(bytes.len(), ready_bytes.len());
        assert_eq!(
            StorageRecord::decode(&authority_key(authority.catalog, authority.namespace), &bytes).unwrap(),
            record
        );
    }
}

#[test]
fn child_scans_reject_foreign_or_unbounded_continuations() {
    use crowdb_access_iceberg::namespace::ChildScan;
    use crowdb_chunk_kv_client::MultiScanContinuation;
    use crowdb_protocol::chunk_kv::ScanDirection;

    let catalog = CatalogId::random();
    let parent = Some(NamespaceId::random());
    let mut scan = ChildScan {
        catalog,
        parent,
        scope: CatalogScope::NamespaceName,
        limit: 1,
        continuation: None,
    };
    let request = scan.request().unwrap();
    scan.continuation = Some(MultiScanContinuation {
        direction: ScanDirection::Forward,
        original_start: request.start,
        original_end: request.end,
        last_key: name_key(catalog, parent, "last").unwrap().encode().unwrap(),
        catalog_generation: 1,
    });
    assert!(scan.request().is_ok());
    for limit in [0, 257, usize::MAX] {
        assert!(ChildScan {
            limit,
            ..scan.clone()
        }
        .request()
        .is_err());
    }
    assert!(ChildScan {
        catalog: CatalogId::random(),
        ..scan.clone()
    }
    .request()
    .is_err());
    assert!(ChildScan {
        parent: Some(NamespaceId::random()),
        ..scan.clone()
    }
    .request()
    .is_err());
    assert!(ChildScan {
        scope: CatalogScope::TableName,
        ..scan.clone()
    }
    .request()
    .is_err());
    scan.continuation.as_mut().unwrap().last_key = vec![0];
    assert!(scan.request().is_err());
}
