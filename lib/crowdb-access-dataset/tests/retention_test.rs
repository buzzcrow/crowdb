use crowdb_access_dataset::{
    FieldDefinition, FieldLocator, FieldRecord, ManifestRecord, OperationId, PublicationState,
    RetentionLeaseRegistry, RetentionPlanner, SampleRecord, SchemaRecord, SnapshotId, SnapshotPublication,
    SnapshotRecord,
};
use std::collections::{HashMap, HashSet};

#[test]
fn active_retention_lease_blocks_gc_until_release_and_ttl() {
    let registry = RetentionLeaseRegistry::new(60).unwrap();
    let lease = registry.acquire(100);
    assert!(registry.reclaim_allowed(1_000));
    drop(lease);
    assert!(registry.reclaim_allowed(159));
    assert!(registry.reclaim_allowed(160));
}

#[test]
fn owned_read_lease_releases_and_expiry_is_observable() {
    let registry = std::sync::Arc::new(RetentionLeaseRegistry::new(10).unwrap());
    let lease = crowdb_access_dataset::RetentionLeaseHandle::new(registry.clone(), 50);
    assert_eq!(registry.active(), 1);
    assert!(!registry.reclaim_allowed(55));
    drop(lease);
    assert_eq!(registry.active(), 0);
    assert!(registry.reclaim_allowed(55));
}

#[test]
fn retention_keeps_shared_and_external_chunks_until_unowned() {
    let old = SnapshotId::random();
    let manifest = ManifestRecord {
        version: 1,
        snapshot: old,
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "blob".into(),
                required: false,
            }],
        },
        samples: vec![SampleRecord {
            sample_id: b"s".to_vec(),
            fields: vec![FieldRecord {
                name: "blob".into(),
                value: FieldLocator::Chunk {
                    location: b"shared".to_vec(),
                    length: 1,
                    md5: [0; 16],
                },
            }],
        }],
    };
    let record = SnapshotRecord {
        publication: SnapshotPublication {
            snapshot: old,
            parent: None,
            operation: OperationId::random(),
            manifest: vec![1],
            state: PublicationState::Published,
        },
    };
    let mut manifests = HashMap::new();
    manifests.insert(old, manifest);
    let mut external = HashSet::new();
    external.insert(b"external".to_vec());
    let plan = RetentionPlanner::plan(&[record], &manifests, &HashSet::new(), false, &external).unwrap();
    assert_eq!(plan.reclaimable_chunks, vec![b"shared".to_vec()]);
}
