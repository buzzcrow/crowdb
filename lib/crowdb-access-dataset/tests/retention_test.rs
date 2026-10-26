use crowdb_access_dataset::{
    FieldDefinition, FieldLocator, FieldRecord, ManifestRecord, OperationId, PublicationState,
    RetentionPlanner, SampleRecord, SchemaRecord, SnapshotId, SnapshotPublication, SnapshotRecord,
};
use std::collections::{HashMap, HashSet};

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
    let plan = RetentionPlanner::plan(&[record], &manifests, &HashSet::new(), &external).unwrap();
    assert_eq!(plan.reclaimable_chunks, vec![b"shared".to_vec()]);
}
