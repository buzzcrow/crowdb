// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/access_store.rs"]
mod access_store;

use std::sync::{atomic::Ordering, Arc};

#[path = "common/snapshot_resource.rs"]
#[allow(dead_code)]
mod resource;
use crowdb_access_dataset::*;
use md5::{Digest, Md5};

struct TestBatch {
    store: Arc<access_store::TestAccessStore>,
    authority: DatasetAuthority,
    identity: DatasetIdentity,
    chain: Vec<SnapshotId>,
    ids: Vec<Vec<u8>>,
}

fn manifest(generation: u8) -> Vec<u8> {
    let field = |name: &str, value| FieldRecord {
        name: name.into(),
        value,
    };
    let inline = |value: u8| FieldLocator::Inline {
        value: vec![value],
        md5: Md5::digest([value]).into(),
    };
    bincode::serialize(&ManifestRecord {
        version: 1,
        snapshot: SnapshotId::from_bytes(&[generation; 16]).unwrap(),
        schema: SchemaRecord {
            version: 1,
            fields: ["a", "b", "c"]
                .into_iter()
                .map(|name| FieldDefinition {
                    name: name.into(),
                    required: false,
                })
                .collect(),
        },
        samples: [b"s1".to_vec(), b"s2".to_vec()]
            .into_iter()
            .map(|sample_id| SampleRecord {
                sample_id,
                fields: match generation {
                    1 => vec![
                        field("a", inline(1)),
                        field("b", inline(2)),
                        field("c", inline(3)),
                    ],
                    2 => vec![field("b", FieldLocator::Tombstone), field("c", inline(4))],
                    _ => vec![field("a", inline(5))],
                },
            })
            .collect(),
    })
    .unwrap()
}

impl TestBatch {
    async fn new() -> Self {
        let store = access_store::TestAccessStore::shared();
        let authority = DatasetAuthority::new(store.clone());
        let identity = DatasetIdentity::new(NamespacePath::root(), "legacy-batch-cost").unwrap();
        authority.create_dataset(identity.clone()).await.unwrap();
        let mut chain = Vec::new();
        for generation in 1..=3 {
            let snapshot = authority
                .publish_snapshot(&identity, chain.last().copied(), manifest(generation))
                .await
                .unwrap();
            chain.push(snapshot);
        }
        Self {
            store,
            authority,
            identity,
            chain,
            ids: vec![b"s2".to_vec(), b"s1".to_vec(), b"s2".to_vec()],
        }
    }

    fn snapshot(&self) -> SnapshotId {
        *self.chain.last().unwrap()
    }

    async fn mark(&self, snapshot: SnapshotId) {
        let key = [
            b"DS\0L\0".as_slice(),
            self.identity.key().unwrap().as_slice(),
            snapshot.as_bytes(),
        ]
        .concat();
        let bytes = bincode::serialize(&(2u16, true, 0u64, 0u64, Vec::<(OperationId, u64)>::new())).unwrap();
        assert!(matches!(
            self.store.compare_exchange(&key, None, &bytes).await.unwrap(),
            CasOutcome::Applied(_)
        ));
    }
}

#[tokio::test]
async fn a_batch_shares_each_ancestry_admission_and_partition_without_changing_legacy_projection() {
    let f = TestBatch::new().await;
    let fields = ["a", "b", "c", "missing", "a"];
    let writes = f.store.writes.load(Ordering::Relaxed);
    let before = f.store.reads.load(Ordering::Relaxed);
    let mut reference = Vec::new();
    for id in &f.ids {
        reference.push(
            f.authority
                .resolve_projection(&f.identity, f.snapshot(), id, &fields)
                .await
                .unwrap(),
        );
    }
    let old_reads = f.store.reads.load(Ordering::Relaxed) - before;
    let before = f.store.reads.load(Ordering::Relaxed);
    let admissions = f.store.admission_reads.load(Ordering::Relaxed);
    let batch = f
        .authority
        .read_batch(&f.identity, f.snapshot(), &f.ids, &fields)
        .await
        .unwrap();
    let reads = f.store.reads.load(Ordering::Relaxed) - before;
    eprintln!("legacy batch: per-field traversal={old_reads} GET, request-local traversal={reads} GET");
    assert_eq!((old_reads, reads), (195, 15));
    assert_eq!(f.store.admission_reads.load(Ordering::Relaxed) - admissions, 3);
    for ((sample, id), fields) in batch.iter().zip(&f.ids).zip(reference) {
        assert_eq!(&sample.sample_id, id);
        assert_eq!(sample.fields, fields);
        assert_eq!(sample.values.len(), 2);
        assert_eq!(sample.values["a"], [5]);
        assert_eq!(sample.values["c"], [4]);
        assert!(!sample.fields.contains_key("b"));
    }
    let cancelled = ReadCancellation::new();
    assert_eq!(
        f.authority
            .read_batch_cancelled(&f.identity, f.snapshot(), &f.ids, &fields, &cancelled)
            .await
            .unwrap(),
        batch
    );
    cancelled.cancel();
    assert!(f
        .authority
        .read_batch_cancelled(&f.identity, f.snapshot(), &f.ids, &fields, &cancelled)
        .await
        .is_err());
    assert_eq!(f.store.writes.load(Ordering::Relaxed), writes);
}

#[tokio::test]
async fn empty_fields_keep_membership_validation_and_empty_batches_keep_snapshot_admission() {
    let f = TestBatch::new().await;
    let before = f.store.reads.load(Ordering::Relaxed);
    let result = f
        .authority
        .read_batch(&f.identity, f.snapshot(), &f.ids, &[])
        .await
        .unwrap();
    assert_eq!(
        result.iter().map(|sample| &sample.sample_id).collect::<Vec<_>>(),
        f.ids.iter().collect::<Vec<_>>()
    );
    assert!(result
        .iter()
        .all(|sample| sample.fields.is_empty() && sample.values.is_empty()));
    assert_eq!(f.store.reads.load(Ordering::Relaxed) - before, 5);
    assert!(matches!(
        f.authority
            .read_batch(&f.identity, f.snapshot(), &[b"absent".to_vec()], &[])
            .await,
        Err(AuthorityError::Invalid(DatasetError::SampleNotFound))
    ));
    f.mark(f.snapshot()).await;
    assert!(f
        .authority
        .read_batch(&f.identity, f.snapshot(), &[], &[])
        .await
        .is_err());
}

#[tokio::test]
async fn every_new_request_checks_ancestor_deletion_even_when_all_fields_are_on_the_child() {
    let f = TestBatch::new().await;
    f.authority
        .read_batch(&f.identity, f.snapshot(), &f.ids, &["a"])
        .await
        .unwrap();
    f.mark(f.chain[0]).await;
    let restarted = DatasetAuthority::new(f.store.clone());
    assert!(restarted
        .read_batch(&f.identity, f.snapshot(), &f.ids, &["a"])
        .await
        .is_err());
    assert!(f
        .authority
        .read_batch(&f.identity, f.snapshot(), &f.ids, &["a"])
        .await
        .is_err());
}

#[tokio::test]
async fn an_invalid_ancestor_and_a_missing_member_are_rejected_without_writes() {
    for cycle in [false, true] {
        let f = TestBatch::new().await;
        let ancestor = f.chain[0];
        resource::mutate(f.store.as_ref(), &f.identity, ancestor, |publication| {
            if cycle {
                publication.parent = Some(f.snapshot());
            } else {
                publication.state = PublicationState::Prepared;
            }
        })
        .await;
        let writes = f.store.writes.load(Ordering::Relaxed);
        assert!(f
            .authority
            .read_batch(&f.identity, f.snapshot(), &f.ids, &["a"])
            .await
            .is_err());
        assert_eq!(f.store.writes.load(Ordering::Relaxed), writes);
    }
    let f = TestBatch::new().await;
    assert!(matches!(
        f.authority
            .read_batch(&f.identity, f.snapshot(), &[b"absent".to_vec()], &["a"])
            .await,
        Err(AuthorityError::Invalid(DatasetError::SampleNotFound))
    ));
}
