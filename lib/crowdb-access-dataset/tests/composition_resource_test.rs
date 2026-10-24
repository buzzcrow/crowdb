// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/composition.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::*, split::*, *};
use std::sync::atomic::Ordering;

fn request(parent: SnapshotId) -> CompositionRequest {
    CompositionRequest {
        parent: Some(parent),
        source: CompositionSource::All,
        schema: None,
        samples: vec![
            SampleChange::Remove(vec![0, 0, 255]),
            SampleChange::Upsert(common::sample(b"new", 9)),
        ],
        splits: split_store::partition_splits(),
        policy: SplitPolicy::default(),
    }
}
fn counts(store: &split_store::TestDraftStore) -> (usize, usize, usize) {
    (
        store.bounded_reads.load(Ordering::Relaxed) + store.unbounded_reads.load(Ordering::Relaxed),
        store.metadata_scans.load(Ordering::Relaxed),
        store.mutations.load(Ordering::Relaxed),
    )
}
fn delta(store: &split_store::TestDraftStore, old: (usize, usize, usize)) -> (usize, usize, usize) {
    let now = counts(store);
    (now.0 - old.0, now.1 - old.1, now.2 - old.2)
}

#[tokio::test]
async fn complete_input_replaces_workflow_writes_and_keeps_the_same_manifest_and_members() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let parent = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let input = request(parent);
    let mut expected = authority.get_manifest(&identity, parent).await.unwrap().samples;
    expected.retain(|sample| sample.sample_id != [0, 0, 255]);
    expected.push(common::sample(b"new", 9));
    expected.sort_by(|a, b| a.sample_id.cmp(&b.sample_id));
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    let before = counts(&store);
    let head_reads = store.data_head_reads.load(Ordering::Relaxed);
    let snapshot = SnapshotId::random();
    let result = authority
        .prepare_composition_resource(&identity, snapshot, &input, None, &ReadCancellation::new())
        .await
        .unwrap();
    let new_cost = delta(&store, before);
    assert_eq!(store.data_head_reads.load(Ordering::Relaxed), head_reads);
    assert_eq!(result.snapshot, snapshot);
    assert_eq!(result.sample_count, 45);
    assert_eq!(new_cost, (187, 14, 153));
    assert_eq!(
        authority.get_manifest(&identity, snapshot).await.unwrap().samples,
        expected
    );
    let initial = result.initial_split.unwrap();
    let split = authority
        .get_split_revision(
            &identity,
            snapshot,
            initial,
            common::budget(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(split.splits.iter().map(|split| split.count).sum::<u64>(), 45);
    assert_eq!(
        authority
            .inspect_split_head(&identity, snapshot)
            .await
            .unwrap()
            .revision,
        Some(initial)
    );
    assert_eq!(
        authority.inspect_head(&identity).await.unwrap().latest,
        Some(parent)
    );
    let before = store.mutations.load(Ordering::Relaxed);
    assert_eq!(
        DatasetAuthority::new(store.clone())
            .prepare_composition_resource(&identity, snapshot, &input, None, &ReadCancellation::new())
            .await
            .unwrap(),
        result
    );
    assert_eq!(store.mutations.load(Ordering::Relaxed), before);
    let mut changed = input.clone();
    changed.splits.clear();
    assert!(authority
        .prepare_composition_resource(&identity, snapshot, &changed, None, &ReadCancellation::new())
        .await
        .is_err());
    changed = input;
    changed.samples[1] = SampleChange::Upsert(common::sample(b"new", 10));
    assert!(authority
        .prepare_composition_resource(&identity, snapshot, &changed, None, &ReadCancellation::new())
        .await
        .is_err());
    assert_eq!(store.mutations.load(Ordering::Relaxed), before);
}

#[tokio::test]
async fn source_filter_and_sparse_patch_preserve_unmodified_parent_fields() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    let mut input = request(parent);
    input.source = CompositionSource::Selection(Selection::Prefix(b"excluded".to_vec()));
    input.splits.clear();
    input.samples = vec![SampleChange::Upsert(SampleRecord {
        sample_id: b"sample-1".to_vec(),
        fields: Vec::new(),
    })];
    let snapshot = SnapshotId::random();
    authority
        .prepare_composition_resource(&identity, snapshot, &input, None, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(
        authority.get_manifest(&identity, snapshot).await.unwrap().samples,
        vec![common::sample(b"sample-1", 1)]
    );
}

#[tokio::test]
async fn frozen_source_and_inherited_members_ignore_later_split_heads() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let parent = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let original = authority
        .prepare_split_revision(
            &identity,
            parent,
            &SplitPreparation {
                revision: SplitRevisionId::random(),
                policy: SplitPolicy::default(),
                splits: vec![DraftSplit {
                    name: SplitName::new("fixed").unwrap(),
                    definition: SplitDefinition::Selection(Selection::Explicit(vec![vec![0, 0, 255]])),
                    origin: MembershipOrigin::Evaluated,
                }],
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    let source = SplitSource {
        snapshot: parent,
        revision: original.revision,
        name: original.splits[0].name.clone(),
    };
    let later = split_store::prepared(&authority, &identity, parent, None).await;
    authority
        .update_split_head(
            &identity,
            parent,
            &SplitHeadUpdate {
                revision: later.revision,
                expected_revision: None,
                expected_head: None,
            },
        )
        .await
        .unwrap();
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    let input = CompositionRequest {
        parent: Some(parent),
        source: CompositionSource::Split {
            source: source.clone(),
            selection: None,
        },
        schema: None,
        samples: vec![SampleChange::Upsert(common::sample(b"new", 10))],
        splits: vec![DraftSplit {
            name: source.name.clone(),
            definition: original.splits[0].definition.clone(),
            origin: MembershipOrigin::Inherited(source),
        }],
        policy: SplitPolicy::default(),
    };
    let result = authority
        .prepare_composition_resource(
            &identity,
            SnapshotId::random(),
            &input,
            None,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.sample_count, 2);
    let revision = authority
        .get_split_revision(
            &identity,
            result.snapshot,
            result.initial_split.unwrap(),
            common::budget(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(revision.splits[0].count, 1);
    assert_eq!(
        authority
            .split_members_page(
                &identity,
                result.snapshot,
                revision.revision,
                &revision.splits[0].name,
                None,
                common::budget(),
                &ReadCancellation::new()
            )
            .await
            .unwrap()
            .entries,
        vec![vec![0, 0, 255]]
    );
}

#[tokio::test]
async fn invalid_cancelled_and_oversized_inputs_cannot_write_a_resource() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    let before = counts(&store);
    let input = request(parent);
    let cancel = ReadCancellation::new();
    cancel.cancel();
    assert!(authority
        .prepare_composition_resource(&identity, SnapshotId::random(), &input, None, &cancel)
        .await
        .is_err());
    assert_eq!(counts(&store), before);
    let mut invalid = input.clone();
    invalid.samples.push(invalid.samples[0].clone());
    assert!(authority
        .prepare_composition_resource(
            &identity,
            SnapshotId::random(),
            &invalid,
            None,
            &ReadCancellation::new()
        )
        .await
        .is_err());
    invalid = input;
    invalid.samples = vec![SampleChange::Remove(vec![1; 9 * 1024 * 1024])];
    assert!(matches!(
        authority
            .prepare_composition_resource(
                &identity,
                SnapshotId::random(),
                &invalid,
                None,
                &ReadCancellation::new()
            )
            .await,
        Err(CompositionError::Limit)
    ));
    assert_eq!(counts(&store), before);
}

#[tokio::test]
async fn removing_schema_fields_and_incomplete_new_samples_cannot_create_a_resource() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    let before = counts(&store);
    let mut input = request(parent);
    input.splits.clear();
    input.schema = Some(SchemaRecord {
        version: 2,
        fields: vec![],
    });
    let removed = SnapshotId::random();
    assert!(authority
        .prepare_composition_resource(&identity, removed, &input, None, &ReadCancellation::new(),)
        .await
        .is_err());
    assert_eq!(counts(&store).2, before.2);
    input.schema = None;
    input.samples = vec![SampleChange::Upsert(SampleRecord {
        sample_id: b"new".to_vec(),
        fields: vec![],
    })];
    let incomplete = SnapshotId::random();
    assert!(authority
        .prepare_composition_resource(&identity, incomplete, &input, None, &ReadCancellation::new(),)
        .await
        .is_err());
    assert_eq!(counts(&store).2, before.2);
    for snapshot in [removed, incomplete] {
        assert!(matches!(
            authority.get_snapshot(&identity, snapshot).await,
            Err(AuthorityError::SnapshotNotFound)
        ));
    }
}
