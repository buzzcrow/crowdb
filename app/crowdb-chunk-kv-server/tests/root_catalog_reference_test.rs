// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_server::storage::open_root_catalog_for_tests;
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, Id128, KeyRange, OwnerDescriptor,
    PartitionArtifact,
};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_tree_ffi::chunk::protection::{
    reference_checksum, CandidateState, ManifestCandidate, MAX_REFERENCE_WINDOW,
};
use crowdb_tree_ffi::chunk::stage::{StageId, StagePack, StageRecord, StageState};
use crowdb_tree_ffi::{CtError, RootCatalogObject, RootCatalogStore};
#[path = "common/metadata.rs"]
mod metadata;

fn entry(epoch: u64) -> ChunkKvRangeCatalogEntry {
    ChunkKvRangeCatalogEntry {
        partition_id: Id128 { high: 1, low: 1 },
        range: KeyRange {
            start: vec![],
            end: None,
        },
        owner: OwnerDescriptor {
            instance_id: epoch,
            rpc_endpoint: "127.0.0.1:1".into(),
        },
        owner_epoch: epoch,
        state: ChunkKvRangeCatalogPartitionState::Serving,
        artifact: PartitionArtifact {
            tree_id: 42,
            stream_name: StreamName { high: 1, low: 2 },
            tail_overlay: None,
        },
        transition_id: None,
    }
}
fn id(low: u64) -> StageId {
    StageId {
        owner_epoch: 1,
        operation_high: 100,
        operation_low: low,
    }
}
fn next(old: &StageRecord, state: StageState) -> StageRecord {
    let mut value = old.clone();
    value.state = state;
    value.revision += 1;
    value
}
fn ready(store: &dyn RootCatalogStore, low: u64) -> StageRecord {
    let reserved = StageRecord {
        tree_id: 42,
        id: id(low),
        revision: 1,
        chunk_high: 2_u64 << 56,
        chunk_low: low,
        capacity: 4096,
        purpose: 2,
        state: StageState::Reserved,
        manifest_generation: 0,
        packs: vec![],
    };
    store.compare_stage(42, 1, None, Some(&reserved)).unwrap();
    let mut writing = next(&reserved, StageState::Writing);
    writing.packs.push(StagePack {
        offset: 0,
        length: 4000,
        checksum: 7,
    });
    store
        .compare_stage(42, 1, Some(&reserved), Some(&writing))
        .unwrap();
    let ready = next(&writing, StageState::Reloadable);
    store.compare_stage(42, 1, Some(&writing), Some(&ready)).unwrap();
    ready
}
fn candidate(low: u64, generation: u64, refs: &[StageId]) -> ManifestCandidate {
    ManifestCandidate {
        id: id(low),
        expected_generation: generation - 1,
        reference_count: refs.len() as u64,
        reference_checksum: reference_checksum(0, refs),
        manifest: format!("fixed manifest {low}").into_bytes(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_reference_windows_prepare_and_release_with_native_metadata_admission() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    let stages: Vec<_> = (1..=MAX_REFERENCE_WINDOW as u64 + 1)
        .map(|low| ready(store.as_ref(), low))
        .collect();
    let refs: Vec<_> = stages.iter().map(|stage| stage.id).collect();
    let protection = store.stage_protection().unwrap();
    // Exercise both a full final window and a full non-final window.
    for (index, refs) in [&refs[..MAX_REFERENCE_WINDOW], refs.as_slice()]
        .into_iter()
        .enumerate()
    {
        let plan = candidate(100 + index as u64, 1, refs);
        protection.begin_manifest_candidate(42, &plan).unwrap();
        for (window, refs) in refs.chunks(MAX_REFERENCE_WINDOW).enumerate() {
            protection
                .prepare_manifest_references(42, plan.id, window as u64, refs)
                .unwrap();
        }
        protection.abort_manifest_candidate(42, plan.id).unwrap();
        for window in 0..refs.len().div_ceil(MAX_REFERENCE_WINDOW) {
            protection
                .release_manifest_references(42, plan.id, window as u64)
                .unwrap();
        }
    }
    for stage in stages {
        let retired = next(&stage, StageState::Retired);
        store.compare_stage(42, 1, Some(&stage), Some(&retired)).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generation_reclamation_advances_within_complete_transaction_limit() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    let protection = store.stage_protection().unwrap();
    for generation in 1..=70 {
        let plan = candidate(100 + generation, generation, &[]);
        protection.begin_manifest_candidate(42, &plan).unwrap();
        protection.commit_manifest_candidate(42, plan.id).unwrap();
    }
    let mut reclaimed = 0;
    while reclaimed < 68 {
        let count = store.reclaim_before(42, 70);
        assert!(count > 0, "reclamation stopped at generation {reclaimed}");
        reclaimed += count;
    }
    assert_eq!(reclaimed, 68);
    assert_eq!(store.reclaim_before(42, 70), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_references_protect_before_commit_and_survive_unknown_commit_reply() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    let first = ready(store.as_ref(), 1);
    let second = ready(store.as_ref(), 2);
    let refs = [first.id, second.id];
    let plan = candidate(10, 1, &refs);
    let protection = store.stage_protection().unwrap();
    protection.begin_manifest_candidate(42, &plan).unwrap();
    protection
        .prepare_manifest_references(42, plan.id, 0, &refs[..1])
        .unwrap();
    let retired = next(&first, StageState::Retired);
    assert_eq!(
        store.compare_stage(42, 1, Some(&first), Some(&retired)),
        Err(CtError::Unavailable)
    );
    assert_eq!(
        protection.commit_manifest_candidate(42, plan.id),
        Err(CtError::Unavailable)
    );
    protection
        .prepare_manifest_references(42, plan.id, 1, &refs[1..])
        .unwrap();
    protection
        .prepare_manifest_references(42, plan.id, 1, &refs[1..])
        .unwrap();
    // Discard the result, query exactly this operation, then repeat it.
    protection.commit_manifest_candidate(42, plan.id).unwrap();
    assert_eq!(
        protection
            .load_manifest_candidate(42, plan.id)
            .unwrap()
            .unwrap()
            .state,
        CandidateState::Committed
    );
    protection.commit_manifest_candidate(42, plan.id).unwrap();
    assert_eq!(
        protection.abort_manifest_candidate(42, plan.id),
        Err(CtError::Unavailable)
    );
    assert_eq!(
        store.load(42, RootCatalogObject::CurrentManifest).unwrap(),
        Some(plan.manifest)
    );
    assert_eq!(protection.list_manifest_candidates(42, None, 1).unwrap().len(), 1);
    assert!(protection
        .list_manifest_candidates(42, Some(plan.id), 1)
        .unwrap()
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generation_pin_and_live_pin_both_prevent_pack_deletion() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    let first = ready(store.as_ref(), 1);
    let protection = store.stage_protection().unwrap();
    let plan = candidate(10, 1, &[first.id]);
    protection.begin_manifest_candidate(42, &plan).unwrap();
    protection
        .prepare_manifest_references(42, plan.id, 0, &[first.id])
        .unwrap();
    protection.commit_manifest_candidate(42, plan.id).unwrap();
    store.pin_generation(42, 1, 99, 1).unwrap();
    protection.pin_stage(42, first.id, id(200)).unwrap();
    for generation in 2..=4 {
        store
            .publish(42, generation - 1, 1, generation, b"other root")
            .unwrap();
    }
    assert_eq!(store.reclaim_before(42, 4), 0);
    assert_eq!(
        protection.release_manifest_references(42, plan.id, 0),
        Err(CtError::Unavailable)
    );
    store.unpin_generation(42, 1, 99).unwrap();
    assert_eq!(store.reclaim_before(42, 4), 2);
    assert_eq!(
        protection
            .load_manifest_candidate(42, plan.id)
            .unwrap()
            .unwrap()
            .state,
        CandidateState::Reclaimed
    );
    protection.release_manifest_references(42, plan.id, 0).unwrap();
    protection.release_manifest_references(42, plan.id, 0).unwrap();
    let retired = next(&first, StageState::Retired);
    assert_eq!(
        store.compare_stage(42, 1, Some(&first), Some(&retired)),
        Err(CtError::Unavailable)
    );
    protection.unpin_stage(42, first.id, id(200)).unwrap();
    store.compare_stage(42, 1, Some(&first), Some(&retired)).unwrap();
    let deleting = next(&retired, StageState::Deleting);
    store
        .compare_stage(42, 1, Some(&retired), Some(&deleting))
        .unwrap();
    assert_eq!(
        protection.pin_stage(42, first.id, id(201)),
        Err(CtError::Unavailable)
    );
    let new_plan = candidate(11, 5, &[first.id]);
    protection.begin_manifest_candidate(42, &new_plan).unwrap();
    assert_eq!(
        protection.prepare_manifest_references(42, new_plan.id, 0, &[first.id]),
        Err(CtError::Unavailable)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_releases_only_prepared_windows_and_fixed_identity_cannot_be_reallocated() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    let first = ready(store.as_ref(), 1);
    let second = ready(store.as_ref(), 2);
    let protection = store.stage_protection().unwrap();
    let plan = candidate(10, 1, &[first.id, second.id]);
    protection.begin_manifest_candidate(42, &plan).unwrap();
    protection
        .prepare_manifest_references(42, plan.id, 0, &[first.id])
        .unwrap();
    protection.abort_manifest_candidate(42, plan.id).unwrap();
    assert_eq!(
        protection.prepare_manifest_references(42, plan.id, 1, &[second.id]),
        Err(CtError::Unavailable)
    );
    protection.release_manifest_references(42, plan.id, 0).unwrap();
    let mut last = first;
    for state in [StageState::Retired, StageState::Deleting, StageState::Reclaimed] {
        let value = next(&last, state);
        store.compare_stage(42, 1, Some(&last), Some(&value)).unwrap();
        last = value;
    }
    store.compare_stage(42, 1, Some(&last), None).unwrap();
    let duplicate = StageRecord {
        id: id(999),
        revision: 1,
        state: StageState::Reserved,
        packs: vec![],
        ..last
    };
    assert_eq!(
        store.compare_stage(42, 1, None, Some(&duplicate)),
        Err(CtError::Unavailable)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checksum_failure_does_not_publish_protection_and_released_pin_never_resurrects() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    let first = ready(store.as_ref(), 1);
    let protection = store.stage_protection().unwrap();
    let mut plan = candidate(10, 1, &[first.id]);
    plan.reference_checksum ^= 1;
    protection.begin_manifest_candidate(42, &plan).unwrap();
    assert_eq!(
        protection.prepare_manifest_references(42, plan.id, 0, &[first.id]),
        Err(CtError::Corruption)
    );
    protection.unpin_stage(42, first.id, id(100)).unwrap();
    assert_eq!(
        protection.pin_stage(42, first.id, id(100)),
        Err(CtError::Unavailable)
    );
    let retired = next(&first, StageState::Retired);
    store.compare_stage(42, 1, Some(&first), Some(&retired)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_pin_invalidates_an_observed_deletion_proof_at_the_authority_revision() {
    use bytes::Bytes;
    use crowdb_paxos_kv_client::{BatchOp, GetOutcome, ReadMode};
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    let first = ready(store.as_ref(), 1);
    let mut prefix = b"\0crowdb/chunk-kv/root/v1/".to_vec();
    prefix.extend_from_slice(&42_u64.to_be_bytes());
    let mut authority = prefix.clone();
    authority.extend_from_slice(b"/authority/");
    authority.extend_from_slice(&0_u64.to_be_bytes());
    let GetOutcome::Found { value, revision } = metadata
        .client
        .get(0, 1, &authority, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("authority must exist");
    };
    store
        .stage_protection()
        .unwrap()
        .pin_stage(42, first.id, id(200))
        .unwrap();
    let mut stage_key = prefix;
    stage_key.extend_from_slice(b"/stage/v1/");
    for part in [
        first.id.owner_epoch,
        first.id.operation_high,
        first.id.operation_low,
    ] {
        stage_key.extend_from_slice(&part.to_be_bytes());
    }
    let ops = [
        BatchOp::Put {
            key: Bytes::from(authority.clone()),
            value,
        },
        BatchOp::Put {
            key: Bytes::from(stage_key),
            value: Bytes::from(next(&first, StageState::Retired).encode().unwrap()),
        },
    ];
    assert!(matches!(
        metadata
            .client
            .batch_write_cas(0, 1, &ops, &authority, revision)
            .await,
        Err(crowdb_paxos_kv_client::Error::CasFailed { .. } | crowdb_paxos_kv_client::Error::CasBusy)
    ));
    assert_eq!(store.load_stage(42, first.id).unwrap(), Some(first));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn candidate_decode_rejects_oversized_checksum_words_even_with_valid_record_crc() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    let candidate_id = id(50);
    let mut key = b"\0crowdb/chunk-kv/root/v1/".to_vec();
    key.extend_from_slice(&42_u64.to_be_bytes());
    key.extend_from_slice(b"/candidate/");
    for value in [
        candidate_id.owner_epoch,
        candidate_id.operation_high,
        candidate_id.operation_low,
    ] {
        key.extend_from_slice(&value.to_be_bytes());
    }
    for checksum_index in 7..10 {
        let mut words = [0_u64; 14];
        words[..3].copy_from_slice(&[1, 2, 1]);
        words[checksum_index] = u64::from(u32::MAX) + 1;
        let mut bytes = words.into_iter().flat_map(u64::to_le_bytes).collect::<Vec<_>>();
        bytes.extend_from_slice(&crowdb_tree_ffi::crc32c(&bytes).to_le_bytes());
        metadata.client.put(0, 1, &key, &bytes, None).await.unwrap();
        assert_eq!(
            store
                .stage_protection()
                .unwrap()
                .load_manifest_candidate(42, candidate_id),
            Err(CtError::Corruption),
            "oversized checksum word {checksum_index} must not truncate"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pin_mutations_rebuild_guard_counts_before_gc() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    let stage = ready(store.as_ref(), 1);
    let retired = next(&stage, StageState::Retired);
    for turn in 0..4 {
        let pins = [id(100 + 2 * turn), id(101 + 2 * turn)];
        let start = std::sync::Arc::new(std::sync::Barrier::new(2));
        let jobs = pins.map(|pin| {
            let catalog = store.clone();
            let start = start.clone();
            let stage_id = stage.id;
            tokio::task::spawn_blocking(move || {
                start.wait();
                catalog.stage_protection().unwrap().pin_stage(42, stage_id, pin)
            })
        });
        for job in jobs {
            job.await.unwrap().unwrap();
        }
        store
            .stage_protection()
            .unwrap()
            .unpin_stage(42, stage.id, pins[0])
            .unwrap();
        assert_eq!(
            store.compare_stage(42, 1, Some(&stage), Some(&retired)),
            Err(CtError::Unavailable),
            "one remaining live pin must retain its exact guard count"
        );
        store
            .stage_protection()
            .unwrap()
            .unpin_stage(42, stage.id, pins[1])
            .unwrap();
    }
    store.compare_stage(42, 1, Some(&stage), Some(&retired)).unwrap();
    assert_eq!(store.load_stage(42, stage.id).unwrap(), Some(retired));
}
