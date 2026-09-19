// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::common::config::CrowDBConfig;
use crowdb_kv_server::background::domain_monitor::{
    spawn_domain_monitor_supervisor, ChunkdbRangeMonitorDriver, DiskdbOwnershipMonitorDriver,
    DomainMonitorDriver, DomainMonitorDrivers, DomainMonitorFuture,
};
use crowdb_kv_server::group0_control_plane::Group0ControlPlane;
use crowdb_kv_server::store_registry::KvStoreRegistry;
use crowdb_protocol::chunk_kv::{
    AuthorityReleaseProof, ChunkKvRangeBalancePolicy, ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead,
    ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef, ChunkKvRangeCatalogPartitionState, HostedPartition,
    Id128, KeyRange, OwnerDescriptor, PartitionArtifact, ServingGrant, SplitPhase, SplitTransition,
    TailOverlayArtifact, TargetReadinessProof, TransferPhase, TransferReadinessLimits, TransferTransition,
};
use crowdb_protocol::chunk_kv::{DomainFailurePolicy, DomainMonitorDescriptor};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::common::{ChunkKvExtra, ChunkKvPartitionLoad, InstanceValue, ServiceExtra};
use crowdb_protocol::key::{
    ChunkKvRangeCatalogHeadKey, ChunkKvRangeCatalogPageKey, ChunkKvSplitKey, ChunkKvTransferKey,
    ChunkdbRangeBindingKey, DomainMonitorKey, InstanceKey, ServingGrantKey, TextKey,
};

struct CountingDriver {
    ticks: Arc<AtomicUsize>,
}

impl DomainMonitorDriver for CountingDriver {
    fn domain(&self) -> &'static str {
        "test-domain"
    }

    fn driver_version(&self) -> u32 {
        1
    }

    fn tick<'a>(
        &'a self,
        _control: &'a Group0ControlPlane,
        _descriptor: &'a DomainMonitorDescriptor,
    ) -> DomainMonitorFuture<'a> {
        Box::pin(async move {
            self.ticks.fetch_add(1, Ordering::AcqRel);
            Ok(())
        })
    }
}

fn descriptor() -> DomainMonitorDescriptor {
    DomainMonitorDescriptor {
        domain: "test-domain".into(),
        service_registry_name: "test-service".into(),
        driver_version: 1,
        capability_version: 1,
        heartbeat_interval_ms: 10,
        suspect_after_ms: 30,
        dead_after_ms: 50,
        lease_duration_ms: 70,
        max_clock_skew_ms: 5,
        self_fence_margin_ms: 5,
        failure_policy: DomainFailurePolicy::AutomaticSharedStorage,
        balance_policy: "test-v1".into(),
        chunk_kv_range_balance: None,
    }
}

fn registry_with_group_zero(role: PxLocalReplicaRole) -> (Arc<KvStoreRegistry>, Arc<PxKvStore>) {
    let registry = Arc::new(KvStoreRegistry::with_config(CrowDBConfig::for_tests()));
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(0, PxLocalReplica::new(1, role)));
    registry.add_store(0, &store);
    (registry, store)
}

#[tokio::test]
async fn persisted_descriptor_starts_one_leader_fenced_driver() {
    let (registry, store) = registry_with_group_zero(PxLocalReplicaRole::Leader);
    let descriptor = descriptor();
    let key = DomainMonitorKey {
        domain: descriptor.domain.clone(),
    }
    .to_path();
    Group0ControlPlane::acquire(&store)
        .await
        .unwrap()
        .compare_and_put(
            Bytes::from(key),
            Bytes::from(serde_json::to_vec(&descriptor).unwrap()),
            0,
        )
        .await
        .unwrap();

    let ticks = Arc::new(AtomicUsize::new(0));
    let handle = spawn_domain_monitor_supervisor(
        registry,
        DomainMonitorDrivers::new(vec![Arc::new(CountingDriver {
            ticks: Arc::clone(&ticks),
        })]),
        Duration::from_millis(5),
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while ticks.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    handle.stop_and_wait().await;
    assert!(ticks.load(Ordering::Acquire) >= 1);
}

#[tokio::test]
async fn follower_discovers_descriptor_but_driver_remains_idle() {
    let (registry, store) = registry_with_group_zero(PxLocalReplicaRole::Leader);
    let descriptor = descriptor();
    let key = DomainMonitorKey {
        domain: descriptor.domain.clone(),
    }
    .to_path();
    Group0ControlPlane::acquire(&store)
        .await
        .unwrap()
        .compare_and_put(
            Bytes::from(key),
            Bytes::from(serde_json::to_vec(&descriptor).unwrap()),
            0,
        )
        .await
        .unwrap();
    store.get_group(0).unwrap().local_replica().become_follower(1);

    let ticks = Arc::new(AtomicUsize::new(0));
    let handle = spawn_domain_monitor_supervisor(
        registry,
        DomainMonitorDrivers::new(vec![Arc::new(CountingDriver {
            ticks: Arc::clone(&ticks),
        })]),
        Duration::from_millis(5),
    );
    tokio::time::sleep(Duration::from_millis(60)).await;
    handle.stop_and_wait().await;
    assert_eq!(ticks.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn chunkdb_driver_reads_and_writes_through_local_group_zero() {
    let (_, store) = registry_with_group_zero(PxLocalReplicaRole::Leader);
    let control = Group0ControlPlane::acquire(&store).await.unwrap();
    let mut descriptor = descriptor();
    descriptor.domain = "chunkdb".into();
    descriptor.service_registry_name = "chunkdb".into();
    let instance = InstanceValue {
        instance_id: 7,
        rpc_endpoint: "127.0.0.1:17007".into(),
        last_heartbeat_ms: u64::MAX,
        extra: Some(ServiceExtra::default()),
    };
    control
        .compare_and_put(
            Bytes::from(
                InstanceKey {
                    service: "chunkdb".into(),
                    instance_id: 7,
                }
                .to_path(),
            ),
            Bytes::from(serde_json::to_vec(&instance).unwrap()),
            0,
        )
        .await
        .unwrap();

    ChunkdbRangeMonitorDriver::with_sub_range_count(4)
        .tick(&control, &descriptor)
        .await
        .unwrap();
    let bindings = control
        .scan_all_prefix(Bytes::from(ChunkdbRangeBindingKey::text_prefix_all()), 16)
        .await
        .unwrap();
    assert_eq!(bindings.len(), 4);
}

#[tokio::test]
async fn diskdb_operator_only_driver_observes_expired_instances_without_reassignment() {
    let (_, store) = registry_with_group_zero(PxLocalReplicaRole::Leader);
    let control = Group0ControlPlane::acquire(&store).await.unwrap();
    let mut descriptor = descriptor();
    descriptor.domain = "diskdb".into();
    descriptor.service_registry_name = "diskdb".into();
    descriptor.failure_policy = DomainFailurePolicy::OperatorOnly;
    let instance = InstanceValue {
        instance_id: 8,
        rpc_endpoint: "127.0.0.1:17008".into(),
        last_heartbeat_ms: 1,
        extra: Some(ServiceExtra::default()),
    };
    put_json(
        &control,
        InstanceKey {
            service: "diskdb".into(),
            instance_id: 8,
        }
        .to_path(),
        &instance,
    )
    .await;

    DiskdbOwnershipMonitorDriver::new()
        .tick(&control, &descriptor)
        .await
        .unwrap();
    assert!(control
        .scan_all_prefix(Bytes::from("/diskdb/"), 16)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn chunk_kv_driver_publishes_ready_transfer_then_issues_matching_grant() {
    use crowdb_kv_server::background::domain_monitor::ChunkKvRangeMonitorDriver;

    let (_, store) = registry_with_group_zero(PxLocalReplicaRole::Leader);
    let control = Group0ControlPlane::acquire(&store).await.unwrap();
    let partition_id = Id128 { high: 1, low: 1 };
    let transition_id = Id128 { high: 9, low: 9 };
    let artifact = PartitionArtifact {
        tree_id: 11,
        stream_name: StreamName { high: 12, low: 13 },
        tail_overlay: None,
    };
    let source = OwnerDescriptor {
        instance_id: 1,
        rpc_endpoint: "127.0.0.1:17001".into(),
    };
    let target = OwnerDescriptor {
        instance_id: 2,
        rpc_endpoint: "127.0.0.1:17002".into(),
    };
    let mut page = ChunkKvRangeCatalogPage {
        generation: 1,
        page_index: 0,
        entries: vec![ChunkKvRangeCatalogEntry {
            partition_id,
            range: KeyRange {
                start: Vec::new(),
                end: None,
            },
            owner: source.clone(),
            owner_epoch: 1,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: artifact.clone(),
            transition_id: None,
        }],
        checksum: [0; 32],
    };
    page.seal().unwrap();
    let mut head = ChunkKvRangeCatalogHead {
        generation: 1,
        previous_generation: None,
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: 1,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    put_json(
        &control,
        ChunkKvRangeCatalogPageKey {
            generation: 1,
            page_index: 0,
        }
        .to_path(),
        &page,
    )
    .await;
    put_json(&control, ChunkKvRangeCatalogHeadKey.to_path(), &head).await;

    let mut target_artifact = artifact.clone();
    target_artifact.stream_name = StreamName { high: 40, low: 41 };
    target_artifact.tail_overlay = Some(TailOverlayArtifact {
        source_partition_id: partition_id,
        source_epoch: 1,
        source_stream_name: artifact.stream_name,
        source_stream_manifest_generation: 1,
        replay_offset: 0,
        cutover_offset: 7,
        base_root_manifest_generation: 1,
        base_tree_manifest: 1,
        base_applied_seq: 0,
        cutover_seq: 7,
        target_stream_start_seq: 8,
    });
    let transition = TransferTransition {
        transition_id,
        partition_id,
        range: KeyRange {
            start: Vec::new(),
            end: None,
        },
        source,
        source_epoch: 1,
        target: target.clone(),
        target_epoch: 2,
        artifact: artifact.clone(),
        target_artifact: target_artifact.clone(),
        readiness_limits: TransferReadinessLimits {
            max_tail_records: 100,
            max_tail_bytes: 1_000_000,
            max_estimated_catchup_ms: 1_000,
            prepare_deadline_ms: 1_000,
            forwarding_grace_ms: 1_000,
        },
        planned_at_ms: 0,
        old_grant_expires_at_ms: 1,
        phase: TransferPhase::TargetPrepared,
        release_proof: None,
        readiness_proof: Some(TargetReadinessProof {
            target_instance_id: 2,
            target_epoch: 2,
            artifact: target_artifact.clone(),
            durable_tail: 7,
        }),
        catchup_proof: None,
        failure: None,
    };
    put_json(
        &control,
        ChunkKvTransferKey { transition_id }.to_path(),
        &transition,
    )
    .await;
    put_json(
        &control,
        InstanceKey {
            service: "chunk-kv".into(),
            instance_id: 1,
        }
        .to_path(),
        &InstanceValue {
            instance_id: 1,
            rpc_endpoint: "127.0.0.1:17001".into(),
            last_heartbeat_ms: u64::MAX,
            extra: Some(ServiceExtra {
                chunk_kv: Some(ChunkKvExtra {
                    hosted: vec![HostedPartition {
                        partition_id,
                        owner_epoch: 1,
                        recovering: false,
                    }],
                    ..ChunkKvExtra::default()
                }),
                ..ServiceExtra::default()
            }),
        },
    )
    .await;
    let mut policy = descriptor();
    policy.domain = "chunk-kv".into();
    policy.service_registry_name = "chunk-kv".into();
    ChunkKvRangeMonitorDriver::new()
        .tick(&control, &policy)
        .await
        .unwrap();
    let transition_path = ChunkKvTransferKey { transition_id }.to_path();
    let waiting = control.get(transition_path.as_bytes()).await.unwrap();
    let waiting: TransferTransition = serde_json::from_slice(waiting.value.as_ref().unwrap()).unwrap();
    assert_eq!(waiting.phase, TransferPhase::TargetPrepared);

    let instance = InstanceValue {
        instance_id: 2,
        rpc_endpoint: target.rpc_endpoint,
        last_heartbeat_ms: u64::MAX,
        extra: Some(ServiceExtra {
            chunk_kv: Some(ChunkKvExtra {
                hosted: vec![HostedPartition {
                    partition_id,
                    owner_epoch: 2,
                    recovering: false,
                }],
                ..ChunkKvExtra::default()
            }),
            ..ServiceExtra::default()
        }),
    };
    put_json(
        &control,
        InstanceKey {
            service: "chunk-kv".into(),
            instance_id: 2,
        }
        .to_path(),
        &instance,
    )
    .await;

    ChunkKvRangeMonitorDriver::new()
        .tick(&control, &policy)
        .await
        .unwrap();

    let persisted = control.get(transition_path.as_bytes()).await.unwrap();
    let mut ready: TransferTransition = serde_json::from_slice(persisted.value.as_ref().unwrap()).unwrap();
    assert_eq!(ready.phase, TransferPhase::AwaitingFence);
    ready.phase = TransferPhase::TargetCatchingUp;
    ready.release_proof = Some(AuthorityReleaseProof::ExplicitFence {
        source_instance_id: 1,
        source_epoch: 1,
        durable_tail: 7,
        durable_tail_offset: 7,
    });
    control
        .compare_and_put(
            Bytes::from(transition_path.clone()),
            Bytes::from(serde_json::to_vec(&ready).unwrap()),
            persisted.revision,
        )
        .await
        .unwrap();
    ChunkKvRangeMonitorDriver::new()
        .tick(&control, &policy)
        .await
        .unwrap();
    let persisted = control.get(transition_path.as_bytes()).await.unwrap();
    let mut ready: TransferTransition = serde_json::from_slice(persisted.value.as_ref().unwrap()).unwrap();
    assert_eq!(ready.phase, TransferPhase::CatchupPublished);
    ready.phase = TransferPhase::TargetReady;
    ready.catchup_proof = Some(TargetReadinessProof {
        target_instance_id: 2,
        target_epoch: 2,
        artifact: target_artifact,
        durable_tail: 7,
    });
    control
        .compare_and_put(
            Bytes::from(transition_path),
            Bytes::from(serde_json::to_vec(&ready).unwrap()),
            persisted.revision,
        )
        .await
        .unwrap();
    ChunkKvRangeMonitorDriver::new()
        .tick(&control, &policy)
        .await
        .unwrap();

    let published: ChunkKvRangeCatalogHead = get_json(&control, &ChunkKvRangeCatalogHeadKey.to_path()).await;
    assert_eq!(published.generation, 3);
    let committed: TransferTransition =
        get_json(&control, &ChunkKvTransferKey { transition_id }.to_path()).await;
    assert_eq!(committed.phase, TransferPhase::CatalogCommitted);
    let grant: ServingGrant = get_json(&control, &ServingGrantKey { instance_id: 2 }.to_path()).await;
    grant.validate().unwrap();
    assert_eq!(grant.catalog_generation, 3);
    assert_eq!(grant.assignments[0].owner_epoch, 2);
}

#[tokio::test]
async fn chunk_kv_driver_plans_dead_owner_recovery_once_and_waits_for_target() {
    use crowdb_kv_server::background::domain_monitor::ChunkKvRangeMonitorDriver;

    let (_, store) = registry_with_group_zero(PxLocalReplicaRole::Leader);
    let control = Group0ControlPlane::acquire(&store).await.unwrap();
    let partition_id = Id128 { high: 3, low: 4 };
    let artifact = PartitionArtifact {
        tree_id: 21,
        stream_name: StreamName { high: 22, low: 23 },
        tail_overlay: None,
    };
    let mut page = ChunkKvRangeCatalogPage {
        generation: 1,
        page_index: 0,
        entries: vec![ChunkKvRangeCatalogEntry {
            partition_id,
            range: KeyRange {
                start: Vec::new(),
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: 1,
                rpc_endpoint: "127.0.0.1:17001".into(),
            },
            owner_epoch: 5,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact,
            transition_id: None,
        }],
        checksum: [0; 32],
    };
    page.seal().unwrap();
    let mut head = ChunkKvRangeCatalogHead {
        generation: 1,
        previous_generation: None,
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: 1,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    put_json(
        &control,
        ChunkKvRangeCatalogPageKey {
            generation: 1,
            page_index: 0,
        }
        .to_path(),
        &page,
    )
    .await;
    put_json(&control, ChunkKvRangeCatalogHeadKey.to_path(), &head).await;
    put_json(
        &control,
        InstanceKey {
            service: "chunk-kv".into(),
            instance_id: 2,
        }
        .to_path(),
        &InstanceValue {
            instance_id: 2,
            rpc_endpoint: "127.0.0.1:17002".into(),
            last_heartbeat_ms: u64::MAX,
            extra: Some(ServiceExtra {
                chunk_kv: Some(ChunkKvExtra::default()),
                ..ServiceExtra::default()
            }),
        },
    )
    .await;
    let mut policy = descriptor();
    policy.domain = "chunk-kv".into();
    policy.service_registry_name = "chunk-kv".into();

    let driver = ChunkKvRangeMonitorDriver::new();
    driver.tick(&control, &policy).await.unwrap();
    driver.tick(&control, &policy).await.unwrap();

    let transitions = control
        .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 16)
        .await
        .unwrap();
    assert_eq!(transitions.len(), 1);
    let transition: TransferTransition = serde_json::from_slice(&transitions[0].value).unwrap();
    transition.validate().unwrap();
    assert_eq!(transition.partition_id, partition_id);
    assert_eq!(transition.target.instance_id, 2);
    assert_eq!(transition.target_epoch, 6);
    assert_eq!(transition.phase, TransferPhase::TargetPreparing);
    assert!(matches!(
        transition.release_proof,
        Some(AuthorityReleaseProof::LeaseExpired { .. })
    ));
    assert_eq!(
        get_json::<ChunkKvRangeCatalogHead>(&control, &ChunkKvRangeCatalogHeadKey.to_path())
            .await
            .generation,
        1
    );
}

#[allow(clippy::too_many_lines)]
async fn assert_chunk_kv_split_plan(target_partitions_per_owner: u32, target_partition_bytes: u64) {
    use crowdb_kv_server::background::domain_monitor::ChunkKvRangeMonitorDriver;

    let (_, store) = registry_with_group_zero(PxLocalReplicaRole::Leader);
    let control = Group0ControlPlane::acquire(&store).await.unwrap();
    let partition_id = Id128 { high: 31, low: 32 };
    let mut page = ChunkKvRangeCatalogPage {
        generation: 1,
        page_index: 0,
        entries: vec![ChunkKvRangeCatalogEntry {
            partition_id,
            range: KeyRange {
                start: Vec::new(),
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: 1,
                rpc_endpoint: "127.0.0.1:17001".into(),
            },
            owner_epoch: 1,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: 33,
                stream_name: StreamName { high: 34, low: 35 },
                tail_overlay: None,
            },
            transition_id: None,
        }],
        checksum: [0; 32],
    };
    page.seal().unwrap();
    let mut head = ChunkKvRangeCatalogHead {
        generation: 1,
        previous_generation: None,
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: 1,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    put_json(
        &control,
        ChunkKvRangeCatalogPageKey {
            generation: 1,
            page_index: 0,
        }
        .to_path(),
        &page,
    )
    .await;
    put_json(&control, ChunkKvRangeCatalogHeadKey.to_path(), &head).await;
    put_json(
        &control,
        InstanceKey {
            service: "chunk-kv".into(),
            instance_id: 1,
        }
        .to_path(),
        &InstanceValue {
            instance_id: 1,
            rpc_endpoint: "127.0.0.1:17001".into(),
            last_heartbeat_ms: u64::MAX,
            extra: Some(ServiceExtra {
                chunk_kv: Some(ChunkKvExtra {
                    capacity_bytes: 1_000,
                    durable_bytes: 600,
                    request_rate: 0,
                    hosted: vec![HostedPartition {
                        partition_id,
                        owner_epoch: 1,
                        recovering: false,
                    }],
                    partition_loads: vec![ChunkKvPartitionLoad {
                        partition_id,
                        durable_bytes: 600,
                        live_byte_samples: vec![
                            (b"a".to_vec(), 100),
                            (b"m".to_vec(), 400),
                            (b"z".to_vec(), 100),
                        ],
                        independently_recoverable: true,
                    }],
                }),
                ..ServiceExtra::default()
            }),
        },
    )
    .await;
    let mut policy = descriptor();
    policy.domain = "chunk-kv".into();
    policy.service_registry_name = "chunk-kv".into();
    policy.chunk_kv_range_balance = Some(ChunkKvRangeBalancePolicy {
        target_partitions_per_owner,
        target_partition_bytes,
        cooldown_ms: 1,
        ..ChunkKvRangeBalancePolicy::default()
    });

    let driver = ChunkKvRangeMonitorDriver::new();
    driver.tick(&control, &policy).await.unwrap();
    driver.tick(&control, &policy).await.unwrap();

    let transitions = control
        .scan_all_prefix(Bytes::from(ChunkKvSplitKey::text_prefix_all()), 16)
        .await
        .unwrap();
    assert_eq!(transitions.len(), 1);
    let transition: SplitTransition = serde_json::from_slice(&transitions[0].value).unwrap();
    transition.validate().unwrap();
    assert_eq!(transition.parent_id, partition_id);
    assert_eq!(transition.split_key, b"m");
    assert_eq!(transition.phase, SplitPhase::Planned);

    let mut prepared = transition;
    let overlay = TailOverlayArtifact {
        source_partition_id: prepared.parent_id,
        source_epoch: prepared.parent_epoch,
        source_stream_name: prepared.parent_artifact.stream_name,
        source_stream_manifest_generation: 1,
        replay_offset: 0,
        cutover_offset: 10,
        base_root_manifest_generation: 1,
        base_tree_manifest: 1,
        base_applied_seq: 10,
        cutover_seq: 11,
        target_stream_start_seq: 12,
    };
    prepared.retained_parent_artifact.tail_overlay = Some(overlay.clone());
    prepared.child.artifact.tail_overlay = Some(overlay.clone());
    prepared.phase = SplitPhase::ChildPrepared;
    prepared.readiness_proof = Some(crowdb_protocol::chunk_kv::SplitReadinessProof {
        cutover_seq: 11,
        parent_next_epoch: prepared.parent_next_epoch,
        retained_parent_artifact: prepared.retained_parent_artifact.clone(),
        retained_parent_tree_manifest: 1,
        retained_parent_root_manifest_generation: 1,
        retained_parent_applied_seq: 11,
        child_applied_seq: 11,
        child_tree_manifest: 1,
        child_root_manifest_generation: 1,
        retained_parent_tail_overlay: overlay.clone(),
        child_tail_overlay: overlay,
    });
    prepared.validate().unwrap();
    control
        .compare_and_put(
            transitions[0].key.clone(),
            Bytes::from(serde_json::to_vec(&prepared).unwrap()),
            transitions[0].revision,
        )
        .await
        .unwrap();
    driver.tick(&control, &policy).await.unwrap();

    let published_head: ChunkKvRangeCatalogHead =
        get_json(&control, &ChunkKvRangeCatalogHeadKey.to_path()).await;
    let published_page: ChunkKvRangeCatalogPage = get_json(
        &control,
        &ChunkKvRangeCatalogPageKey {
            generation: published_head.generation,
            page_index: 0,
        }
        .to_path(),
    )
    .await;
    let retained = published_page
        .entries
        .iter()
        .find(|entry| entry.partition_id == prepared.parent_id)
        .unwrap();
    assert_eq!(retained.artifact, prepared.retained_parent_artifact);
}

#[tokio::test]
async fn chunk_kv_driver_plans_count_driven_split() {
    assert_chunk_kv_split_plan(2, u64::MAX).await;
}

#[tokio::test]
async fn chunk_kv_driver_plans_size_driven_split() {
    assert_chunk_kv_split_plan(1, 100).await;
}

#[tokio::test]
async fn chunk_kv_driver_enables_child_owner_balance_from_policy() {
    use crowdb_kv_server::background::domain_monitor::ChunkKvRangeMonitorDriver;

    let (_, store) = registry_with_group_zero(PxLocalReplicaRole::Leader);
    let control = Group0ControlPlane::acquire(&store).await.unwrap();
    put_balance_fixture(&control).await;

    let driver = ChunkKvRangeMonitorDriver::new();
    let mut policy = descriptor();
    policy.domain = "chunk-kv".into();
    policy.service_registry_name = "chunk-kv".into();
    driver.tick(&control, &policy).await.unwrap();
    assert!(control
        .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 16)
        .await
        .unwrap()
        .is_empty());

    policy.chunk_kv_range_balance = Some(ChunkKvRangeBalancePolicy {
        target_partitions_per_owner: 1,
        target_partition_bytes: u64::MAX,
        cooldown_ms: 1,
        ..ChunkKvRangeBalancePolicy::default()
    });
    driver.tick(&control, &policy).await.unwrap();
    let transfers = control
        .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 16)
        .await
        .unwrap();
    assert_eq!(transfers.len(), 1);
    let transfer: TransferTransition = serde_json::from_slice(&transfers[0].value).unwrap();
    assert_eq!(transfer.source.instance_id, 1);
    assert_eq!(transfer.target.instance_id, 2);
}

async fn put_balance_fixture(control: &Group0ControlPlane) {
    let partition_ids = [Id128 { high: 41, low: 1 }, Id128 { high: 41, low: 2 }];
    let mut page = ChunkKvRangeCatalogPage {
        generation: 1,
        page_index: 0,
        entries: partition_ids
            .iter()
            .enumerate()
            .map(|(index, partition_id)| ChunkKvRangeCatalogEntry {
                partition_id: *partition_id,
                range: KeyRange {
                    start: if index == 0 { Vec::new() } else { b"m".to_vec() },
                    end: (index == 0).then(|| b"m".to_vec()),
                },
                owner: OwnerDescriptor {
                    instance_id: 1,
                    rpc_endpoint: "127.0.0.1:17001".into(),
                },
                owner_epoch: 1,
                state: ChunkKvRangeCatalogPartitionState::Serving,
                artifact: PartitionArtifact {
                    tree_id: 50 + index as u64,
                    stream_name: StreamName {
                        high: 51,
                        low: index as u64 + 1,
                    },
                    tail_overlay: None,
                },
                transition_id: None,
            })
            .collect(),
        checksum: [0; 32],
    };
    page.seal().unwrap();
    let mut head = ChunkKvRangeCatalogHead {
        generation: 1,
        previous_generation: None,
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: 1,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    put_json(
        control,
        ChunkKvRangeCatalogPageKey {
            generation: 1,
            page_index: 0,
        }
        .to_path(),
        &page,
    )
    .await;
    put_json(control, ChunkKvRangeCatalogHeadKey.to_path(), &head).await;

    let source_loads = partition_ids
        .iter()
        .map(|partition_id| ChunkKvPartitionLoad {
            partition_id: *partition_id,
            durable_bytes: 100,
            live_byte_samples: vec![(b"a".to_vec(), 100)],
            independently_recoverable: true,
        })
        .collect();
    for (instance_id, chunk_kv) in [
        (
            1,
            ChunkKvExtra {
                capacity_bytes: 1_000,
                durable_bytes: 200,
                request_rate: 0,
                hosted: partition_ids
                    .iter()
                    .map(|partition_id| HostedPartition {
                        partition_id: *partition_id,
                        owner_epoch: 1,
                        recovering: false,
                    })
                    .collect(),
                partition_loads: source_loads,
            },
        ),
        (
            2,
            ChunkKvExtra {
                capacity_bytes: 1_000,
                ..ChunkKvExtra::default()
            },
        ),
    ] {
        put_chunk_kv_instance(control, instance_id, chunk_kv).await;
    }
}

async fn put_chunk_kv_instance(control: &Group0ControlPlane, instance_id: u64, chunk_kv: ChunkKvExtra) {
    put_json(
        control,
        InstanceKey {
            service: "chunk-kv".into(),
            instance_id,
        }
        .to_path(),
        &InstanceValue {
            instance_id,
            rpc_endpoint: format!("127.0.0.1:{}", 17_000 + instance_id),
            last_heartbeat_ms: u64::MAX,
            extra: Some(ServiceExtra {
                chunk_kv: Some(chunk_kv),
                ..ServiceExtra::default()
            }),
        },
    )
    .await;
}

async fn put_json<T: serde::Serialize>(control: &Group0ControlPlane, path: String, value: &T) {
    control
        .compare_and_put(
            Bytes::from(path),
            Bytes::from(serde_json::to_vec(value).unwrap()),
            0,
        )
        .await
        .unwrap();
}

async fn get_json<T: serde::de::DeserializeOwned>(control: &Group0ControlPlane, path: &str) -> T {
    serde_json::from_slice(&control.get(path.as_bytes()).await.unwrap().value.unwrap()).unwrap()
}
