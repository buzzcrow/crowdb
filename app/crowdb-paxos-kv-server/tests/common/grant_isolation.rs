// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_paxos_kv_server::background::domain_monitor::ChunkKvRangeMonitorDriver;
use crowdb_protocol::{
    chunk_stream::{StreamProcessLease, StreamPurpose},
    key::StreamProcessLeaseKey,
};

struct TestGrantInstances {
    store: Arc<PxKvStore>,
    control: Group0ControlPlane,
    policy: DomainMonitorDescriptor,
}

fn lease_path(instance_id: u64) -> String {
    StreamProcessLeaseKey {
        purpose: StreamPurpose::Wal,
        instance_id,
    }
    .to_path()
}

impl TestGrantInstances {
    async fn start() -> Self {
        let (_, store) = registry_with_group_zero(PxLocalReplicaRole::Leader);
        let control = Group0ControlPlane::acquire(&store).await.unwrap();
        let mut entries = Vec::new();
        for instance_id in 1..=2 {
            let partition_id = Id128 {
                high: 91,
                low: instance_id,
            };
            let endpoint = format!("127.0.0.1:{}", 17000 + instance_id);
            entries.push(ChunkKvRangeCatalogEntry {
                partition_id,
                range: KeyRange {
                    start: if instance_id == 1 { vec![] } else { b"m".to_vec() },
                    end: (instance_id == 1).then(|| b"m".to_vec()),
                },
                owner: OwnerDescriptor {
                    instance_id,
                    rpc_endpoint: endpoint.clone(),
                },
                owner_epoch: 1,
                state: ChunkKvRangeCatalogPartitionState::Serving,
                artifact: PartitionArtifact {
                    tree_id: 910 + instance_id,
                    stream_name: StreamName {
                        high: 92,
                        low: instance_id,
                    },
                    tail_overlay: None,
                },
                transition_id: None,
            });
            test_process::TestProcessLease::seed(&control, instance_id).await;
            let mut instance = test_process::TestProcessLease::idle_instance(instance_id, &endpoint);
            instance.extra.as_mut().unwrap().chunk_kv.as_mut().unwrap().hosted = vec![HostedPartition {
                partition_id,
                owner_epoch: 1,
                recovering: false,
            }];
            put_json(
                &control,
                InstanceKey {
                    service: "chunk-kv".into(),
                    instance_id,
                }
                .to_path(),
                &instance,
            )
            .await;
        }
        Self::seed_catalog(&control, entries).await;
        let mut policy = descriptor();
        policy.domain = "chunk-kv".into();
        policy.service_registry_name = "chunk-kv".into();
        policy.failure_policy = DomainFailurePolicy::OperatorOnly;
        Self {
            store,
            control,
            policy,
        }
    }

    async fn seed_catalog(control: &Group0ControlPlane, entries: Vec<ChunkKvRangeCatalogEntry>) {
        let mut page = ChunkKvRangeCatalogPage {
            generation: 1,
            page_index: 0,
            entries,
            checksum: [0; 32],
        };
        page.seal().unwrap();
        let mut head = ChunkKvRangeCatalogHead {
            generation: 1,
            previous_generation: None,
            pages: vec![ChunkKvRangeCatalogPageRef {
                page_generation: 1,
                page_index: 0,
                first_key: vec![],
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
    }

    async fn assert_second_grant(&self) {
        let path = ServingGrantKey { instance_id: 2 }.to_path();
        let observed = self.control.get(path.as_bytes()).await.unwrap();
        assert!(
            observed.value.is_some(),
            "one instance's failed grant publication must not skip an independent ready instance"
        );
        let grant: ServingGrant = serde_json::from_slice(&observed.value.unwrap()).unwrap();
        let lease_record = self.control.get(lease_path(2).as_bytes()).await.unwrap();
        let lease: StreamProcessLease = serde_json::from_slice(&lease_record.value.unwrap()).unwrap();
        assert_eq!(grant.instance_id, 2);
        assert_eq!(grant.process_epoch, 1);
        assert_eq!(grant.catalog_generation, 1);
        assert_eq!(grant.assignments.len(), 1);
        assert_eq!(grant.assignments[0].partition_id, Id128 { high: 91, low: 2 });
        assert_eq!(grant.assignments[0].owner_epoch, 1);
        assert_eq!(grant.lease_sequence, lease.grant_sequence);
        assert!(lease.grant_expires_at_ms >= grant.expires_at_ms);
        assert_eq!(observed.revision, lease_record.revision);
        assert!(self
            .control
            .get(ServingGrantKey { instance_id: 1 }.to_path().as_bytes())
            .await
            .unwrap()
            .value
            .is_none());
    }
}

async fn until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn busy_instance_cas_does_not_skip_another_instances_grant() {
    let test = TestGrantInstances::start().await;
    let group = test.store.get_group(0).unwrap();
    let first_path = lease_path(1);
    let current = test.control.get(first_path.as_bytes()).await.unwrap();
    let original_lease = current.value.clone();
    let permits = group.try_acquire_all_inflight_permits();
    assert_eq!(permits.len(), group.inflight_window_size());
    let contender = tokio::spawn({
        let control = test.control.clone();
        let path = first_path.clone();
        async move {
            control
                .compare_and_put(Bytes::from(path), current.value.unwrap(), current.revision)
                .await
        }
    });
    until(|| group.conditional_write_pending_for_tests(first_path.as_bytes())).await;
    let renewal = tokio::spawn({
        let control = test.control.clone();
        let policy = test.policy.clone();
        async move { ChunkKvRangeMonitorDriver::new().tick(&control, &policy).await }
    });
    until(|| renewal.is_finished() || group.conditional_write_pending_for_tests(lease_path(2).as_bytes()))
        .await;
    drop(permits);
    let result = tokio::time::timeout(Duration::from_secs(3), renewal)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), contender)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(result.unwrap_err().contains("admission is busy"));
    assert_eq!(
        test.control.get(first_path.as_bytes()).await.unwrap().value,
        original_lease
    );
    test.assert_second_grant().await;
}

#[tokio::test]
async fn invalid_instance_grant_does_not_skip_another_instances_grant() {
    let test = TestGrantInstances::start().await;
    let path = lease_path(1);
    let current = test.control.get(path.as_bytes()).await.unwrap();
    let mut lease: StreamProcessLease = serde_json::from_slice(&current.value.unwrap()).unwrap();
    lease.grant_sequence = u64::MAX;
    lease.grant_expires_at_ms = 1;
    lease.validate().unwrap();
    test.control
        .compare_and_put(
            Bytes::from(path),
            Bytes::from(serde_json::to_vec(&lease).unwrap()),
            current.revision,
        )
        .await
        .unwrap();
    let result = ChunkKvRangeMonitorDriver::new()
        .tick(&test.control, &test.policy)
        .await;
    assert!(result.unwrap_err().contains("grant sequence overflowed"));
    let retained: StreamProcessLease = get_json(&test.control, &lease_path(1)).await;
    assert_eq!(retained, lease);
    test.assert_second_grant().await;
}
