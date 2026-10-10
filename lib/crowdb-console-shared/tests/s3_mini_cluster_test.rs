// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_console_shared::ops::s3;
use crowdb_console_shared::{lifecycle, ops::OpContext};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, RangeBindingClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{ChunkType, ListChunksRequest, QueryChunkRequest, Strip, StripType};
use crowdb_protocol::common::HwStatus;
use crowdb_test_harness::test_dirs::TestDir;
use reqwest::Method;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

struct StopClusterOnDrop<'a>(&'a Path);

impl Drop for StopClusterOnDrop<'_> {
    fn drop(&mut self) {
        let _ = s3::stop(self.0);
    }
}

#[test]
fn foreign_nonempty_directory_is_not_a_cluster() {
    let dir = TestDir::new("s3-mini-foreign").expect("create test directory");
    std::fs::write(dir.path().join("owned-by-user"), b"keep").expect("write sentinel");

    let error = s3::status(dir.path()).expect_err("foreign directory must be rejected");

    assert!(error.to_string().contains("is not a CROWDB S3 mini-cluster"));
    assert_eq!(std::fs::read(dir.path().join("owned-by-user")).unwrap(), b"keep");
}

#[test]
fn incomplete_marker_fails_closed() {
    let dir = TestDir::new("s3-mini-incomplete").expect("create test directory");
    std::fs::write(dir.path().join("s3-mini-cluster.json"), b"{}").expect("write marker");

    let error = s3::status(dir.path()).expect_err("incomplete marker must be rejected");

    assert!(error.to_string().contains("config error"));
}

#[test]
fn local_launch_state_rejects_topology_and_legacy_console_file() {
    let dir = TestDir::new("s3-mini-local-only").expect("create test directory");
    std::fs::write(
        dir.path().join("s3-mini-cluster.json"),
        r#"{"version":1,"endpoint":"http://127.0.0.1:16000","tenant":"local"}"#,
    )
    .unwrap();
    std::fs::write(dir.path().join("console.toml"), "[[rack]]\nid = 1\n").unwrap();
    assert!(
        s3::status(dir.path()).is_err(),
        "legacy topology must not be loaded"
    );
    std::fs::write(
        dir.path().join("s3-local-state.toml"),
        "version = 1\ngroup0_seeds = ['http://127.0.0.1:10000']\n[[rack]]\nid = 1\n",
    )
    .unwrap();
    let error = s3::status(dir.path()).expect_err("local state cannot contain topology");
    assert!(error.to_string().contains("unknown field"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    target_os = "macos",
    ignore = "starts the complete local storage and S3 process stack"
)]
async fn persistent_cluster_survives_stop_restart_and_range_read() {
    let dir = TestDir::new("s3-mini-persistent-e2e").expect("create test directory");
    let started = s3::start(dir.path()).await.expect("start persistent cluster");
    assert!(started.web_endpoint.starts_with("http://127.0.0.1:"));
    let health = reqwest::get(format!("{}/healthz", started.web_endpoint))
        .await
        .expect("web health request");
    assert!(health.status().is_success());
    let preview = reqwest::get(format!("{}/api/preview", started.web_endpoint))
        .await
        .expect("web preview request")
        .text()
        .await
        .expect("web preview body");
    assert!(preview.contains("group0"));
    let client = s3::S3HttpClient::from_data_dir(dir.path()).expect("S3 client");
    client
        .request(Method::PUT, Some("durable-bucket"), None, &[], None, None)
        .await
        .expect("create bucket");
    client
        .request(
            Method::PUT,
            Some("durable-bucket"),
            Some("nested/object"),
            &[],
            Some(b"durable-object-bytes".to_vec()),
            None,
        )
        .await
        .expect("put object");
    let marker = std::fs::read_to_string(dir.path().join("s3-mini-cluster.json")).expect("marker");
    let config = std::fs::read_to_string(dir.path().join("s3-local-state.toml")).expect("local state");
    assert!(!marker.contains("1111111111111111"));
    assert!(!config.contains("1111111111111111"));
    assert!(!config.contains("[[rack]]"));
    assert!(!config.contains("[[node]]"));
    assert!(!config.contains("[[store]]"));
    assert!(!dir.path().join("console.toml").exists());

    let stopped = s3::stop(dir.path()).expect("stop cluster");
    assert_eq!(stopped.running_services, 0);
    let restarted = s3::start(dir.path()).await.expect("restart cluster");
    assert_eq!(restarted.running_services, restarted.total_services);
    let client = s3::S3HttpClient::from_data_dir(dir.path()).expect("restarted S3 client");
    let (_, body) = client
        .request(
            Method::GET,
            Some("durable-bucket"),
            Some("nested/object"),
            &[],
            None,
            None,
        )
        .await
        .expect("read after restart");
    assert_eq!(body, b"durable-object-bytes");
    let (_, range) = client
        .request(
            Method::GET,
            Some("durable-bucket"),
            Some("nested/object"),
            &[],
            None,
            Some((8, 13)),
        )
        .await
        .expect("range read");
    assert_eq!(range, b"object");
    s3::delete(dir.path()).expect("delete cluster");
    assert!(!dir.path().exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    target_os = "macos",
    ignore = "starts a complete simulated three-rack process stack"
)]
async fn protected_cluster_starts_and_reads_after_restart() {
    let dir = TestDir::new("s3-mini-protected-e2e").expect("create test directory");
    let started = s3::start_protected_test_cluster(dir.path())
        .await
        .expect("start protected cluster");
    let (config, record) = s3::load(dir.path()).expect("load protected cluster");
    assert!(record.protected_test);
    for node_id in 1..=3 {
        assert!(dir.path().join(format!("rack{node_id}/node{node_id}")).is_dir());
        for kind in [
            crowdb_console_shared::config::ServiceType::Chunkdb,
            crowdb_console_shared::config::ServiceType::Diskdb,
            crowdb_console_shared::config::ServiceType::Diskio,
        ] {
            assert!(config
                .servers
                .iter()
                .any(|server| { server.node_id == Some(node_id) && server.service_type == kind }));
        }
    }
    let client = s3::S3HttpClient::from_data_dir(dir.path()).expect("S3 client");
    client
        .request(Method::PUT, Some("protected-bucket"), None, &[], None, None)
        .await
        .expect("create bucket");
    client
        .request(
            Method::PUT,
            Some("protected-bucket"),
            Some("protected-object"),
            &[],
            Some(b"protected-object-bytes".to_vec()),
            None,
        )
        .await
        .expect("put protected object");
    assert_eq!(
        s3::stop(dir.path())
            .expect("stop protected cluster")
            .running_services,
        0
    );
    let restarted = s3::start_protected_test_cluster(dir.path())
        .await
        .expect("restart protected cluster");
    assert_eq!(restarted.endpoint, started.endpoint);
    let client = s3::S3HttpClient::from_data_dir(dir.path()).expect("restarted S3 client");
    let (_, body) = client
        .request(
            Method::GET,
            Some("protected-bucket"),
            Some("protected-object"),
            &[],
            None,
            None,
        )
        .await
        .expect("read protected object");
    assert_eq!(body, b"protected-object-bytes");
    s3::delete(dir.path()).expect("delete protected cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    target_os = "macos",
    ignore = "stops one node in a complete simulated three-rack process stack"
)]
async fn protected_cluster_reads_and_writes_after_node_one_stops() {
    protected_cluster_reads_and_writes_after_node_stops(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    target_os = "macos",
    ignore = "stops one node in a complete simulated three-rack process stack"
)]
async fn protected_cluster_reads_and_writes_after_node_three_stops() {
    protected_cluster_reads_and_writes_after_node_stops(3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    target_os = "macos",
    ignore = "stops one node in a complete simulated three-rack process stack"
)]
async fn protected_cluster_reads_and_writes_after_node_two_stops() {
    protected_cluster_reads_and_writes_after_node_stops(2).await;
}

#[allow(clippy::too_many_lines)]
async fn protected_cluster_reads_and_writes_after_node_stops(failed_node: u64) {
    let dir =
        TestDir::new(&format!("s3-mini-protected-outage-{failed_node}")).expect("create test directory");
    s3::start_protected_test_cluster(dir.path())
        .await
        .expect("start protected cluster");
    let _cleanup = StopClusterOnDrop(dir.path());
    let client = s3::S3HttpClient::from_data_dir(dir.path()).expect("S3 client");
    client
        .request(Method::PUT, Some("outage-bucket"), None, &[], None, None)
        .await
        .expect("create bucket");
    client
        .request(
            Method::PUT,
            Some("outage-bucket"),
            Some("before-outage"),
            &[],
            Some(b"before-outage-bytes".to_vec()),
            None,
        )
        .await
        .expect("write before outage");

    let (config, _) = s3::load(dir.path()).expect("load process identities");
    for kind in [
        crowdb_console_shared::config::ServiceType::Chunkdb,
        crowdb_console_shared::config::ServiceType::Diskdb,
        crowdb_console_shared::config::ServiceType::Diskio,
        crowdb_console_shared::config::ServiceType::PaxosKv,
    ] {
        let server = config
            .servers
            .iter()
            .find(|server| server.node_id == Some(failed_node) && server.service_type == kind)
            .expect("failed-node process");
        lifecycle::stop_pid_with_timeout(server.pid.expect("process pid"), Duration::from_secs(5))
            .expect("stop failed-node process");
    }
    let seeds = config
        .servers
        .iter()
        .filter(|server| server.service_type == crowdb_console_shared::config::ServiceType::PaxosKv)
        .map(|server| server.url.clone())
        .collect::<Vec<_>>();
    let surviving_rpc = config
        .servers
        .iter()
        .find(|server| {
            server.service_type == crowdb_console_shared::config::ServiceType::PaxosKv
                && server.node_id != Some(failed_node)
        })
        .and_then(|server| server.rpc_url.as_deref())
        .expect("surviving KV RPC")
        .trim_start_matches("http://")
        .to_owned();
    let ctx = OpContext::new(surviving_rpc, seeds.clone(), config);
    ctx.sysmd()
        .set_node_status(failed_node, failed_node, HwStatus::Offline)
        .await
        .expect("mark unavailable node offline");
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(seeds)));
    let bindings = RangeBindingClient::from_shared(Arc::clone(&kv));
    let failed_instance = 20_000 + failed_node - 1;
    let registry = ServiceRegistryClient::from_shared(Arc::clone(&kv));
    let hardware = crowdb_kv_client::HardwareClient::from_shared(Arc::clone(&kv));
    let reassigned = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            if bindings.refresh().await.is_ok() {
                let snapshot = bindings.snapshot();
                if crowdb_protocol::chunk_slot::ChunkSlot::all().all(|slot| {
                    let owners = snapshot
                        .iter()
                        .filter(|binding| binding.slots.contains(slot))
                        .collect::<Vec<_>>();
                    owners.len() == 1 && owners[0].instance_id != failed_instance
                }) && diskdb_ownership_recovered(&registry, &hardware, failed_node).await
                {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    if reassigned.is_err() {
        let snapshot = bindings.snapshot();
        let stale = snapshot
            .iter()
            .filter(|binding| binding.instance_id == failed_instance && !binding.slots.is_empty())
            .count();
        let instances = ServiceRegistryClient::from_shared(kv)
            .read_all_instance_observations("chunkdb")
            .await;
        panic!(
            "ChunkDB ranges did not move: bindings={}, stale={stale}, instances={instances:?}",
            snapshot.len()
        );
    }

    let (_, old_body) = client
        .request(
            Method::GET,
            Some("outage-bucket"),
            Some("before-outage"),
            &[],
            None,
            None,
        )
        .await
        .expect("read existing object with one node stopped");
    assert_eq!(old_body, b"before-outage-bytes");
    let new_body = vec![0x5a; 2 * 1024 * 1024];
    client
        .request(
            Method::PUT,
            Some("outage-bucket"),
            Some("during-outage"),
            &[],
            Some(new_body.clone()),
            None,
        )
        .await
        .expect("write new object with one node stopped");
    let (_, read_back) = client
        .request(
            Method::GET,
            Some("outage-bucket"),
            Some("during-outage"),
            &[],
            None,
            None,
        )
        .await
        .expect("read new object with one node stopped");
    assert_eq!(read_back, new_body);
    if failed_node == 2 {
        let (outage_config, _) = s3::load(dir.path()).expect("load outage cluster");
        let surviving_chunkdbs = outage_config
            .servers
            .iter()
            .filter(|server| {
                server.service_type == crowdb_console_shared::config::ServiceType::Chunkdb
                    && server.node_id != Some(failed_node)
            })
            .map(|server| server.rpc_url.as_deref().expect("surviving ChunkDB RPC"));
        let chunk_transport = Arc::new(ChunkdbRpcTransport::new());
        let mut chunks = Vec::new();
        for endpoint in surviving_chunkdbs {
            let mut start_token = None;
            loop {
                let listed = chunk_transport
                    .send_list_chunks(
                        endpoint,
                        &ListChunksRequest {
                            start_token,
                            max_keys: 1_024,
                            ..ListChunksRequest::default()
                        },
                    )
                    .await
                    .expect("list chunks written during outage");
                let next = listed.next_token;
                chunks.extend(listed.chunks);
                if next.is_none() {
                    break;
                }
                assert_ne!(next, start_token, "chunk listing must advance");
                start_token = next;
            }
        }
        let degraded_chunks = chunks
            .iter()
            .filter(|chunk| chunk.chunk_type == ChunkType::S3 as i32)
            .filter(|chunk| {
                chunk.strips.iter().any(|strip| {
                    strip.strip_type == StripType::Ec as i32
                        && matches!(strip.strip.as_ref(), Some(Strip::EcStrip(_)))
                        && strip.placement_repair_required
                })
            })
            .filter_map(|chunk| chunk.id)
            .collect::<Vec<_>>();
        assert!(
            !degraded_chunks.is_empty(),
            "outage write did not persist degraded S3 EC placement"
        );
        ctx.sysmd()
            .set_node_status(failed_node, failed_node, HwStatus::Up)
            .await
            .expect("restore recovered node status");
        s3::stop(dir.path()).expect("stop protected cluster after outage");
        s3::start_protected_test_cluster(dir.path())
            .await
            .expect("restart protected cluster after outage");
        let restarted = s3::S3HttpClient::from_data_dir(dir.path()).expect("restarted S3 client");
        let (_, recovered) = restarted
            .request(
                Method::GET,
                Some("outage-bucket"),
                Some("during-outage"),
                &[],
                None,
                None,
            )
            .await
            .expect("read outage write after all processes restart");
        assert_eq!(recovered, new_body);
        let (restarted_config, _) = s3::load(dir.path()).expect("load restarted cluster");
        let seeds = restarted_config
            .servers
            .iter()
            .filter(|server| server.service_type == crowdb_console_shared::config::ServiceType::PaxosKv)
            .map(|server| server.url.clone())
            .collect();
        let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(seeds)));
        let chunkdb = ChunkdbClient::new(
            ServiceRegistryClient::from_shared(Arc::clone(&kv)),
            Arc::new(ChunkdbRpcTransport::new()),
        )
        .with_range_binding(RangeBindingClient::from_shared(kv));
        chunkdb
            .refresh_routes()
            .await
            .expect("refresh restarted ChunkDB routes");
        tokio::time::timeout(Duration::from_secs(25), async {
            loop {
                let mut repaired = true;
                for chunk_id in &degraded_chunks {
                    let chunk = chunkdb
                        .query_chunk(QueryChunkRequest {
                            chunk_id: Some(*chunk_id),
                        })
                        .await
                        .expect("query outage chunk after restart")
                        .chunk
                        .expect("outage chunk exists after restart");
                    repaired &= chunk
                        .strips
                        .iter()
                        .filter(|strip| strip.strip_type == StripType::Ec as i32)
                        .all(|strip| {
                            !strip.placement_repair_required
                                && strip.placement_assessment.as_ref().is_some_and(|assessment| {
                                    assessment.rack_protected
                                        && assessment.node_protected
                                        && assessment.disk_protected
                                })
                        });
                }
                if repaired {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("placement repair did not complete after ChunkDB restart");
    }
    s3::delete(dir.path()).expect("delete protected cluster");
}

async fn diskdb_ownership_recovered(
    registry: &ServiceRegistryClient,
    hardware: &crowdb_kv_client::HardwareClient,
    failed_node: u64,
) -> bool {
    let instances = registry.read_all_diskdb_instances().await.unwrap();
    let owners = hardware.list_owners().await.unwrap();
    let mut surviving_groups = hardware
        .list_disk_groups()
        .await
        .unwrap()
        .into_iter()
        .filter(|group| group.node_id != failed_node);
    !instances.iter().any(|(id, _)| *id == 10_000 + failed_node)
        && surviving_groups.all(|group| {
            owners.iter().any(|owner| {
                owner.dg_id == group.dg_id
                    && instances.iter().any(|(id, value)| {
                        *id == owner.instance_id
                            && value
                                .extra
                                .as_ref()
                                .and_then(|extra| extra.diskdb.as_ref())
                                .is_some_and(|diskdb| diskdb.owned_dg_ids.contains(&group.dg_id))
                    })
            })
        })
}
