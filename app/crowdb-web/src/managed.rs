use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use crowdb_kv_client::CrowdbSysmdClient;
use crowdb_monitor::{MonitorStatus, ServiceStatus, StatusStore};
use crowdb_protocol::common::StoreValue;
use serde::Serialize;
use serde_json::{json, Value};

use crate::state::AppState;

const SERVICE_TYPES: [(&str, &str); 5] = [
    ("kv-server", "kv"),
    ("diskdb", "diskdb"),
    ("diskio", "diskio"),
    ("chunkdb", "chunkdb"),
    ("chunk-kv", "chunk-kv"),
];

#[derive(Serialize)]
pub struct ManagedSnapshot {
    source: &'static str,
    racks: Vec<Value>,
    nodes: Vec<Value>,
    disk_groups: Vec<Value>,
    disks: Vec<Value>,
    stores: Vec<Value>,
    groups: Vec<Value>,
    replicas: Vec<Value>,
    services: Vec<ServiceView>,
    monitor: MonitorStatus,
}

#[derive(Serialize)]
struct ServiceView {
    kind: &'static str,
    instance_id: String,
    endpoint: String,
    last_heartbeat_ms: u64,
    monitor: Option<ServiceStatus>,
}

#[derive(Clone, Copy)]
enum SnapshotFailure {
    Group0,
    Monitor,
}

impl SnapshotFailure {
    fn reason(self) -> &'static str {
        match self {
            Self::Group0 => "group0_unavailable",
            Self::Monitor => "monitor_unavailable",
        }
    }
}

async fn monitor_status(path: PathBuf) -> Result<MonitorStatus, SnapshotFailure> {
    tokio::task::spawn_blocking(move || {
        StatusStore::open_file(&path)
            .and_then(|store| store.read(Duration::from_secs(15)))
            .map_err(|error| {
                tracing::debug!(%error, "managed monitor status unavailable");
                SnapshotFailure::Monitor
            })
    })
    .await
    .map_err(|error| {
        tracing::debug!(%error, "managed monitor status task failed");
        SnapshotFailure::Monitor
    })?
}

async fn validate_live_store_nodes(
    sysmd: &CrowdbSysmdClient,
    stores: &[StoreValue],
) -> Result<(), crowdb_kv_client::Error> {
    let mut live_nodes = BTreeSet::new();
    for (_, instance) in sysmd.read_all_kv_server_instances().await? {
        let Some(node_id) = instance
            .extra
            .as_ref()
            .and_then(|extra| extra.kv_server.as_ref())
            .and_then(|extra| extra.node_id)
        else {
            continue;
        };
        if instance.rpc_endpoint.is_empty() || !live_nodes.insert(node_id) {
            return Err(crowdb_kv_client::Error::Topology(
                "live KV management registration is ambiguous".into(),
            ));
        }
    }
    if stores
        .iter()
        .flat_map(|store| &store.node_ids)
        .any(|node_id| !live_nodes.contains(node_id))
    {
        return Err(crowdb_kv_client::Error::Topology(
            "store node has no live KV management registration".into(),
        ));
    }
    Ok(())
}

async fn load_snapshot(state: &AppState) -> Result<ManagedSnapshot, SnapshotFailure> {
    let Some(path) = state.monitor_status_path.as_ref() else {
        return Err(SnapshotFailure::Monitor);
    };
    if state.authority_seeds.is_empty() {
        return Err(SnapshotFailure::Group0);
    }
    let monitor = monitor_status(path.as_ref().clone()).await?;
    let client = state.kv_client().await;
    let timeout = Duration::from_millis(state.authority_timeout_ms);
    tokio::time::timeout(timeout, async {
        client.refresh_topology().await?;
        let sysmd = CrowdbSysmdClient::from_shared(client);
        let racks = sysmd.list_racks().await?;
        let nodes = sysmd.list_nodes().await?;
        let disk_groups = sysmd.list_disk_groups().await?;
        let disks = sysmd.list_all_disks().await?;
        let stores = sysmd.list_stores().await?;
        if racks.is_empty() || nodes.is_empty() || !stores.iter().any(|store| store.store_id == 0) {
            return Err(crowdb_kv_client::Error::Topology("managed topology is incomplete".into()));
        }
        validate_live_store_nodes(&sysmd, &stores).await?;

        let mut groups = Vec::new();
        let mut replicas = Vec::new();
        for store in &stores {
            for group in sysmd.list_groups_in_store(store.store_id).await? {
                replicas.extend(sysmd.list_replicas_in_group(store.store_id, group.group_id).await?);
                groups.push(group);
            }
        }
        let mut services = Vec::new();
        for (kind, monitor_id) in SERVICE_TYPES {
            let instances = sysmd.read_service_instances(kind).await?;
            let overlay = if instances.len() == 1 {
                monitor.services.get(monitor_id).cloned()
            } else {
                None
            };
            services.extend(instances.into_iter().map(|(instance_id, record)| ServiceView {
                kind,
                instance_id: instance_id.to_string(),
                endpoint: record.rpc_endpoint,
                last_heartbeat_ms: record.last_heartbeat_ms,
                monitor: overlay.clone(),
            }));
        }
        Ok(ManagedSnapshot {
            source: "group0",
            racks: racks.into_iter().map(|(id, value)| json!({"id": id, "status": value.status, "node_ids": value.node_ids})).collect(),
            nodes: nodes.into_iter().map(|(rack_id, id, value)| json!({"rack_id": rack_id, "id": id, "status": value.status, "disk_group_ids": value.disk_group_ids})).collect(),
            disk_groups: disk_groups.into_iter().map(|group| json!(group)).collect(),
            disks: disks.into_iter().map(|disk| json!({"rack_id": disk.rack_id, "node_id": disk.node_id, "disk_group_id": disk.disk_group_id, "disk_id": disk.disk_id, "value": disk.value})).collect(),
            stores: stores.into_iter().map(|store| json!(store)).collect(),
            groups: groups.into_iter().map(|group| json!(group)).collect(),
            replicas: replicas.into_iter().map(|replica| json!(replica)).collect(),
            services,
            monitor,
        })
    })
    .await
    .map_err(|error| {
        tracing::debug!(%error, "managed Group 0 snapshot timed out");
        SnapshotFailure::Group0
    })?
    .map_err(|error| {
        tracing::debug!(%error, "managed Group 0 snapshot failed");
        SnapshotFailure::Group0
    })
}

pub async fn authority(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    match load_snapshot(&state).await {
        Ok(snapshot) => (
            StatusCode::OK,
            Json(
                json!({"source": "group0", "available": true, "monitor_revision": snapshot.monitor.revision}),
            ),
        ),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"source": "group0", "available": false, "reason": error.reason()})),
        ),
    }
}

pub async fn snapshot(
    State(state): State<AppState>,
) -> Result<Json<ManagedSnapshot>, (StatusCode, Json<Value>)> {
    load_snapshot(&state).await.map(Json).map_err(|error| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"source": "group0", "available": false, "reason": error.reason()})),
        )
    })
}
