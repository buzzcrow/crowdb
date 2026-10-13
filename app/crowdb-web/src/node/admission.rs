// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{extract::State, http::StatusCode, Json};
use crowdb_console_shared::config::{NodeEntry, ServerEntry};
use crowdb_console_shared::deployment::{
    admission,
    registry::{self, NodeRecord},
};
use crowdb_protocol::mgmt::node::{CandidateState, NodeBinding, NODE_PROTOCOL_VERSION};
use serde::Deserialize;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use crate::{
    error::{err_400, err_409, err_502, map_config_err, map_persist_err, ErrorBody},
    state::AppState,
};
type Failure = (StatusCode, Json<ErrorBody>);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AdmitRequest {
    pub(super) discovery_id: String,
    pub(super) rack_id: u64,
    pub(super) ssh_user: String,
    pub(super) ssh_port: u16,
    pub(super) ssh_password: Option<String>,
}

pub(crate) async fn admit(
    State(state): State<AppState>,
    Json(body): Json<AdmitRequest>,
) -> Result<Json<NodeRecord>, Failure> {
    let id = uuid::Uuid::parse_str(&body.discovery_id).map_err(|_| err_400("Invalid discovery UUID"))?;
    let _operation =
        crate::services::Operation::claim(&state, vec!["node/admission".into(), "cluster/init".into()])?;
    let (host, handshake) = observe(&state, &body).await?;
    admit_observed(&state, body, id, host, handshake).await.map(Json)
}

pub(super) async fn observe(
    state: &AppState,
    body: &AdmitRequest,
) -> Result<(String, crowdb_protocol::mgmt::node::NodeHandshake), Failure> {
    let observation = super::candidates(State(state.clone()))
        .await
        .map_err(|status| {
            (
                status,
                Json(ErrorBody {
                    error: "Discovery unavailable".into(),
                }),
            )
        })?
        .0;
    let candidate = observation
        .candidates
        .iter()
        .find(|candidate| candidate.advertisement.discovery_id == body.discovery_id)
        .ok_or_else(|| err_400("Candidate no longer discovered"))?;
    if candidate.advertisement.protocol_version != NODE_PROTOCOL_VERSION
        || matches!(
            candidate.state,
            CandidateState::ForeignCluster | CandidateState::Incompatible | CandidateState::IdentityConflict
        )
    {
        return Err(err_409(
            "Candidate identity, version or cluster binding prevents admission",
        ));
    }
    if body.ssh_user.is_empty() || body.ssh_port == 0 {
        return Err(err_400("SSH login and port are required"));
    }
    let endpoint = candidate
        .advertisement
        .monitor_endpoints
        .first()
        .ok_or_else(|| err_400("Candidate has no endpoint"))?;
    let url = reqwest::Url::parse(endpoint).map_err(|_| err_400("Invalid monitor endpoint"))?;
    let host = url
        .host_str()
        .ok_or_else(|| err_400("Missing candidate host"))?
        .trim_matches(['[', ']'])
        .to_owned();
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(3))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| err_502(error.to_string()))?;
    let handshake: crowdb_protocol::mgmt::node::NodeHandshake =
        super::read(&client, &format!("{}/node", endpoint.trim_end_matches('/')))
            .await
            .map_err(|_| err_502("Candidate handshake failed or exceeded its bound"))?;
    if handshake.advertisement != candidate.advertisement {
        return Err(err_409("Candidate changed during admission; refresh"));
    }
    Ok((host, handshake))
}

async fn admit_observed(
    state: &AppState,
    body: AdmitRequest,
    id: uuid::Uuid,
    host: String,
    handshake: crowdb_protocol::mgmt::node::NodeHandshake,
) -> Result<NodeRecord, Failure> {
    let path = state.runtime_root.join(format!("admission-{id}.json"));
    let config = state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .clone();
    if !config.racks.iter().any(|rack| rack.id == body.rack_id) {
        return Err(err_400("Select an existing rack"));
    }
    let mut record: NodeRecord = if path.exists() {
        serde_json::from_slice(&fs::read(&path).map_err(|error| err_502(error.to_string()))?)
            .map_err(|error| err_502(error.to_string()))?
    } else {
        NodeRecord {
            discovery_id: id.to_string(),
            node_id: config
                .nodes
                .iter()
                .map(|node| node.id)
                .chain(records(state)?.iter().map(|node| node.node_id))
                .max()
                .unwrap_or(0)
                + 1,
            physical_host_id: handshake.physical_host_id,
            rack_id: body.rack_id,
            host: host.clone(),
            ssh_port: body.ssh_port,
            ssh_user: body.ssh_user.clone(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            confirmed: false,
            cancelled: false,
        }
    };
    if record.cancelled {
        record.operation_id = uuid::Uuid::new_v4().to_string();
        record.cancelled = false;
    }
    let binding = binding(state)?;
    if binding.is_none() {
        record.host = host;
        record.rack_id = body.rack_id;
        record.ssh_user = body.ssh_user;
        record.ssh_port = body.ssh_port;
    }
    if let Some(binding) = &binding {
        record = registry::admit(
            state.kv_client().await.as_ref(),
            &binding.bootstrap.cluster_id,
            record,
        )
        .await
        .map_err(map_config_err)?;
    } else if state.runtime_root.join("prepared-bootstrap.json").exists() {
        return Err(err_409(
            "Bootstrap inputs are sealed; resume or explicitly clean up before changing membership",
        ));
    }
    save(&path, &record)?;
    let key = node_key_path(state);
    let mut target = as_node(&record);
    target.ssh_password = body.ssh_password;
    if target.ssh_password.is_none() {
        target.ssh_key = Some(key.to_string_lossy().into_owned());
    }
    admission::prepare(
        &control_socket(),
        &key,
        &record.operation_id,
        &record.discovery_id,
        &target,
        &config.nodes,
    )
    .await
    .map_err(map_config_err)?;
    target.ssh_password = None;
    target.ssh_key = None;
    target.ssh_credential_ref = Some("id_ed25519".into());
    if let Some(binding) = &binding {
        confirm_admission(state, binding, &key, &target, &mut record).await?;
    }
    update_config(state, &record, target)?;
    save(&path, &record)?;
    state.persist().map_err(map_persist_err)?;
    Ok(record)
}

pub(crate) async fn admissions(State(state): State<AppState>) -> Result<Json<Vec<NodeRecord>>, Failure> {
    if binding(&state)?.is_some() {
        if let Ok(Ok(Some((registry, _)))) = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            registry::read(state.kv_client().await.as_ref()),
        )
        .await
        {
            save(&state.runtime_root.join("confirmed-nodes.json"), &registry.nodes)?;
            return Ok(Json(registry.nodes));
        }
        if let Ok(bytes) = fs::read(state.runtime_root.join("confirmed-nodes.json")) {
            return Ok(Json(
                serde_json::from_slice(&bytes).map_err(|error| err_502(error.to_string()))?,
            ));
        }
    }
    Ok(Json(records(&state)?))
}

pub(crate) fn records(state: &AppState) -> Result<Vec<NodeRecord>, Failure> {
    let mut records = Vec::new();
    for entry in fs::read_dir(state.runtime_root.as_ref()).map_err(|error| err_502(error.to_string()))? {
        let entry = entry.map_err(|error| err_502(error.to_string()))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("admission-") && name.ends_with(".json") {
            let metadata = fs::symlink_metadata(entry.path()).map_err(|error| err_502(error.to_string()))?;
            if !metadata.is_file() || metadata.len() > 65536 {
                return Err(err_409("Invalid admission state"));
            }
            records.push(
                serde_json::from_slice(&fs::read(entry.path()).map_err(|error| err_502(error.to_string()))?)
                    .map_err(|error| err_502(error.to_string()))?,
            );
        }
    }
    Ok(records)
}

pub(crate) fn binding(state: &AppState) -> Result<Option<NodeBinding>, Failure> {
    let path = state
        .runtime_root
        .parent()
        .ok_or_else(|| err_409("Node workspace has no root"))?
        .join("node-binding.json");
    match fs::read(path) {
        Ok(bytes) => Ok(Some(
            serde_json::from_slice(&bytes).map_err(|error| err_502(error.to_string()))?,
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(err_502(error.to_string())),
    }
}

pub(super) fn as_node(record: &NodeRecord) -> NodeEntry {
    NodeEntry {
        id: record.node_id,
        rack_id: record.rack_id,
        host: record.host.clone(),
        ssh_port: record.ssh_port,
        ssh_user: record.ssh_user.clone(),
        ssh_key: None,
        ssh_password: None,
        ssh_credential_ref: None,
    }
}

pub(crate) fn control_socket() -> PathBuf {
    std::env::var_os("CROWDB_RUNTIME_ROOT")
        .map_or_else(|| PathBuf::from("/opt/crowdb/run"), PathBuf::from)
        .join("node-control.sock")
}
pub(crate) fn node_key_path(state: &AppState) -> PathBuf {
    state
        .runtime_root
        .parent()
        .unwrap_or(&state.runtime_root)
        .join("ssh/id_ed25519")
}
fn bracket_host(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}

pub(crate) fn save(path: &Path, record: &impl serde::Serialize) -> Result<(), Failure> {
    let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(|error| err_502(error.to_string()))?;
    file.write_all(&serde_json::to_vec(record).map_err(|error| err_502(error.to_string()))?)
        .and_then(|()| file.sync_all())
        .map_err(|error| err_502(error.to_string()))?;
    fs::rename(temporary, path)
        .and_then(|()| File::open(path.parent().unwrap()).and_then(|parent| parent.sync_all()))
        .map_err(|error| err_502(error.to_string()))
}

pub(super) fn update_config(state: &AppState, record: &NodeRecord, target: NodeEntry) -> Result<(), Failure> {
    {
        let mut config = state.config.write().map_err(|error| err_502(error.to_string()))?;
        if let Some(node) = config.nodes.iter_mut().find(|node| node.id == record.node_id) {
            *node = target;
        } else {
            config.nodes.push(target);
        }
        if config.server_for_node(record.node_id).is_none() {
            let mut server = ServerEntry::new(
                format!("kv-{}", record.node_id),
                format!("http://{}:10000", bracket_host(&record.host)),
            );
            server.node_id = Some(record.node_id);
            server.rpc_url = Some(format!("{}:10100", bracket_host(&record.host)));
            config.servers.push(server);
        }
    }
    Ok(())
}

async fn confirm_admission(
    state: &AppState,
    binding: &NodeBinding,
    key: &Path,
    target: &NodeEntry,
    record: &mut NodeRecord,
) -> Result<(), Failure> {
    let ctx = state.op_context().await.map_err(map_config_err)?;
    let mut execution_node = target.clone();
    execution_node.ssh_key = Some(key.to_string_lossy().into_owned());
    if !record.confirmed {
        let credentials = admission::local_control(
            &control_socket(),
            &crowdb_protocol::mgmt::node::NodeControl::ServiceCredentials {
                cluster_id: binding.bootstrap.cluster_id.clone(),
            },
        )
        .await
        .map_err(map_config_err)?;
        admission::remote_controls(
            &execution_node,
            &[
                crowdb_protocol::mgmt::node::NodeControl::StartKv {
                    node_id: record.node_id,
                    bootstrap: binding.bootstrap.clone(),
                    manifest: None,
                    credentials: None,
                    admission: Some(crowdb_protocol::mgmt::node::NodeAdmissionGrant {
                        operation_id: record.operation_id.clone(),
                        management_seeds: binding.management_seeds.clone(),
                    }),
                },
                crowdb_protocol::mgmt::node::NodeControl::ProvisionServiceCredentials {
                    bootstrap: binding.bootstrap.clone(),
                    credentials: serde_json::from_value(credentials)
                        .map_err(|error| err_502(error.to_string()))?,
                },
            ],
        )
        .await
        .map_err(map_config_err)?;
        crowdb_console_shared::ops::hardware::add_node_to_group0(&ctx, target.clone())
            .await
            .map_err(map_config_err)?;
        record.confirmed = true;
        *record = registry::admit(ctx.kv(), &binding.bootstrap.cluster_id, record.clone())
            .await
            .map_err(map_config_err)?;
    }
    admission::remote_control(
        &execution_node,
        &crowdb_protocol::mgmt::node::NodeControl::Bind {
            binding: NodeBinding {
                node_id: record.node_id,
                bootstrap: binding.bootstrap.clone(),
                management_seeds: binding.management_seeds.clone(),
            },
        },
    )
    .await
    .map_err(map_config_err)?;
    Ok(())
}
