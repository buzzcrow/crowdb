// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::Path;

use crowdb_console_shared::{
    config::{LocalLaunchSpec, NodeEntry},
    lifecycle::{self, ChunkdbDeployRequest, DiskioDeployRequest},
};
use serde_json::json;

use super::{Deploy, Kind};
use crate::{
    error::{err_400, err_500, err_502},
    services::Failure,
    state::AppState,
};

pub(super) struct Launched {
    pub(super) url: String,
    pub(super) rpc_url: Option<String>,
    pub(super) pid: u32,
    pub(super) spec: LocalLaunchSpec,
}

pub(super) async fn launch(
    state: &AppState,
    node: &NodeEntry,
    id: &str,
    seeds: &[String],
    body: &Deploy,
    workspace: &Path,
) -> Result<Launched, Failure> {
    match body.kind {
        Kind::Chunkdb => {
            let request = ChunkdbDeployRequest {
                server_id: id.into(),
                instance_id: body.instance_id,
                http_port: body.http_port.unwrap(),
                rpc_port: body.rpc_port.unwrap(),
                kv_server_mgmt_seeds: seeds.to_vec(),
                allow_unsafe_ec: body.test_single_node,
                rpc_workers: None,
                kv_connections: None,
                kv_client_rpc_workers: None,
                diskdb_connections: None,
                diskdb_client_rpc_workers: None,
                metrics_interval: None,
            };
            let deployed = lifecycle::deploy_chunkdb_local(&request, node, workspace)
                .await
                .map_err(|error| err_502(error.to_string()))?;
            Ok(Launched {
                url: format!("http://{}:{}", node.host, request.http_port),
                rpc_url: Some(deployed.endpoint),
                pid: deployed.pid,
                spec: deployed.launch,
            })
        }
        Kind::Diskio => {
            let request = DiskioDeployRequest {
                server_id: id.into(),
                instance_id: body.instance_id,
                rpc_port: body.rpc_port.unwrap(),
                rack_id: node.rack_id,
                node_id: node.id,
                disk_group_id: body.disk_group_id.unwrap(),
                kv_server_mgmt_seeds: seeds.to_vec(),
                dummy_disk_type: "mem".into(),
                rpc_workers: None,
                metrics_interval: None,
                o_direct: true,
                disks: vec![],
            };
            let deployed = lifecycle::deploy_diskio_local(&request, node, workspace)
                .await
                .map_err(|error| err_502(error.to_string()))?;
            Ok(Launched {
                url: deployed.endpoint.clone(),
                rpc_url: Some(deployed.endpoint),
                pid: deployed.pid,
                spec: deployed.launch,
            })
        }
        Kind::ChunkKv | Kind::AccessServer => native(state, node, id, seeds, body, workspace).await,
    }
}

async fn native(
    state: &AppState,
    node: &NodeEntry,
    id: &str,
    seeds: &[String],
    body: &Deploy,
    workspace: &Path,
) -> Result<Launched, Failure> {
    let http = format!("http://{}:{}", node.host, body.http_port.unwrap());
    let rpc = body.rpc_port.map(|port| format!("{}:{port}", node.host));
    let (config, env_file, ready) = match body.kind {
        Kind::ChunkKv => (
            chunk_kv_config(body, seeds, node).await?,
            None,
            format!("{http}/health"),
        ),
        Kind::AccessServer => {
            // One cluster key set survives removal/redeployment of individual Access instances.
            crowdb_monitor::ServerCredentials::load_or_create(state.runtime_root.as_ref())
                .map_err(|error| err_500(error.to_string()))?;
            let config = access_config(body, seeds, node);
            (
                config,
                Some(state.runtime_root.join("secrets/server.env")),
                format!(
                    "http://{}:{}/_crowdb/health/ready",
                    node.host,
                    body.s3_port.unwrap()
                ),
            )
        }
        _ => return Err(err_400("Unsupported native service")),
    };
    let mut spec =
        lifecycle::prepare_native_launch(body.kind.service_type(), workspace, &config, env_file, ready)
            .map_err(|error| err_502(error.to_string()))?;
    if matches!(body.kind, Kind::AccessServer) {
        spec.env.insert("CROWDB_ICEBERG_PUBLIC_URI".into(), http.clone());
        spec.env.insert(
            "CROWDB_S3_PUBLIC_URI".into(),
            format!("http://{}:{}", node.host, body.s3_port.unwrap()),
        );
        super::credentials::prepare(state.runtime_root.as_ref(), &spec, seeds).await?;
    }
    let pid = lifecycle::restart_local_service(id, 0, &spec)
        .await
        .map_err(|error| err_502(error.to_string()))?;
    Ok(Launched {
        url: http,
        rpc_url: rpc,
        pid,
        spec,
    })
}

async fn chunk_kv_config(
    body: &Deploy,
    seeds: &[String],
    node: &NodeEntry,
) -> Result<serde_json::Value, Failure> {
    let store = body.metadata_store_id.unwrap();
    let mut config = json!({ "instance_id":body.instance_id, "node_id":node.id,
        "rpc_listen_addr":format!("{}:{}",node.host,body.rpc_port.unwrap()),
        "rpc_advertise_addr":format!("{}:{}",node.host,body.rpc_port.unwrap()),
        "http_listen_addr":format!("{}:{}",node.host,body.http_port.unwrap()),
        "http_advertise_addr":format!("{}:{}",node.host,body.http_port.unwrap()),
        "group0_mgmt_seeds":seeds, "storage":{"metadata_store_id":store,"stream_mirror_copies":if body.test_single_node {1} else {2}},
    });
    if let Some(group) = body.bootstrap_group_id {
        let client =
            crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(seeds.to_vec()));
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.get(
                store,
                group,
                b"console-deployment-probe",
                crowdb_kv_client::ReadMode::Linearizable,
                None,
            ),
        )
        .await
        .map_err(|_| err_502("Metadata group readiness probe timed out"))?
        .map_err(|error| err_502(format!("Metadata group is not readable: {error}")))?;
        let (high, low) = uuid::Uuid::new_v4().as_u64_pair();
        let high = high & (u64::MAX >> 1);
        let low = low & (u64::MAX >> 1);
        config["bootstrap_partition"] = json!({"partition_id":{"high":high,"low":low},
            "tree_id":low.max(1), "stream_name":{"high":high,"low":low ^ 1}, "owner_epoch":1,"metadata_group_id":group});
    }
    Ok(config)
}

fn access_config(body: &Deploy, seeds: &[String], node: &NodeEntry) -> serde_json::Value {
    let mut config = json!({ "common":{"management_seeds":seeds},
        "s3":{"listen":format!("{}:{}",node.host,body.s3_port.unwrap()),"tenant":"default","region":"us-east-1"},
        "iceberg":{"listen":format!("{}:{}",node.host,body.http_port.unwrap())},
    });
    if body.test_single_node {
        config["deployment"] = json!({"mode":"test_single_node","max_node_failures":0});
        config["small_write"] = json!({"mirror_copies":1,"conversion_enabled":false,"ec_data":2,"ec_code":1});
        for name in ["s3", "iceberg"] {
            config[name]["large_mirror_copies"] = json!(1);
        }
        config["s3"]["small_write"] =
            json!({"mirror_copies":1,"conversion_enabled":false,"ec_data":2,"ec_code":1});
    }
    config
}
