// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{json, Api, Duration, Error, Instant, Result, State, Value};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, ServiceRegistryClient};

mod files;

pub(super) async fn prepare(api: &Api, state: &mut State) -> Result<()> {
    let groups = api.get("/api/stores/0/groups").await?;
    if !array(&groups, "Store groups")?
        .iter()
        .any(|group| group["group_id"] == 1)
    {
        api.post(
            "/api/stores/0/groups",
            json!({"group_id":1,"replica_id":10,"nodes":[1,2,3]}),
        )
        .await?;
    }
    for node in 1..=3 {
        let groups = api.get(&format!("/api/nodes/{node}/disk-groups")).await?;
        if !array(&groups, "Disk groups")?
            .iter()
            .any(|group| group["id"] == node)
        {
            api.post(
                &format!("/api/nodes/{node}/disk-groups"),
                json!({"id":node,"store_id":0,"group_id":1,"name":"container storage"}),
            )
            .await?;
        }
        if !has_service(api, node, &format!("diskdb-{node}")).await? {
            api.post(
                &format!("/api/nodes/{node}/diskdb/deploy"),
                json!({"rpc_port":7100,"keepalive_interval_secs":state.diskdb_heartbeat_interval_secs}),
            )
            .await?;
            state.phase = "storage".into();
            state.save()?;
        }
        let disks = api
            .get(&format!("/api/nodes/{node}/disk-groups/{node}/disks"))
            .await?;
        if disks
            .as_array()
            .ok_or_else(|| Error::Config("disk inventory is invalid".into()))?
            .is_empty()
        {
            files::prepare(api, state, node).await?;
        }
    }
    wait_http(api, &state.addresses, 7101, "/ready", "Diskdb").await?;
    wait_diskdb_ownership(api, state).await
}

async fn wait_diskdb_ownership(api: &Api, state: &State) -> Result<()> {
    let seeds = state
        .addresses
        .iter()
        .map(|address| format!("http://{address}:10000"))
        .collect();
    let group = api.get("/api/stores/0/groups/0").await?;
    let leader = array(&group["replicas"], "Group-0 replicas")?
        .iter()
        .find(|replica| replica["role"] == "leader")
        .and_then(|replica| replica["node_id"].as_u64())
        .and_then(|node| usize::try_from(node).ok())
        .and_then(|node| node.checked_sub(1))
        .and_then(|index| state.addresses.get(index))
        .ok_or_else(|| Error::Config("Group 0 has no observed cluster leader".into()))?;
    let client = CrowdbKvClient::new(ClientConfig::new(seeds));
    client.seed_leader(0, 0, format!("{leader}:10100"));
    let registry = ServiceRegistryClient::new(client);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let instances = tokio::time::timeout(
            remaining.min(Duration::from_secs(2)),
            registry.read_all_diskdb_instances(),
        )
        .await;
        let last = match instances {
            Ok(Ok(instances)) => {
                let endpoints: Vec<_> = state
                    .addresses
                    .iter()
                    .map(|address| format!("{address}:7102"))
                    .collect();
                let missing: Vec<_> = (0..3)
                    .filter_map(|index| {
                        let group = u64::try_from(index + 1).expect("three node index");
                        (!instances.iter().any(|(_, value)| {
                            endpoints.contains(&value.rpc_endpoint)
                                && value
                                    .extra
                                    .as_ref()
                                    .and_then(|extra| extra.diskdb.as_ref())
                                    .is_some_and(|extra| extra.owned_dg_ids.contains(&group))
                        }))
                        .then_some(group)
                    })
                    .collect();
                if missing.is_empty() {
                    return Ok(());
                }
                format!("disk groups {missing:?} have no live Diskdb endpoint")
            }
            Ok(Err(error)) => error.to_string(),
            Err(error) => error.to_string(),
        };
        if Instant::now() >= deadline {
            return Err(Error::Config(format!(
                "Diskdb ownership is not published: {last}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn array<'a>(value: &'a Value, description: &str) -> Result<&'a Vec<Value>> {
    value
        .as_array()
        .ok_or_else(|| Error::Config(format!("{description} inventory is invalid")))
}

async fn has_service(api: &Api, node: usize, id: &str) -> Result<bool> {
    // Deployment ownership comes from committed intent, independently of
    // whether the monitor has finished restarting the retained process.
    let node = u64::try_from(node).map_err(|error| Error::Config(error.to_string()))?;
    let intent = crowdb_console_shared::deployment::services::current(&api.kv, node, id)
        .await
        .map_err(|error| Error::Config(format!("read deployment intent {node}/{id}: {error:?}")))?;
    Ok(intent.is_some_and(|intent| intent.action != crowdb_protocol::mgmt::node::NodeServiceAction::Delete))
}

pub(super) async fn services(api: &Api, state: &mut State) -> Result<()> {
    for (kind, http, rpc) in [
        ("diskio", None, Some(7200)),
        ("chunkdb", Some(7300), Some(7301)),
        ("chunk-kv", Some(7400), Some(7401)),
        ("access-server", Some(9000), None),
    ] {
        if kind == "chunkdb" {
            wait_storage(api).await?;
        }
        if kind == "access-server" {
            wait_chunk_kv(api, state).await?;
        }
        for node in 1..=3 {
            if !has_service(api, node, &format!("{kind}-{node}")).await? {
                let defaults = api
                    .get(&format!("/api/deployment-defaults?node_id={node}"))
                    .await?;
                let mut body = defaults[kind].clone();
                body["kind"] = json!(kind);
                body["instance_id"] = json!(node.to_string());
                body["local_deployment"] = json!(false);
                if let Some(port) = http {
                    body["http_port"] = json!(port);
                }
                if let Some(port) = rpc {
                    body["rpc_port"] = json!(port);
                }
                match kind {
                    "diskio" => body["disk_group_id"] = json!(node),
                    "chunk-kv" => {
                        body["metadata_store_id"] = json!(0);
                        body["bootstrap_group_id"] = json!(1);
                    }
                    "access-server" => {
                        body["s3_port"] = json!(9001);
                        body["health_port"] = json!(9002);
                    }
                    _ => {}
                }
                api.post(&format!("/api/nodes/{node}/services/deploy"), body)
                    .await?;
                state.phase = format!("deploying-{kind}");
                state.save()?;
            }
            let address = std::slice::from_ref(&state.addresses[node - 1]);
            match kind {
                "chunkdb" => wait_http(api, address, 7300, "/ready", "Chunkdb").await?,
                "chunk-kv" => wait_http(api, address, 7400, "/ready", "Chunk-KV catalog").await?,
                "access-server" => wait_http(api, address, 9002, "/_crowdb/health/ready", "Access").await?,
                _ => {}
            }
        }
    }
    wait_access(api, state).await
}

async fn wait_storage(api: &Api) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let state = api.get("/api/chunk-storage-readiness").await?;
        if state["ready"] == true {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Error::Config(format!("DiskIO ownership is not ready: {state}")));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_chunk_kv(api: &Api, state: &State) -> Result<()> {
    wait_http(api, &state.addresses, 7400, "/ready", "Chunk-KV catalog").await
}

async fn wait_access(api: &Api, state: &State) -> Result<()> {
    wait_http(api, &state.addresses, 9002, "/_crowdb/health/ready", "Access S3").await
}

async fn wait_http(api: &Api, addresses: &[String], port: u16, path: &str, description: &str) -> Result<()> {
    for address in addresses {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut last;
        loop {
            match api
                .client
                .get(format!("http://{address}:{port}{path}"))
                .timeout(
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_secs(2)),
                )
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => break,
                Ok(response) => {
                    last = format!(
                        "{}: {}",
                        response.status(),
                        response.text().await.map_err(|error| Error::Config(format!(
                            "{description} readiness response from {address}:{port}{path}: {error:?}"
                        )))?
                    );
                }
                Err(error) => last = error.to_string(),
            }
            if Instant::now() >= deadline {
                return Err(Error::Config(format!(
                    "{description} at {address}:{port} is not ready: {last}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(())
}
