// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::state::State;
use crowdb_console_shared::error::{Error, Result};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::mgmt::node::setup::{NodeSetupChoices, SetupStorage};
use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

mod storage;

struct Api {
    client: Client,
    web: String,
    kv: CrowdbKvClient,
}

impl Api {
    async fn request(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        self.request_at(&self.web, method, path, body).await
    }

    async fn request_at(
        &self,
        origin: &str,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value> {
        let read = method == Method::GET;
        let mut request = self.client.request(method, format!("{origin}{path}"));
        if read {
            request = request.timeout(Duration::from_secs(2));
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|e| Error::Config(format!("container cluster request {path}: {e}")))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|error| Error::Config(format!("container cluster response {path}: {error:?}")))?;
        if !status.is_success() {
            return Err(Error::Config(format!(
                "container cluster {path}: {status}: {text}"
            )));
        }
        if text.is_empty() {
            Ok(Value::Null)
        } else {
            serde_json::from_str(&text).map_err(|e| Error::Config(e.to_string()))
        }
    }
    async fn get(&self, path: &str) -> Result<Value> {
        self.request(Method::GET, path, None).await
    }
    async fn post(&self, path: &str, body: Value) -> Result<Value> {
        self.request(Method::POST, path, Some(body)).await
    }
}

pub(super) async fn run(state: &mut State) -> Result<()> {
    let api = Api {
        client: Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|e| Error::Config(e.to_string()))?,
        web: state.web()?,
        kv: CrowdbKvClient::new(ClientConfig::new(
            state
                .addresses
                .iter()
                .map(|address| format!("http://{address}:10000"))
                .collect(),
        )),
    };
    wait_web(&api, state).await?;
    // This CLI owns fixed node identities and explicitly initializes Group 0.
    for address in &state.addresses {
        api.request_at(
            &format!("http://{address}:9090"),
            Method::POST,
            "/api/node/auto-join",
            Some(json!({"enabled": false})),
        )
        .await?;
    }
    let phase = api.get("/api/node/status").await?;
    if phase["phase"] != "active" {
        prepare(&api, state).await?;
        let phase = api.get("/api/node/status").await?;
        if phase["phase"] != "active" {
            api.post("/api/cluster/init", json!({"nodes":[1,2,3]})).await?;
        }
    }
    wait_phase(&api, "active").await?;
    state.phase = "bound".into();
    state.client_record()?;
    state.save()?;
    storage::prepare(&api, state).await?;
    storage::services(&api, state).await?;
    Ok(())
}

async fn wait_web(api: &Api, state: &State) -> Result<()> {
    for address in &state.addresses {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if api
                .client
                .get(format!("http://{address}:9090/healthz"))
                .timeout(Duration::from_secs(2))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
            if Instant::now() >= deadline {
                return Err(Error::Config(format!(
                    "container Web endpoint http://{address}:9090 is not reachable from the host"
                )));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(())
}

async fn wait_phase(api: &Api, phase: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let observed = api.get("/api/node/status").await?;
        if observed["phase"] == phase {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Error::Config(format!(
                "container cluster phase is not {phase}: {observed}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn prepare(api: &Api, state: &mut State) -> Result<()> {
    let racks = api.get("/api/racks").await?;
    if !racks
        .as_array()
        .is_some_and(|racks| racks.iter().any(|rack| rack["id"] == 1))
    {
        api.post("/api/racks", json!({"id":1,"name":"container cluster"}))
            .await?;
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    let candidates = loop {
        let observed = api.get("/api/node/candidates").await?;
        if observed["candidates"]
            .as_array()
            .is_some_and(|entries| entries.len() == 3)
        {
            break observed;
        }
        if Instant::now() >= deadline {
            return Err(Error::Config("three container nodes were not discovered".into()));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let entries = candidates["candidates"]
        .as_array()
        .ok_or_else(|| Error::Config("node candidates are invalid".into()))?;
    for (index, address) in state.addresses.iter().enumerate() {
        let candidate = entries
            .iter()
            .find(|entry| {
                entry["advertisement"]["monitor_endpoints"]
                    .as_array()
                    .is_some_and(|endpoints| {
                        endpoints.iter().any(|endpoint| {
                            endpoint
                                .as_str()
                                .and_then(|s| reqwest::Url::parse(s).ok())
                                .is_some_and(|url| url.host_str() == Some(address))
                        })
                    })
            })
            .ok_or_else(|| Error::Config("container endpoint has no discovered node identity".into()))?;
        let discovery = &candidate["advertisement"]["discovery_id"];
        let admissions = api.get("/api/node/admissions").await?;
        if let Some(record) = admissions
            .as_array()
            .and_then(|nodes| nodes.iter().find(|node| node["discovery_id"] == *discovery))
        {
            if record["node_id"] != index + 1 {
                return Err(Error::Config(
                    "container node admission changed its fixed identity".into(),
                ));
            }
            continue;
        }
        let password = std::env::var("CROWDB_SSH_PASSWORD").unwrap_or_else(|_| "crowdb".into());
        // The CLI resumes its own fixed storage and service deployment steps.
        let mut setup = NodeSetupChoices {
            storage: SetupStorage::Diskless,
            ..Default::default()
        };
        for service in &mut setup.services {
            service.enabled = false;
        }
        let record = api
            .post(
                "/api/node/admit",
                json!({"discovery_id":discovery,"rack_id":1,"ssh_user":"crowdb",
                    "ssh_port":2222,"ssh_password":password,"setup":setup}),
            )
            .await?;
        if record["node_id"] != index + 1 {
            return Err(Error::Config(
                "container node admission returned a different identity".into(),
            ));
        }
    }
    state.phase = "prepared".into();
    state.save()
}
