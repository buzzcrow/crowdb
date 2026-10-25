// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod inspection;
mod options;
mod ownership;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use crowdb_console_shared::{
    config::{ConsoleConfig, ServiceType},
    deployment::PreparedBootstrap,
    ops::{cluster, OpContext},
};
use crowdb_protocol::mgmt::cluster::ClusterTargetIdentity;
use options::TestSetup;
use ownership::{process_start, TestOwnedProcesses};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

pub type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
struct TestFixtureArgs {
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    state: PathBuf,
    #[arg(long)]
    owner_pid: u32,
    #[arg(last = true)]
    setup: Vec<String>,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
enum TestControl {
    Inspect,
    Setup {
        arguments: Vec<String>,
    },
    Reset {
        store: u64,
        group: u64,
        restart_storage: bool,
    },
    Stop,
}

type TestRequest = (TestControl, oneshot::Sender<Result<serde_json::Value, String>>);

#[derive(Clone)]
struct TestControlState {
    identity: ClusterTargetIdentity,
    token: String,
    requests: mpsc::Sender<TestRequest>,
}

fn write_private(path: &Path, body: &[u8]) -> TestResult<()> {
    if fs::symlink_metadata(path).is_ok_and(|metadata| !metadata.file_type().is_file()) {
        return Err("fixture state path must be a regular file".into());
    }
    let temporary = path.with_extension("new");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(body)?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn parse_setup(arguments: Vec<String>) -> TestResult<TestSetup> {
    let setup = TestSetup::try_parse_from(std::iter::once("fixture-setup".to_owned()).chain(arguments))?;
    if !matches!(setup.nodes, 1 | 3) {
        return Err("fixture requires one or three KV voters".into());
    }
    Ok(setup)
}

async fn setup(
    context: &OpContext,
    root: &Path,
    options: &TestSetup,
    owned: &mut TestOwnedProcesses,
) -> TestResult<()> {
    let workspace = root.join("services");
    match options.kind.as_str() {
        "kv" | "combined" => {
            if !context.config().servers.is_empty() {
                return Err("KV fixture already exists; stop it before replacing inputs".into());
            }
            let (_, members) =
                cluster::prepare_local_deploy(context, options.nodes, Some(&workspace), Some(&options.kv()))
                    .await?;
            let config = context.config().clone();
            owned.record(context)?;
            let prepared = PreparedBootstrap::open(&root.join("bootstrap.json"), &config, &members)?;
            let result = prepared.execute(context).await;
            // Bootstrap seals topology and intentionally drops process-local
            // fields. This test fixture still owns the native children.
            context.config_mut().servers = config.servers;
            result?;
            if options.kind == "combined" {
                cluster::local_deploy_combined_after_kv(
                    context,
                    &workspace,
                    &options.disk(),
                    &options.chunk(),
                    &options.diskio_dummy_disk_type,
                )
                .await?;
            }
        }
        "diskdb" => {
            cluster::local_deploy_diskdb(context, &workspace, &options.disk()).await?;
        }
        "chunkdb" => {
            cluster::local_deploy_chunkdb(context, &workspace, &options.chunk()).await?;
        }
        _ => return Err("unsupported fixture kind".into()),
    }
    Ok(())
}

fn identity(context: &OpContext, root: &Path) -> TestResult<ClusterTargetIdentity> {
    let prepared = PreparedBootstrap::load(&root.join("bootstrap.json"))?;
    let config = context.config();
    let servers: Vec<_> = config
        .servers
        .iter()
        .filter(|server| server.service_type == ServiceType::PaxosKv)
        .collect();
    Ok(ClusterTargetIdentity {
        version: 1,
        cluster_id: prepared.identity.cluster_id,
        group0_management_endpoints: servers.iter().map(|server| server.url.clone()).collect(),
        group0_rpc_endpoints: servers
            .iter()
            .filter_map(|server| server.rpc_url.as_ref())
            .map(|url| url.trim_start_matches("http://").to_owned())
            .collect(),
    })
}

async fn control(
    State(state): State<TestControlState>,
    headers: HeaderMap,
    Json(command): Json<TestControl>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if headers
        .get("X-Crowdb-Fixture-Owner")
        .and_then(|value| value.to_str().ok())
        != Some(&state.token)
    {
        return Err((StatusCode::FORBIDDEN, "fixture ownership token differs".into()));
    }
    let (reply, received) = oneshot::channel();
    state
        .requests
        .send((command, reply))
        .await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "fixture stopped".into()))?;
    let value = received
        .await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "fixture stopped".into()))?
        .map_err(|error| (StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(value))
}

async fn command(
    context: &OpContext,
    root: &Path,
    request: TestControl,
    owned: &mut TestOwnedProcesses,
) -> TestResult<(bool, serde_json::Value)> {
    match request {
        TestControl::Inspect => {
            return Ok((
                false,
                serde_json::json!({"ok": true,
            "result": tokio::time::timeout(Duration::from_secs(10), inspection::read(context)).await??}),
            ))
        }
        TestControl::Setup { arguments } => setup(context, root, &parse_setup(arguments)?, owned).await?,
        TestControl::Reset {
            store,
            group,
            restart_storage,
        } => {
            if (store, group) == (0, 0) {
                return Err("fixture reset cannot wipe Group 0 authority".into());
            }
            let mut result = cluster::clean(context, store, group).await?;
            if restart_storage {
                result.restarted_services = cluster::restart_storage_services(context).await?;
            }
            return Ok((false, serde_json::json!({"ok": true, "result": result})));
        }
        TestControl::Stop => return Ok((true, serde_json::json!({"ok": true}))),
    }
    Ok((false, serde_json::json!({"ok": true})))
}

#[derive(Serialize)]
struct TestReady<'a> {
    endpoint: String,
    token: &'a str,
    root: &'a Path,
    pid: u32,
    start: String,
}

pub async fn run() -> TestResult<()> {
    let args = TestFixtureArgs::parse();
    let owner_start = process_start(args.owner_pid).ok_or("fixture owner is not alive")?;
    let mut owned = TestOwnedProcesses::new(&args.root)?;
    let context = OpContext::new("127.0.0.1:1".into(), vec![], ConsoleConfig::default());
    let result = setup(&context, &args.root, &parse_setup(args.setup)?, &mut owned).await;
    owned.record(&context)?;
    result?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let token = uuid::Uuid::new_v4().to_string();
    let (requests, mut receive) = mpsc::channel(8);
    let state = TestControlState {
        identity: identity(&context, &args.root)?,
        token: token.clone(),
        requests,
    };
    let router = Router::new()
        .route(
            "/api/cluster/target",
            get(|State(state): State<TestControlState>| async move { Json(state.identity) }),
        )
        .route("/_fixture/control", post(control))
        .with_state(state);
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    write_private(&args.state, toml::to_string(&*context.config())?.as_bytes())?;
    let ready = TestReady {
        endpoint,
        token: &token,
        root: &args.root,
        pid: std::process::id(),
        start: process_start(std::process::id()).unwrap(),
    };
    write_private(
        &args.state.with_extension("fixture.json"),
        &serde_json::to_vec(&ready)?,
    )?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            Some((request, reply)) = receive.recv() => {
                let result = command(&context, &args.root, request, &mut owned).await;
                owned.record(&context)?;
                write_private(&args.state, toml::to_string(&*context.config())?.as_bytes())?;
                let stop = result.as_ref().is_ok_and(|(stop, _)| *stop);
                let _ = reply.send(result.map(|(_, value)| value).map_err(|error| error.to_string()));
                if stop { break; }
            }
            _ = terminate.recv() => break,
            _ = tokio::signal::ctrl_c() => break,
            _ = tick.tick() => if process_start(args.owner_pid).as_ref() != Some(&owner_start) { break; },
        }
    }
    let _ = stop.send(());
    server.await??;
    let result = owned.shutdown().await;
    write_private(
        &args.state.with_extension("fixture-stopped.json"),
        &serde_json::to_vec(&serde_json::json!({
            "pid": ready.pid, "start": ready.start, "ok": result.is_ok(),
            "error": result.as_ref().err().map(ToString::to_string)
        }))?,
    )?;
    result
}
