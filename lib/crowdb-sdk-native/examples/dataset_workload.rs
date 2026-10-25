// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reproducible native workload over the HTTP-prepared immutable snapshot; the external runner owns fixtures and affinity.

use crowdb_access_dataset::{DatasetIdentity, NamespacePath, SnapshotId};
use crowdb_sdk_native::{
    dataset::{BatchRequest, DatasetClient},
    ClientRuntime, OperationContext, RuntimeConfig,
};
use md5::{Digest, Md5};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
static REQUEST: AtomicU64 = AtomicU64::new(1);
use std::{
    num::NonZeroU64,
    path::Path,
    time::{Duration, Instant},
};

type Failure = Box<dyn std::error::Error + Send + Sync>;
const SIZES: [usize; 8] = [0, 128, 513, 4096, 65_535, 262_144, 1_048_576, 4_194_321];

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Failure> {
    let mut config = RuntimeConfig::new(
        std::env::var("CROWDB_MANAGEMENT_SEEDS")?
            .split(',')
            .map(str::to_owned)
            .collect(),
    );
    config.data_rdma = Some(crowdb_rpc_ffi::RdmaConfig {
        enabled: false,
        ..Default::default()
    });
    let runtime = ClientRuntime::connect(config).await?;
    let client = DatasetClient::new(
        &runtime,
        DatasetIdentity::new(NamespacePath::root(), std::env::var("CROWDB_DATASET_NAME")?)?,
    )?;
    let root = std::env::var("CROWDB_DATASET_CONSUMER_ROOT")?;
    let root = Path::new(&root);
    let snapshot = std::fs::read_to_string(root.join("snapshot"))?;
    for concurrency in [1, 3] {
        trial(&client, &snapshot, concurrency, 2).await?;
        for repeat in 0..3 {
            let observation = observation_trial("native-rust", &snapshot, concurrency, repeat)?;
            observe(observation.as_ref(), "ready", "go", None, || {
                local_observation(&runtime)
            })?;
            let result = trial(&client, &snapshot, concurrency, 16).await?;
            observe(observation.as_ref(), "done", "ack", Some(&result), || {
                local_observation(&runtime)
            })?;
            let report = json!({"path":"native-rust", "snapshot":snapshot, "sizes":SIZES,
                "concurrency":concurrency, "repeat":repeat, "result":result,
                "digest":digest_cost(), "load":"closed-loop", "warmup_rounds":2,
                "in_flight_budget_bytes":256 * 1024 * 1024, "implicit_http_retry":false});
            std::fs::write(
                root.join(format!("native-rust-{concurrency}-{repeat}.json")),
                serde_json::to_vec_pretty(&report)?,
            )?;
            println!("{report}");
        }
    }
    let resources = runtime.resources().status();
    std::fs::write(
        root.join("native-rust-resources.json"),
        serde_json::to_vec_pretty(&json!({
        "held_bytes":resources.held_bytes,"high_water":resources.held_bytes_high_water,
        "compatibility_copy_bytes":resources.compatibility_copy_bytes}))?,
    )?;
    runtime.shutdown().await?;
    Ok(())
}

fn snapshot_id(value: &str) -> Result<SnapshotId, Failure> {
    let bytes: Result<Vec<_>, _> = (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16))
        .collect();
    Ok(SnapshotId::from_bytes(&bytes?)?)
}

async fn read(client: &DatasetClient, snapshot: &str, index: usize) -> Result<u64, Failure> {
    let start = Instant::now();
    let context = OperationContext::new(
        NonZeroU64::new(REQUEST.fetch_add(1, Ordering::Relaxed)).unwrap(),
        Instant::now() + Duration::from_secs(30),
    );
    let mut batch = client
        .read_batch(
            context,
            BatchRequest {
                snapshot: snapshot_id(snapshot)?,
                sample_ids: vec![format!("size-{index}").into_bytes()],
                fields: vec!["value".into()],
            },
        )
        .await?;
    if batch.header.snapshot.to_string() != snapshot || batch.header.fields.len() != 1 {
        return Err("unexpected snapshot/projection".into());
    }
    let mut length = 0;
    while let Some(field) = batch.body.next().await? {
        let data = field.buffer.as_slice();
        if data.iter().any(|byte| usize::from(*byte) != index) {
            return Err("payload mismatch".into());
        }
        length += data.len();
    }
    if length != SIZES[index] {
        return Err("payload length mismatch".into());
    }
    Ok(u64::try_from(start.elapsed().as_micros())?)
}

async fn trial(
    client: &DatasetClient,
    snapshot: &str,
    concurrency: usize,
    rounds: usize,
) -> Result<serde_json::Value, Failure> {
    let cpu = cpu_ticks()?;
    let start = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for lane in 0..concurrency {
        let client = client.clone();
        let snapshot = snapshot.to_owned();
        tasks.spawn(async move {
            let mut observations = Vec::new();
            for round in 0..rounds {
                for order in 0..SIZES.len() {
                    let index = (order * 5 + lane + round) % SIZES.len();
                    observations.push((index, read(&client, &snapshot, index).await?));
                }
            }
            Ok::<_, Failure>(observations)
        });
    }
    let mut times: [Vec<u64>; 8] = std::array::from_fn(|_| Vec::new());
    while let Some(result) = tasks.join_next().await {
        for (index, micros) in result?? {
            times[index].push(micros);
        }
    }
    let seconds = start.elapsed().as_secs_f64();
    let cpu = cpu_ticks()? - cpu;
    let mut latency = Vec::new();
    for (index, values) in times.iter_mut().enumerate() {
        values.sort_unstable();
        latency.push(json!({"bytes":SIZES[index], "count":values.len(),
            "p50_us":values[(values.len()-1)/2], "p99_us":values[(values.len()*99).div_ceil(100)-1]}));
    }
    let requests = concurrency * rounds * SIZES.len();
    let bytes = concurrency * rounds * SIZES.iter().sum::<usize>();
    Ok(
        json!({"seconds":seconds, "completed_requests":requests, "bytes":bytes,
        "cpu_ticks":cpu, "rss_kib":rss_kib()?, "latency":latency, "errors":0}),
    )
}

fn cpu_ticks() -> Result<u64, Failure> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let (_, fields) = stat.rsplit_once(") ").ok_or("invalid proc stat")?;
    let fields: Vec<_> = fields.split_whitespace().collect();
    Ok(fields[11].parse::<u64>()? + fields[12].parse::<u64>()?)
}
fn rss_kib() -> Result<u64, Failure> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let line = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .ok_or("missing VmHWM")?;
    Ok(line.split_whitespace().nth(1).ok_or("missing RSS")?.parse()?)
}
fn digest_cost() -> serde_json::Value {
    let data = vec![93; 1024 * 1024];
    let start = Instant::now();
    for _ in 0..64 {
        std::hint::black_box(Md5::digest(std::hint::black_box(&data)));
    }
    json!({"algorithm":"md5", "bytes":64 * data.len(), "microseconds":start.elapsed().as_micros(),
        "scope":"separate resident-buffer kernel; real reads always verify integrity"})
}

fn observation_trial(
    path: &str,
    snapshot: &str,
    concurrency: usize,
    repeat: usize,
) -> Result<Option<serde_json::Value>, Failure> {
    let run = match std::env::var("CROWDB_DATASET_OBSERVATION_RUN_ID") {
        Ok(run) if !run.is_empty() => run,
        Ok(_) | Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if [run.as_str(), snapshot].iter().any(|value| {
        value.len() != 32
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }) {
        return Err("invalid workload observation identity".into());
    }
    Ok(Some(
        json!({"run_id":run,"path":path,"concurrency":concurrency,"repeat":repeat,"snapshot":snapshot}),
    ))
}

fn observe(
    trial: Option<&serde_json::Value>,
    event: &str,
    reply: &str,
    result: Option<&serde_json::Value>,
    capture: impl FnOnce() -> Result<serde_json::Value, Failure>,
) -> Result<(), Failure> {
    use std::io::{BufRead, Read, Write};
    let Some(trial) = trial else { return Ok(()) };
    let mut record = json!({"v":1,"event":event,"trial":trial,"observation":capture()?});
    if let Some(result) = result {
        record["result"] = result.clone();
    }
    let line = format!("CROWDB_DATASET_OBSERVATION {record}\n");
    if line.len() > 1024 * 1024 {
        return Err("workload observation exceeds the line limit".into());
    }
    std::io::stdout().write_all(line.as_bytes())?;
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin()
        .lock()
        .take(1024 * 1024 + 1)
        .read_line(&mut line)?;
    if line.len() > 1024 * 1024 || !line.ends_with('\n') {
        return Err("missing or oversized workload observation reply".into());
    }
    if serde_json::from_str::<serde_json::Value>(&line)? != json!({"v":1,"event":reply,"trial":trial}) {
        return Err("mismatched workload observation reply".into());
    }
    Ok(())
}

fn observation_time() -> Result<String, Failure> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos()
        .to_string())
}

fn local_observation(runtime: &ClientRuntime) -> Result<serde_json::Value, Failure> {
    let started = observation_time()?;
    let (topology, unavailable) = match local_topology(runtime) {
        Ok(topology) => (topology, json!({})),
        Err(error) => (serde_json::Value::Null, json!({"topology":error.to_string()})),
    };
    let resource = runtime.resources().status();
    let resources = json!({"closed":resource.closed,"requests":resource.requests,
        "producer_bytes":resource.producer_bytes,"producer_bytes_high_water":resource.producer_bytes_high_water,
        "held_bytes":resource.held_bytes,"held_bytes_high_water":resource.held_bytes_high_water,
        "compatibility_copy_bytes":resource.compatibility_copy_bytes});
    Ok(
        json!({"started_unix_ns":started,"finished_unix_ns":observation_time()?,
        "topology":topology,"resources":resources,"unavailable":unavailable}),
    )
}

fn local_topology(runtime: &ClientRuntime) -> Result<serde_json::Value, Failure> {
    let view = runtime.topology()?;
    let metadata: serde_json::Value = serde_json::from_slice(&view.metadata_transport_json)?;
    let data: Option<serde_json::Value> = view
        .data_transport_json
        .as_deref()
        .map(serde_json::from_slice)
        .transpose()?;
    if !metadata.is_object() || data.as_ref().is_some_and(|value| !value.is_object()) {
        return Err("native transport status is not an object".into());
    }
    Ok(json!({"metadata":metadata,"data":data}))
}
