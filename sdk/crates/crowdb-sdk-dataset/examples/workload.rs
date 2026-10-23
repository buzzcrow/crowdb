// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reproducible CPU HTTP workload; the external runner owns fixtures and affinity.

use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use crowdb_sdk_dataset::{
    wire::BatchEvent, BatchRequest, DatasetClient, FieldDefinition, FieldValue, Manifest, Publication,
    Sample, UploadRequest,
};
use md5::{Digest, Md5};
use serde_json::json;
use std::{collections::BTreeMap, path::Path, time::Instant};

type Failure = Box<dyn std::error::Error + Send + Sync>;
const SIZES: [usize; 8] = [0, 128, 513, 4096, 65_535, 262_144, 1_048_576, 4_194_321];

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Failure> {
    let client = DatasetClient::with_config(
        &std::env::var("CROWDB_DATASET_ENDPOINT")?,
        &std::env::var("CROWDB_DATASET_NAME")?,
        std::env::var("CROWDB_DATASET_READ_TOKEN").ok(),
        crowdb_sdk_core::HttpConfig {
            max_in_flight_bytes: 256 * 1024 * 1024,
            max_attempts: 1,
            ..Default::default()
        },
    )?;
    let root = std::env::var("CROWDB_DATASET_CONSUMER_ROOT")?;
    let root = Path::new(&root);
    std::fs::create_dir_all(root)?;
    let snapshot = match std::fs::read_to_string(root.join("snapshot")) {
        Ok(snapshot) => snapshot,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let snapshot = prepare(&client).await?;
            std::fs::write(root.join("snapshot"), &snapshot)?;
            snapshot
        }
        Err(error) => return Err(error.into()),
    };
    for concurrency in [1, 3] {
        trial(&client, &snapshot, concurrency, 2).await?;
        for repeat in 0..3 {
            let result = trial(&client, &snapshot, concurrency, 16).await?;
            let report = json!({"path":"http", "snapshot":snapshot, "sizes":SIZES,
                "concurrency":concurrency, "repeat":repeat, "result":result,
                "digest":digest_cost(), "load":"closed-loop", "warmup_rounds":2,
                "in_flight_budget_bytes":256 * 1024 * 1024, "http_max_attempts":1});
            std::fs::write(
                root.join(format!("http-{concurrency}-{repeat}.json")),
                serde_json::to_vec_pretty(&report)?,
            )?;
            println!("{report}");
        }
    }
    Ok(())
}

async fn prepare(client: &DatasetClient) -> Result<String, Failure> {
    client.create().await?;
    let mut samples = Vec::new();
    for (index, size) in SIZES.into_iter().enumerate() {
        let data = Bytes::from(vec![u8::try_from(index)?; size]);
        let value = if size <= 4096 {
            FieldValue::Inline {
                data: STANDARD.encode(&data),
            }
        } else {
            let id = uuid::Uuid::new_v4().simple().to_string();
            client
                .create_upload(&UploadRequest {
                    id: id.clone(),
                    length: u64::try_from(size)?,
                    part_size: 1024 * 1024,
                    md5: hex::encode(Md5::digest(&data)),
                })
                .await?;
            for (part, offset) in (0..size).step_by(1024 * 1024).enumerate() {
                client
                    .upload_part(
                        &id,
                        u32::try_from(part)?,
                        data.slice(offset..size.min(offset + 1024 * 1024)),
                    )
                    .await?;
            }
            client.complete_upload(&id).await?;
            FieldValue::Payload { upload: id }
        };
        samples.push(Sample {
            id: STANDARD.encode(format!("size-{index}")),
            fields: BTreeMap::from([("value".into(), value)]),
        });
    }
    let published = client
        .publish(&Publication {
            operation: uuid::Uuid::new_v4().simple().to_string(),
            parent: None,
            manifest: Manifest {
                schema: vec![FieldDefinition {
                    name: "value".into(),
                    required: true,
                }],
                samples,
            },
        })
        .await?;
    Ok(published.id)
}

async fn read(client: &DatasetClient, snapshot: &str, index: usize) -> Result<u64, Failure> {
    let start = Instant::now();
    let mut body = client
        .stream_batch(&BatchRequest {
            snapshot: snapshot.into(),
            sample_ids: vec![STANDARD.encode(format!("size-{index}"))],
            fields: vec!["value".into()],
        })
        .await?;
    let mut length = 0;
    while let Some(event) = body.next_event().await? {
        match event {
            BatchEvent::Header(header) => {
                if header.snapshot != snapshot || header.fields.len() != 1 {
                    return Err("unexpected snapshot/projection".into());
                }
            }
            BatchEvent::Data(data) => {
                if data.iter().any(|byte| usize::from(*byte) != index) {
                    return Err("payload mismatch".into());
                }
                length += data.len();
            }
            _ => {}
        }
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
