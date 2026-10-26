// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{
    collections::BTreeSet,
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use crowdb_chunk_kv_client::{
    BatchItem, ChunkKvClient, ChunkKvRpcTransport, ChunkKvRpcTransportConfig, ClientConfig,
    Group0ChunkKvRangeCatalogSource,
};
use crowdb_kv_client::{ClientConfig as ControlConfig, CrowdbKvClient};
use crowdb_protocol::chunk_kv::PointOperation;
use crowdb_rpc_ffi::RdmaConfig;
use futures::{stream::FuturesUnordered, StreamExt};
use serde_json::{json, Value};

#[path = "routed_write_batch/rows.rs"]
mod rows;
#[path = "routed_write_batch/transport.rs"]
mod transport;
use rows::{batch_result, point_result, TestOutcomes, TestRow, TestRowError, MAX_BYTES, MAX_ROWS};
pub use rows::{TestProfile, TestRows};
use transport::{tcp_delta, TestCountedCatalog, TestCountedTransport, TestProcessSample};

pub type TestFailure = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, Debug)]
pub enum TestMode {
    Point,
    Batch,
}
impl TestMode {
    pub fn key(self) -> char {
        match self {
            Self::Point => 'p',
            Self::Batch => 'b',
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Point => "point_sliding_16",
            Self::Batch => "routed_batch",
        }
    }
}

pub struct TestTrial {
    pub elapsed_ns: u64,
    pub valid: bool,
    pub report: Value,
}

pub struct TestRoutedBatch {
    client: Arc<ChunkKvClient>,
    transport: Arc<TestCountedTransport>,
    catalog: Arc<TestCountedCatalog>,
    partition_group_concurrency: usize,
}

impl TestRoutedBatch {
    pub async fn connect() -> Result<Self, TestFailure> {
        let seeds: Vec<_> = std::env::var("CROWDB_MANAGEMENT_SEEDS")?
            .split(',')
            .map(str::trim)
            .filter(|seed| !seed.is_empty())
            .map(str::to_owned)
            .collect();
        if seeds.is_empty() {
            return Err("measurement requires explicit management seeds".into());
        }
        let control = Arc::new(CrowdbKvClient::new(ControlConfig::new(seeds)));
        let config = ClientConfig::default();
        let partition_group_concurrency = config.max_inflight_partition_groups;
        let native = Arc::new(ChunkKvRpcTransport::with_config(
            config.max_owner_connections,
            1,
            2,
            &ChunkKvRpcTransportConfig {
                metadata_rdma: Some(RdmaConfig {
                    enabled: false,
                    ..RdmaConfig::default()
                }),
                aggregate_rdma: Some(RdmaConfig {
                    enabled: false,
                    ..RdmaConfig::default()
                }),
            },
        )?);
        let transport = Arc::new(TestCountedTransport::new(native));
        let catalog = Arc::new(TestCountedCatalog::new(
            Group0ChunkKvRangeCatalogSource::from_shared(control),
        ));
        let client = Arc::new(ChunkKvClient::new(config, catalog.clone(), transport.clone())?);
        client.refresh_catalog().await?;
        Ok(Self {
            client,
            transport,
            catalog,
            partition_group_concurrency,
        })
    }

    fn native_status(&self) -> Result<Value, TestFailure> {
        Ok(serde_json::from_slice(
            &self.transport.native.transport_status_json()?,
        )?)
    }

    fn routes(&self, rows: &TestRows) -> Result<Value, TestFailure> {
        let catalog = self
            .client
            .cached_catalog()
            .ok_or("measurement catalog unavailable")?;
        let mut partitions = BTreeSet::new();
        let mut owners = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        for index in 0..rows.profile.rows() {
            let key = rows.key(index);
            let entry = catalog.route(&key).ok_or("measurement key not routed")?;
            partitions.insert(format!(
                "{:016x}{:016x}",
                entry.partition_id.high, entry.partition_id.low
            ));
            owners.insert(entry.owner.instance_id);
            endpoints.insert(entry.owner.rpc_endpoint.clone());
        }
        Ok(
            json!({"catalog_generation":catalog.generation(), "partitions":partitions,
            "owners":owners, "endpoints":endpoints}),
        )
    }

    pub async fn trial(
        &self,
        rows: TestRows,
        mode: TestMode,
        warmup: bool,
    ) -> Result<TestTrial, TestFailure> {
        let routes_before = self.routes(&rows)?;
        let native_before = self.native_status()?;
        let attempts_before = self.transport.counts();
        let catalog_loads = self.catalog.loads.load(Ordering::Relaxed);
        let process_before = TestProcessSample::capture()?;
        let start = Instant::now();
        let outcomes = match mode {
            TestMode::Point => self.point_rows(&rows).await,
            TestMode::Batch => self.batch_rows(&rows).await,
        };
        let elapsed_ns = u64::try_from(start.elapsed().as_nanos())?;
        let process_after = TestProcessSample::capture()?;
        let attempts = self.transport.delta(attempts_before);
        let tcp = tcp_delta(&native_before, &self.native_status()?);
        let frames = tcp["metadata"]["completed_rpc_frames_sent"].as_u64().unwrap()
            + tcp["aggregate"]["completed_rpc_frames_sent"].as_u64().unwrap();
        let calls = attempts["point_rpc_attempts"].as_u64().unwrap()
            + attempts["batch_rpc_attempts"].as_u64().unwrap();
        let setup = tcp["metadata"]["connection_setup_frames"].as_u64().unwrap()
            + tcp["aggregate"]["connection_setup_frames"].as_u64().unwrap();
        let frames_match = frames == calls + setup;
        let valid = outcomes.clean()
            && outcomes.applied == rows.profile.rows()
            && frames_match
            && (warmup || setup == 0);
        let report = json!({"profile":rows.profile.name(), "path":mode.name(), "elapsed_ns":elapsed_ns,
            "requested_rows":rows.profile.rows(), "outcomes":outcomes.json(), "transport_attempts":attempts,
            "tcp_protocol_counters":tcp,"protocol_frame_counter_matches_rpc_plus_setup":frames_match,
            "connection_setup_frames":setup,"warmup":warmup,
            "process_resources":process_before.delta(&process_after)?,
            "catalog_source_loads":self.catalog.loads.load(Ordering::Relaxed) - catalog_loads,
            "routes_before":routes_before, "routes_after":self.routes(&rows)?,
            "limits":{"pending_rows":MAX_ROWS,"pending_logical_bytes":MAX_BYTES,
                "additional_prepared_rows":1,"client_partition_group_concurrency":self.partition_group_concurrency}, "valid":valid});
        Ok(TestTrial {
            elapsed_ns,
            valid,
            report,
        })
    }

    async fn point_rows(&self, rows: &TestRows) -> TestOutcomes {
        let mut stats = TestOutcomes::default();
        let mut pending = FuturesUnordered::new();
        let mut bytes = 0;
        for index in 0..rows.profile.rows() {
            let row = rows.row(index);
            while pending.len() >= MAX_ROWS || bytes + row.size() > MAX_BYTES {
                let (row, result): (TestRow, _) =
                    pending.next().await.expect("bounded row fits an empty window");
                bytes -= row.size();
                stats.record(result, &row);
                if !stats.clean() {
                    break;
                }
            }
            if !stats.clean() {
                stats.not_attempted = rows.profile.rows() - index;
                break;
            }
            bytes += row.size();
            stats.admitted(pending.len() + 1, bytes);
            let client = self.client.clone();
            pending.push(async move {
                let result = client.put_if_absent(row.key.clone(), row.value.clone()).await;
                (row, point_result(result))
            });
        }
        while let Some((row, result)) = pending.next().await {
            stats.record(result, &row);
        }
        stats
    }

    async fn batch_rows(&self, rows: &TestRows) -> TestOutcomes {
        let mut stats = TestOutcomes::default();
        let mut index = 0;
        let mut prepared = None;
        while index < rows.profile.rows() {
            let mut window = Vec::new();
            let mut bytes = 0;
            while index < rows.profile.rows() {
                let row = prepared.take().unwrap_or_else(|| rows.row(index));
                if window.len() == MAX_ROWS || bytes + row.size() > MAX_BYTES {
                    prepared = Some(row);
                    break;
                }
                bytes += row.size();
                window.push(row);
                index += 1;
            }
            stats.admitted(window.len(), bytes);
            self.batch_window(&window, &mut stats).await;
            if !stats.clean() {
                stats.not_attempted = rows.profile.rows() - index;
                break;
            }
        }
        stats
    }

    async fn batch_window(&self, window: &[TestRow], stats: &mut TestOutcomes) {
        let items = window
            .iter()
            .map(|row| BatchItem {
                request_id: None,
                operation: PointOperation::PutIfAbsent {
                    key: row.key.clone(),
                    value: row.value.clone(),
                },
            })
            .collect();
        match self.client.batch_mutate(items).await {
            Ok(results) if results.len() == window.len() => {
                for (row, result) in window.iter().zip(results) {
                    stats.record(batch_result(result), row);
                }
            }
            Ok(results) => {
                for row in window {
                    stats.record(
                        Err(TestRowError::Malformed(format!(
                            "batch result count changed: expected={} actual={}",
                            window.len(),
                            results.len()
                        ))),
                        row,
                    );
                }
            }
            Err(error) => {
                for row in window {
                    stats.record(Err(TestRowError::Client(error.clone())), row);
                }
            }
        }
    }

    pub fn shutdown(&self) {
        self.transport.native.shutdown();
    }
}
