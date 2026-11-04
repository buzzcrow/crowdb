// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod sdk;

use super::super::foreground::write_s3_with_delay;
use crate::{access_iceberg::frontend::TestFrontend, common::control};
use futures::FutureExt;
use sdk::TestSdkWorker;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(in crate::access_iceberg::gc) struct TestGcPressure {
    sdk: TestSdkWorker,
    writes: Option<tokio::task::JoinHandle<()>>,
    progress: Arc<AtomicUsize>,
    previous_active: Option<(u64, u64)>,
    deletion_intervals: Vec<(u64, u64)>,
    sdk_complete: bool,
}

impl TestGcPressure {
    pub(in crate::access_iceberg::gc) async fn prepare_before(
        deadline: tokio::time::Instant,
    ) -> Result<Self, String> {
        // Call before publishing an executable purge marker. This ordinary
        // frontend warms the real SDK while no seeded deletion can race it.
        let result = tokio::time::timeout_at(deadline, async {
            let server = TestFrontend::start(12).await;
            let mut sdk = TestSdkWorker::prepare(&server.origin)?;
            sdk.wait_ready().await?;
            // Restart after publishing the marker so its first scan sees the
            // backlog, without waiting for the production idle scan interval.
            server.stop().await;
            Ok::<_, String>(sdk)
        })
        .await;
        let sdk = match result
            .unwrap_or_else(|_| Err("SDK warmup exceeded the existing GC case deadline".into()))
        {
            Ok(sdk) => sdk,
            Err(error) => {
                let cleanup = std::panic::AssertUnwindSafe(control::node("stop", 12))
                    .catch_unwind()
                    .await;
                if cleanup.is_err() {
                    eprintln!("SDK warmup frontend cleanup also failed; retaining the original warmup error");
                }
                return Err(error);
            }
        };
        Ok(Self {
            sdk,
            writes: None,
            progress: Arc::new(AtomicUsize::new(0)),
            previous_active: None,
            deletion_intervals: Vec::new(),
            sdk_complete: false,
        })
    }

    pub(in crate::access_iceberg::gc) fn released(&self) -> bool {
        self.sdk.released()
    }

    pub(in crate::access_iceberg::gc) fn release(&mut self, seeds: Vec<String>) -> Result<(), String> {
        self.sdk.release()?;
        self.writes = Some(tokio::spawn(write_s3_with_delay(
            seeds,
            self.progress.clone(),
            Duration::from_millis(250),
        )));
        Ok(())
    }

    pub(in crate::access_iceberg::gc) async fn observe(
        &mut self,
        deleted: u64,
        gc_complete: bool,
    ) -> Result<(), String> {
        if !self.released() {
            return Err("foreground observation requires the deletion-boundary release".into());
        }
        if let Some(status) = self.sdk.try_wait()? {
            if !status.success() {
                return Err(format!("official SDK foreground failed: {status}"));
            }
            self.sdk_complete = true;
        }
        if gc_complete && self.writes.is_none() {
            self.sdk.stop()?;
        }
        if self
            .writes
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            self.writes
                .take()
                .unwrap()
                .await
                .map_err(|error| error.to_string())?;
            if self.progress.load(Ordering::Acquire) != 64 {
                return Err("foreground small writer did not finish 64 writes".into());
            }
        }
        if self.sdk_complete || self.writes.is_none() || self.progress.load(Ordering::Acquire) == 0 {
            self.previous_active = None;
            return Ok(());
        }
        let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()).unwrap();
        if let Some((previous, before)) = self.previous_active {
            if deleted > previous {
                self.deletion_intervals.push((before, now));
            }
        }
        self.previous_active = Some((deleted, now));
        Ok(())
    }

    pub(in crate::access_iceberg::gc) fn complete(&self) -> bool {
        self.released() && self.sdk_complete && self.writes.is_none()
    }

    pub(in crate::access_iceberg::gc) async fn stop(&mut self) {
        if let Err(error) = self.sdk.stop() {
            eprintln!("SDK cleanup also failed: {error}");
        }
        if let Some(writes) = self.writes.take() {
            writes.abort();
            let _ = writes.await;
        }
        if let Err(error) = self.sdk.terminate() {
            eprintln!("SDK cleanup also failed: {error}");
        }
        if let Err(error) = self.sdk.output() {
            eprintln!("SDK output: {error}");
        }
    }

    pub(in crate::access_iceberg::gc) fn finish(mut self) -> Result<(), String> {
        if !self.complete() {
            return Err("foreground workloads did not finish".into());
        }
        self.sdk.terminate()?;
        let stdout = self.sdk.output()?;
        if self.deletion_intervals.is_empty() {
            return Err("no actual deletion advanced while both foreground workloads were active".into());
        }
        let report = stdout
            .lines()
            .find_map(|line| line.strip_prefix("gc_foreground_official_sdk "))
            .ok_or("official SDK latency report is missing")?;
        let report: serde_json::Value = serde_json::from_str(report).map_err(|error| error.to_string())?;
        for operation in [
            "create_table",
            "commit",
            "load_table",
            "credentials",
            "read_metadata",
            "drop_table",
        ] {
            if report[operation]["count"]
                .as_u64()
                .map_or(true, |count| count < 48)
            {
                return Err(format!(
                    "official SDK did not complete its minimum foreground workload: {operation}"
                ));
            }
        }
        let overlapping = report
            .as_object()
            .ok_or("SDK report must be an object")?
            .values()
            .any(|value| {
                value["windows_unix_ns"].as_array().is_some_and(|windows| {
                    windows.iter().any(|window| {
                        let Some(completed) = window.get(1).and_then(serde_json::Value::as_u64) else {
                            return false;
                        };
                        self.deletion_intervals
                            .iter()
                            .any(|(begin, end)| (*begin..=*end).contains(&completed))
                    })
                })
            });
        if !overlapping {
            return Err(
                "no successful SDK request completed inside an observed active deletion interval".into(),
            );
        }
        println!(
            "gc_foreground_deletion_overlap {}",
            serde_json::json!({
                "deletion_intervals_unix_ns":self.deletion_intervals,"small_writes":64,
                "sdk_workers":4,"minimum_tables_per_worker":12,"inter_table_sleep_ms":500,
                "small_write_interval_ms":250,"sleeps_in_latency_samples":false,
                "sdk_stdout_artifact":self.sdk.stdout(),
                "scope":"continuous foreground through cleanup and small-writer completion; latency is not a fixed-load baseline",
            })
        );
        Ok(())
    }
}

impl Drop for TestGcPressure {
    fn drop(&mut self) {
        if let Some(writes) = self.writes.take() {
            writes.abort();
        }
    }
}
