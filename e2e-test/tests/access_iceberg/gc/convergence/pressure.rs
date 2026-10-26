// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::foreground::{write_s3_with_delay, SDK_PRESSURE_SCRIPT};
use crowdb_access_iceberg::key::OperationId;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(in crate::access_iceberg::gc) struct TestGcPressure {
    sdk: std::process::Child,
    stdout: PathBuf,
    writes: Option<tokio::task::JoinHandle<()>>,
    progress: Arc<AtomicUsize>,
    previous_active: Option<(u64, u64)>,
    deletion_intervals: Vec<(u64, u64)>,
    sdk_complete: bool,
}

impl TestGcPressure {
    pub(in crate::access_iceberg::gc) fn start(origin: &str, seeds: Vec<String>) -> Result<Self, String> {
        let stdout = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").map_err(|error| error.to_string())?)
            .join(format!("gc-inline-sdk-{}.log", OperationId::random()));
        let output = std::fs::File::create(&stdout).map_err(|error| error.to_string())?;
        let sdk = std::process::Command::new("pixi")
            .args(["run", "-e", "iceberg-e2e", "python", "-c", SDK_PRESSURE_SCRIPT])
            .args([origin, "12", "0.5", "gc-convergence"])
            .stdout(output)
            .spawn()
            .map_err(|error| error.to_string())?;
        let progress = Arc::new(AtomicUsize::new(0));
        let writes = tokio::spawn(write_s3_with_delay(
            seeds,
            progress.clone(),
            Duration::from_millis(250),
        ));
        Ok(Self {
            sdk,
            stdout,
            writes: Some(writes),
            progress,
            previous_active: None,
            deletion_intervals: Vec::new(),
            sdk_complete: false,
        })
    }

    pub(in crate::access_iceberg::gc) async fn observe(&mut self, deleted: u64) -> Result<(), String> {
        if let Some(status) = self.sdk.try_wait().map_err(|error| error.to_string())? {
            if !status.success() {
                return Err(format!("official SDK foreground failed: {status}"));
            }
            self.sdk_complete = true;
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
        self.sdk_complete && self.writes.is_none()
    }

    pub(in crate::access_iceberg::gc) async fn stop(&mut self) {
        if let Some(writes) = self.writes.take() {
            writes.abort();
            let _ = writes.await;
        }
        if !self.sdk_complete {
            let _ = self.sdk.kill();
            let _ = self.sdk.wait();
        }
        match std::fs::read_to_string(&self.stdout) {
            Ok(output) => print!("{output}"),
            Err(error) => eprintln!("SDK stdout artifact {}: {error}", self.stdout.display()),
        }
    }

    pub(in crate::access_iceberg::gc) fn finish(self) -> Result<(), String> {
        if !self.complete() {
            return Err("foreground workloads did not finish".into());
        }
        let stdout = std::fs::read_to_string(&self.stdout).map_err(|error| error.to_string())?;
        print!("{stdout}");
        if self.deletion_intervals.is_empty() {
            return Err("no actual deletion advanced while both foreground workloads were active".into());
        }
        let report = stdout
            .lines()
            .find_map(|line| line.strip_prefix("gc_foreground_official_sdk "))
            .ok_or("official SDK latency report is missing")?;
        let report: serde_json::Value = serde_json::from_str(report).map_err(|error| error.to_string())?;
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
                "sdk_workers":4,"tables_per_worker":12,"inter_table_sleep_ms":500,
                "small_write_interval_ms":250,"sleeps_in_latency_samples":false,
                "sdk_stdout_artifact":self.stdout,
                "scope":"independent fixed load; latency is not directly comparable to the original three-table, 50-ms probe",
            })
        );
        Ok(())
    }
}
