// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_common::metrics::{MetricsRunner, MetricsSampler};
use crowdb_test_harness::test_dirs::tempdir_in_test_data;

struct TestSampler {
    samples: usize,
    ready: Arc<tokio::sync::Notify>,
}

impl MetricsSampler for TestSampler {
    fn sample(&mut self) {
        self.samples += 1;
        if self.samples >= 2 {
            self.ready.notify_one();
        }
    }

    fn flush(&mut self, writer: &mut dyn std::io::Write, _: &str) -> std::io::Result<()> {
        writeln!(writer, "resource_test samples={}", self.samples)
    }
}

#[tokio::test]
async fn sampling_runs_between_log_periods_and_shutdown_preserves_owned_window() {
    let dir = tempdir_in_test_data("metrics-resource-sampler");
    let file = crowdb_common::logging::open_metrics_log(dir.path(), "test", 10, 2).unwrap();
    let ready = Arc::new(tokio::sync::Notify::new());
    let mut runner = MetricsRunner::new(file, 60);
    runner.set_sampler(TestSampler {
        samples: 0,
        ready: ready.clone(),
    });
    runner.start();
    tokio::time::timeout(std::time::Duration::from_secs(5), ready.notified())
        .await
        .unwrap();
    runner.stop().await;
    let path = std::fs::read_dir(dir.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let log = std::fs::read_to_string(path).unwrap();
    let samples: usize = log
        .lines()
        .find_map(|line| line.strip_prefix("resource_test samples="))
        .unwrap()
        .parse()
        .unwrap();
    assert!(samples >= 2, "{log}");
    assert_eq!(log.matches("resource_test").count(), 1, "{log}");
}
