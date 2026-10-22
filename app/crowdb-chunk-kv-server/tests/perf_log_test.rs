// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_server::metrics::start_stream_perf_log;
use crowdb_test_harness::test_dirs::tempdir_in_test_data;

#[tokio::test]
async fn service_perf_log_flushes_registered_stream_histograms_on_shutdown() {
    let dir = tempdir_in_test_data("stream-perf-log");
    let path = dir.path().to_str().unwrap();
    assert!(start_stream_perf_log(path, 0, 10, 2).unwrap().is_none());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    let mut runner = start_stream_perf_log(path, 1, 10, 2).unwrap().unwrap();
    assert!(runner
        .registry()
        .lock()
        .unwrap()
        .snapshot_named("stream.append.cursor.lh", 1.0)
        .is_some());
    runner
        .registry()
        .lock()
        .unwrap()
        .register_histogram("stream.append.cursor.lh")
        .observe(2_000_000);
    runner.stop().await;
    let entry = std::fs::read_dir(dir.path()).unwrap().next().unwrap().unwrap();
    let log = std::fs::read_to_string(entry.path()).unwrap();
    assert!(
        log.contains("stream.append.cursor.lh"),
        "missing stream phase: {log}"
    );
    assert!(log.contains("p50(us)"));
    assert!(log.contains("p99(us)"));
}
