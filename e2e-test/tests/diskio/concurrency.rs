// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::io_round::Backend;
use super::pattern::null_disk_pattern;
use crowdb_diskio_client::{DiskId as DioDiskId, DiskIoRetCode, TestWireDiskioClient as DiskioClient};
use crowdb_rpc_ffi::RpcServer;
use std::sync::Arc;
use std::time::Instant;

const BENCH_THREADS: usize = 4;
const BENCH_CYCLES: usize = 25;
const BENCH_SIZE: usize = 4096;

/// Run a concurrent write/read benchmark: `BENCH_THREADS` tasks each
/// doing `BENCH_CYCLES` write+read cycles on different offsets of the
/// same disk. Reports throughput and verifies read data content under
/// concurrent load.
#[allow(clippy::cast_precision_loss)]
pub async fn run_concurrent_benchmark(
    dio_client: &Arc<DiskioClient>,
    server: &Arc<RpcServer>,
    conn: &crowdb_rpc_ffi::Connection,
    backend: Backend,
    disk_id: DioDiskId,
) {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(BENCH_THREADS);

    for tid in 0..BENCH_THREADS {
        let client = Arc::clone(dio_client);
        let server = Arc::clone(server);
        let conn = conn.clone();
        handles.push(tokio::spawn(async move {
            let mut ok = 0usize;
            let mut errors = 0usize;
            for i in 0..BENCH_CYCLES {
                let offset = u64::try_from((tid * BENCH_CYCLES + i) * BENCH_SIZE).unwrap();
                // Skip fill value 0x01: the diskio server validates that
                // data beginning with a frame magic (0x0101-0x0501) is a
                // real frame, so a uniform 0x01 buffer would be rejected.
                let fill = (tid + i + 2) % 256;
                let data = vec![u8::try_from(fill).unwrap(); BENCH_SIZE];

                let Ok(wf) = client.write(&server, &conn, disk_id, 0, offset, data.clone()) else {
                    errors += 1;
                    continue;
                };
                let Ok(wc) = DiskioClient::await_write_response(wf).await else {
                    errors += 1;
                    continue;
                };
                if wc != DiskIoRetCode::Success {
                    errors += 1;
                    continue;
                }

                let Ok(rf) = client.read(
                    &server,
                    &conn,
                    disk_id,
                    0,
                    offset,
                    u32::try_from(BENCH_SIZE).unwrap(),
                    offset,
                ) else {
                    errors += 1;
                    continue;
                };
                let Ok((rc, rd)) = DiskioClient::await_read_response(rf).await else {
                    errors += 1;
                    continue;
                };
                if rc != DiskIoRetCode::Success {
                    errors += 1;
                    continue;
                }
                let Some(rd) = rd else {
                    errors += 1;
                    continue;
                };
                if rd.len() != BENCH_SIZE {
                    errors += 1;
                    continue;
                }
                let phys_offset = offset;
                let content_ok = match backend {
                    Backend::Mem => rd == data,
                    Backend::Null => rd == null_disk_pattern(disk_id, phys_offset, BENCH_SIZE),
                };
                if !content_ok {
                    errors += 1;
                    continue;
                }
                ok += 1;
            }
            (ok, errors)
        }));
    }

    let mut total_ok = 0usize;
    let mut total_err = 0usize;
    for h in handles {
        let (ok, err) = h.await.expect("benchmark task panicked");
        total_ok += ok;
        total_err += err;
    }

    let elapsed = start.elapsed();
    let total_ops = total_ok + total_err;
    let secs = elapsed.as_secs_f64();
    let ops_per_sec = if secs > 0.0 { total_ops as f64 / secs } else { 0.0 };
    let data_bytes = u64::try_from(total_ok * BENCH_SIZE * 2).unwrap_or(u64::MAX);
    let data_mb = data_bytes as f64 / (1024.0 * 1024.0);
    let mb_per_sec = if secs > 0.0 { data_mb / secs } else { 0.0 };

    eprintln!(
        "  {backend} benchmark: {BENCH_THREADS} threads × {BENCH_CYCLES} cycles, {BENCH_SIZE}B — {total_ops} ops in {elapsed:.2?} ({ops_per_sec:.0} ops/s, {mb_per_sec:.1} MB/s, {total_ok} ok, {total_err} errors)"
    );
    assert_eq!(total_err, 0, "{backend}: benchmark should have 0 errors");
    assert_eq!(
        total_ok,
        BENCH_THREADS * BENCH_CYCLES,
        "{backend}: all ops should succeed"
    );
}

// IO sizes to test: zero, small, middle, large, max (2 MB).
pub const IO_SIZES: &[(usize, &str)] = &[
    (0, "zero"),
    (100, "small"),
    (4096, "middle"),
    (1024 * 1024, "large"),   // 1 MB
    (2 * 1024 * 1024, "max"), // 2 MB (default max block size)
];
