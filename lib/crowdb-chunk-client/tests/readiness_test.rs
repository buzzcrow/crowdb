// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

use crowdb_chunk_client::{
    client::observe_readiness_phase_for_tests, metrics::ReadinessMetrics, ChunkIoClient, IoError,
};
use tokio::time::Instant;

#[path = "common/readiness_schedule.rs"]
mod schedule;

#[tokio::test(start_paused = true)]
async fn disk_headers_start_after_hardware_without_waiting_for_chunkdb() {
    let metrics = Arc::new(ReadinessMetrics::default());
    let writer =
        schedule::TestReadinessWriter::new(Duration::from_millis(100), schedule::TestHeaderReply::Ready);
    let start = Instant::now();
    let result = crowdb_chunk_client::ChunkIoClient::observe_readiness_dependencies_for_tests(
        start + Duration::from_millis(750),
        &metrics,
        writer.clone(),
        async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(schedule::test_disks(1))
        },
        async {
            tokio::time::sleep(Duration::from_millis(700)).await;
            Ok(())
        },
    )
    .await;
    assert!(
        result.is_ok(),
        "independent hardware/header and ChunkDB work must fit the original 750ms total: {result:?}"
    );
    assert_eq!(result.unwrap(), 1);
    assert_eq!(Instant::now() - start, Duration::from_millis(700));
    assert_eq!(writer.started.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.snapshot().diskio.errors, 0);
}

#[tokio::test(start_paused = true)]
async fn held_chunkdb_still_fails_after_a_successful_physical_header() {
    let metrics = Arc::new(ReadinessMetrics::default());
    let writer =
        schedule::TestReadinessWriter::new(Duration::from_millis(100), schedule::TestHeaderReply::Ready);
    let deadline = Instant::now() + Duration::from_millis(750);
    let result = ChunkIoClient::observe_readiness_dependencies_for_tests(
        deadline,
        &metrics,
        writer.clone(),
        async { Ok(schedule::test_disks(1)) },
        std::future::pending(),
    )
    .await;
    assert!(result.unwrap_err().to_string().contains("ChunkDB read exceeded"));
    assert_eq!(Instant::now(), deadline);
    assert_eq!(writer.started.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.snapshot().chunkdb.errors, 1);
    assert_eq!(metrics.snapshot().diskio.errors, 0);
}

#[tokio::test(start_paused = true)]
async fn rejected_chunkdb_still_fails_after_a_successful_physical_header() {
    let metrics = Arc::new(ReadinessMetrics::default());
    let writer =
        schedule::TestReadinessWriter::new(Duration::from_millis(100), schedule::TestHeaderReply::Ready);
    let result = ChunkIoClient::observe_readiness_dependencies_for_tests(
        Instant::now() + Duration::from_millis(750),
        &metrics,
        writer.clone(),
        async { Ok(schedule::test_disks(1)) },
        async { Err(IoError::ReadFailed("injected ChunkDB rejection".into())) },
    )
    .await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected ChunkDB rejection"));
    assert_eq!(writer.started.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.snapshot().chunkdb.errors, 1);
    assert_eq!(metrics.snapshot().diskio.errors, 0);
}

#[tokio::test(start_paused = true)]
async fn held_physical_header_still_owes_the_original_total_deadline() {
    let metrics = Arc::new(ReadinessMetrics::default());
    let writer = schedule::TestReadinessWriter::new(Duration::ZERO, schedule::TestHeaderReply::Held);
    let deadline = Instant::now() + Duration::from_millis(750);
    let result = ChunkIoClient::observe_readiness_dependencies_for_tests(
        deadline,
        &metrics,
        writer.clone(),
        async { Ok(schedule::test_disks(1)) },
        async { Ok(()) },
    )
    .await;
    assert!(result.unwrap_err().to_string().contains("DiskIO read exceeded"));
    assert_eq!(Instant::now(), deadline);
    assert_eq!(metrics.snapshot().diskio.errors, 1);
    assert_eq!(writer.active.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn rejected_or_short_physical_headers_cannot_pass_with_ready_chunkdb() {
    for (reply, message) in [
        (schedule::TestHeaderReply::Rejected, "injected physical rejection"),
        (schedule::TestHeaderReply::Short, "short disk header"),
    ] {
        let metrics = Arc::new(ReadinessMetrics::default());
        let writer = schedule::TestReadinessWriter::new(Duration::ZERO, reply);
        let result = ChunkIoClient::observe_readiness_dependencies_for_tests(
            Instant::now() + Duration::from_millis(750),
            &metrics,
            writer.clone(),
            async { Ok(schedule::test_disks(1)) },
            async { Ok(()) },
        )
        .await;
        assert!(result.unwrap_err().to_string().contains(message));
        assert_eq!(metrics.snapshot().diskio.errors, 1);
        assert_eq!(writer.started.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn failed_or_held_hardware_never_starts_a_physical_read() {
    for held in [false, true] {
        let metrics = Arc::new(ReadinessMetrics::default());
        let writer = schedule::TestReadinessWriter::new(Duration::ZERO, schedule::TestHeaderReply::Ready);
        let result = ChunkIoClient::observe_readiness_dependencies_for_tests(
            Instant::now() + Duration::from_millis(750),
            &metrics,
            writer.clone(),
            async {
                if held {
                    std::future::pending().await
                } else {
                    Err(IoError::Topology("injected hardware rejection".into()))
                }
            },
            async { Ok(()) },
        )
        .await;
        let message = result.unwrap_err().to_string();
        assert!(message.contains(if held {
            "hardware read exceeded"
        } else {
            "injected hardware rejection"
        }));
        assert_eq!(metrics.snapshot().hardware.errors, 1);
        assert_eq!(metrics.snapshot().diskio.observations, 0);
        assert_eq!(writer.started.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn empty_or_offline_hardware_does_not_claim_physical_readiness() {
    for count in [0, 1] {
        let metrics = Arc::new(ReadinessMetrics::default());
        let writer = schedule::TestReadinessWriter::new(Duration::ZERO, schedule::TestHeaderReply::Ready);
        let mut disks = schedule::test_disks(count);
        for disk in &mut disks {
            disk.value.status = crowdb_protocol::common::HwStatus::Offline as i32;
        }
        let result = ChunkIoClient::observe_readiness_dependencies_for_tests(
            Instant::now() + Duration::from_millis(750),
            &metrics,
            writer.clone(),
            async { Ok(disks) },
            async { Ok(()) },
        )
        .await;
        assert!(result.unwrap_err().to_string().contains("no online disks"));
        assert_eq!(writer.started.load(Ordering::SeqCst), 0);
        assert_eq!(metrics.snapshot().diskio.observations, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn physical_headers_keep_the_existing_eight_read_batch_bound() {
    let metrics = Arc::new(ReadinessMetrics::default());
    let writer =
        schedule::TestReadinessWriter::new(Duration::from_millis(100), schedule::TestHeaderReply::Ready);
    let start = Instant::now();
    let result = ChunkIoClient::observe_readiness_dependencies_for_tests(
        start + Duration::from_millis(750),
        &metrics,
        writer.clone(),
        async { Ok(schedule::test_disks(17)) },
        async { Ok(()) },
    )
    .await;
    assert_eq!(result.unwrap(), 17);
    assert_eq!(writer.started.load(Ordering::SeqCst), 17);
    assert_eq!(writer.maximum.load(Ordering::SeqCst), 8);
    assert_eq!(Instant::now() - start, Duration::from_millis(300));
    assert_eq!(metrics.snapshot().diskio.observations, 17);
}

#[tokio::test(start_paused = true)]
async fn cancelling_dependency_observation_aborts_physical_reads_and_both_phase_guards() {
    let metrics = Arc::new(ReadinessMetrics::default());
    let writer = schedule::TestReadinessWriter::new(Duration::ZERO, schedule::TestHeaderReply::Held);
    let worker = tokio::spawn({
        let metrics = metrics.clone();
        let writer = writer.clone();
        async move {
            ChunkIoClient::observe_readiness_dependencies_for_tests(
                Instant::now() + Duration::from_millis(750),
                &metrics,
                writer,
                async { Ok(schedule::test_disks(1)) },
                std::future::pending(),
            )
            .await
        }
    });
    writer.read_started.notified().await;
    assert_eq!(metrics.snapshot().chunkdb.inflight, 1);
    assert_eq!(metrics.snapshot().diskio.inflight, 1);
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    tokio::task::yield_now().await;
    assert_eq!(writer.active.load(Ordering::SeqCst), 0);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.chunkdb.inflight, 0);
    assert_eq!(snapshot.chunkdb.errors, 1);
    assert_eq!(snapshot.diskio.inflight, 0);
    assert_eq!(snapshot.diskio.errors, 1);
}

#[tokio::test(start_paused = true)]
async fn phase_deadlines_share_the_remaining_budget_and_identify_the_dependency() {
    let metrics = ReadinessMetrics::default();
    let deadline = Instant::now() + Duration::from_millis(750);
    tokio::time::advance(Duration::from_millis(600)).await;
    let result = observe_readiness_phase_for_tests(&metrics, deadline, async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(())
    })
    .await;
    assert!(result.unwrap_err().to_string().contains("DiskIO read exceeded"));
    assert_eq!(Instant::now(), deadline);
    let snapshot = metrics.snapshot().diskio;
    assert_eq!(snapshot.observations, 1);
    assert_eq!(snapshot.errors, 1);
    assert_eq!(snapshot.inflight, 0);
}

#[tokio::test]
async fn cancelled_probes_record_latency_and_error_without_leaking_inflight_work() {
    let metrics = Arc::new(ReadinessMetrics::default());
    let worker_metrics = Arc::clone(&metrics);
    let (started, observing) = tokio::sync::oneshot::channel();
    let worker = tokio::spawn(async move {
        observe_readiness_phase_for_tests(&worker_metrics, Instant::now() + Duration::from_secs(10), async {
            started.send(()).unwrap();
            std::future::pending().await
        })
        .await
    });
    observing.await.unwrap();
    assert_eq!(metrics.snapshot().diskio.inflight, 1);
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    let snapshot = metrics.snapshot().diskio;
    assert_eq!(snapshot.observations, 1);
    assert_eq!(snapshot.errors, 1);
    assert_eq!(snapshot.inflight, 0);
    observe_readiness_phase_for_tests(&metrics, Instant::now() + Duration::from_secs(1), async {
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(metrics.snapshot().diskio.observations, 2);
    assert_eq!(metrics.snapshot().diskio.errors, 1);
}
