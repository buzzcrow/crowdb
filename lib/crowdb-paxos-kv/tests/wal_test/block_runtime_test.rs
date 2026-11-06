// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Real block-backed WAL I/O must leave a single Tokio worker responsive.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Barrier,
};
use std::time::Duration;

use crowdb_paxos_kv::paxos::roles::PxBallot;
use crowdb_paxos_kv::wal::{BlockDevice, IoBackend, OpenOptions, WALRecord, WalConfig, WalEngine};

async fn held_wal_io(sync: bool, expected_progress: bool) {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("block-runtime-held");
    let device = BlockDevice::ssd();
    let backend = Arc::new(IoBackend::BlockDevice(device.clone()));
    let wal = WalEngine::create(
        Arc::clone(&backend),
        WalConfig {
            wal_disks: vec![directory.path().into()],
            ..Default::default()
        },
        1,
    )
    .await
    .unwrap();
    // Finish initial segment creation before installing the one-operation gate.
    wal.append(&WALRecord::from_promised(1, 1, 1, PxBallot::new(0, 1)))
        .await
        .unwrap();
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    device.set_io_gate_for_tests(sync, Arc::clone(&entered), Arc::clone(&release));
    let acknowledged = Arc::new(AtomicBool::new(false));
    let controller_acknowledged = Arc::clone(&acknowledged);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (progress_tx, progress_rx) = mpsc::channel();
    let controller = std::thread::spawn(move || {
        entered.wait();
        let _ = started_tx.send(());
        let progressed = progress_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        let premature_ack = controller_acknowledged.load(Ordering::Acquire);
        release.wait();
        (progressed, premature_ack)
    });
    let independent = tokio::spawn(async move {
        started_rx.await.unwrap();
        let _ = progress_tx.send(());
    });
    wal.append(&WALRecord::from_promised(1, 1, 2, PxBallot::new(0, 1)))
        .await
        .unwrap();
    acknowledged.store(true, Ordering::Release);
    independent.await.unwrap();
    let (progressed, premature_ack) = controller.join().unwrap();
    assert!(
        !premature_ack,
        "WAL acknowledged before the held durable I/O completed"
    );
    assert_eq!(
        progressed, expected_progress,
        "held BlockDevice WAL I/O blocked the only Tokio worker"
    );
    assert!(device.fdatasync_count() >= 2);
    wal.flush_all().await.unwrap();
    drop(wal);
    let recovered = crowdb_paxos_kv::wal::replay::replay_group(&backend, &[directory.path().into()], 1)
        .await
        .unwrap();
    assert_eq!(
        recovered
            .records
            .iter()
            .map(|record| record.slot)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn single_worker_progresses_while_block_wal_write_is_held() {
    held_wal_io(false, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn single_worker_progresses_while_block_wal_sync_is_held() {
    held_wal_io(true, true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn current_thread_fallback_remains_synchronous_without_panicking() {
    held_wal_io(true, false).await;
}

async fn block_io_round_trip() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("block-runtime-reopen");
    let path = directory.path().join("block");
    let backend = IoBackend::BlockDevice(BlockDevice::ssd());
    let mut file = backend.open(&path, OpenOptions::create_rw()).await.unwrap();
    file.write_at(b"first", 7).await.unwrap();
    file.write_vectored_at(
        &[std::io::IoSlice::new(b"next"), std::io::IoSlice::new(b"last")],
        21,
    )
    .await
    .unwrap();
    file.fdatasync().await.unwrap();
    drop(file);
    let mut reopened = backend.open(&path, OpenOptions::read_only()).await.unwrap();
    let mut first = [0; 5];
    reopened.read_exact_at(&mut first, 7).await.unwrap();
    let mut next = [0; 8];
    reopened.read_exact_at(&mut next, 21).await.unwrap();
    assert_eq!(&first, b"first");
    assert_eq!(&next, b"nextlast");
    let mut short = [0; 8];
    assert_eq!(reopened.read_at(&mut short, 4093).await.unwrap(), 3);
    assert_eq!(&short[..3], &[0; 3]);
    let mut beyond = [0; 1];
    assert_eq!(
        reopened
            .read_exact_at(&mut beyond, 8192)
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    assert!(backend.open(&path, OpenOptions::create_new_rw()).await.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn current_thread_block_io_preserves_order_rmw_and_durable_reopen() {
    block_io_round_trip().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn single_worker_block_io_preserves_order_rmw_and_durable_reopen() {
    block_io_round_trip().await;
}
