// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/route_retirement.rs"]
mod route_retirement;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use crowdb_rpc_ffi::OwnedClientRoute;
use route_retirement::{route, TestCleanup, TestReleaseGuard};

#[test]
fn normal_process_exit_drains_queued_owners_before_native_statics() {
    const CHILD: &str = "CROWDB_ROUTE_EXIT_CHILD";
    if let Ok(mode) = std::env::var(CHILD) {
        let (released, _receiver) = mpsc::channel();
        let route = route(TestCleanup::ExitDelay, released);
        if mode == "live" {
            std::mem::forget(route);
            std::process::exit(0);
        }
        drop(route);
        if mode == "logging" {
            crowdb_rpc_ffi::shutdown_logging();
        }
        return;
    }
    for mode in ["normal", "logging", "live"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "normal_process_exit_drains_queued_owners_before_native_statics",
                "--nocapture",
            ])
            .env(CHILD, mode)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if mode == "live" {
            assert!(!output.status.success(), "{stdout}\n{stderr}");
            assert!(
                stderr.contains("live RPC route owners remain at native shutdown"),
                "{stdout}\n{stderr}"
            );
            continue;
        }
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(
            stderr.contains("ROUTE_RETIREMENT_EXIT_DRAINED"),
            "{stdout}\n{stderr}"
        );
        assert!(!stderr.contains("LOG ERROR"), "{stdout}\n{stderr}");
    }
}

#[test]
fn request_clones_keep_one_owner_bundle_until_the_last_release() {
    let (released_tx, released_rx) = mpsc::channel();
    let route = route(TestCleanup::None, released_tx);
    let clones = vec![route.clone(); 256];
    let handles = route.raw_handles();
    assert!(clones.iter().all(|route| route.raw_handles() == handles));
    drop(clones);
    assert!(released_rx.try_recv().is_err());
    assert!(route.is_open());
    drop(route);
    released_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(OwnedClientRoute::drain_retired_for_tests(Duration::from_secs(3)));
    assert!(released_rx.try_recv().is_err());
}

#[test]
fn cascading_retirement_drains_and_a_stalled_destructor_keeps_backlog_visible() {
    let (released_tx, released_rx) = mpsc::channel();
    let (entered_tx, entered_rx) = mpsc::channel();
    let release = Arc::new(AtomicBool::new(false));
    let guard = TestReleaseGuard(Arc::clone(&release));
    let child = route(
        TestCleanup::Block {
            entered: entered_tx,
            release: Arc::clone(&release),
        },
        released_tx.clone(),
    );
    let parent = route(TestCleanup::Cascade(Some(child)), released_tx.clone());
    drop(parent);
    entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(OwnedClientRoute::pending_retired_for_tests() >= 1);
    assert!(!OwnedClientRoute::drain_retired_for_tests(Duration::from_millis(
        10
    )));
    let tail = route(TestCleanup::None, released_tx);
    let (queued_tx, queued_rx) = mpsc::channel();
    let producer = std::thread::spawn(move || {
        drop(tail);
        queued_tx.send(()).unwrap();
    });
    queued_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(OwnedClientRoute::pending_retired_for_tests() >= 2);
    release.store(true, Ordering::Release);
    drop(guard);
    producer.join().unwrap();
    assert!(OwnedClientRoute::drain_retired_for_tests(Duration::from_secs(3)));
    for _ in 0..3 {
        released_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    }
    assert!(released_rx.try_recv().is_err());
}
