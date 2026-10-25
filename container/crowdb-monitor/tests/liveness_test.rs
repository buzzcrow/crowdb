use std::fs;
use std::process::Command;

use crowdb_monitor::{probe_liveness, LivenessServer};
#[path = "common/run_root.rs"]
mod run_root;
use run_root::TestRunRoot;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_requires_responsive_monitor_not_status_file() {
    let root = TestRunRoot::new("monitor-liveness");
    assert!(probe_liveness(root.path()).await.is_err());
    let server = LivenessServer::start(root.path()).unwrap();
    assert!(LivenessServer::start(root.path()).is_err());
    assert!(probe_liveness(root.path()).await.is_ok());
    let path = root.path().to_path_buf();
    let success = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"))
            .args(["liveness", "--run-root"])
            .arg(path)
            .output()
            .unwrap()
            .status
            .success()
    })
    .await
    .unwrap();
    assert!(success);
    drop(server);
    assert!(probe_liveness(root.path()).await.is_err());
}

#[tokio::test]
async fn liveness_reclaims_only_a_disconnected_socket() {
    use std::os::unix::fs::DirBuilderExt;
    let root = TestRunRoot::new("monitor-liveness");
    let directory = root.path().join("health");
    fs::DirBuilder::new().mode(0o700).create(&directory).unwrap();
    let path = directory.join("monitor.sock");
    let stale = std::os::unix::net::UnixListener::bind(&path).unwrap();
    drop(stale);
    let server = LivenessServer::start(root.path()).unwrap();
    assert!(probe_liveness(root.path()).await.is_ok());
    assert!(LivenessServer::start(root.path()).is_err());
    assert!(probe_liveness(root.path()).await.is_ok());
    drop(server);
    fs::write(&path, "operator file").unwrap();
    assert!(LivenessServer::start(root.path()).is_err());
    assert_eq!(fs::read_to_string(path).unwrap(), "operator file");
}
