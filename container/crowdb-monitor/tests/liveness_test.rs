use std::fs;
use std::path::PathBuf;
use std::process::Command;

use crowdb_monitor::{probe_liveness, LivenessServer};
use uuid::Uuid;

struct TestRunRoot(PathBuf);

impl TestRunRoot {
    fn new() -> Self {
        // macOS limits Unix-domain socket paths to a short fixed buffer. Keep
        // this test root under /tmp instead of the long per-user temp path.
        let base = if cfg!(target_os = "macos") {
            PathBuf::from("/tmp")
        } else {
            std::env::temp_dir()
        };
        let root = base.join(format!("cm-live-{}", Uuid::new_v4().simple()));
        fs::create_dir_all(&root).unwrap();
        Self(root)
    }
}

impl Drop for TestRunRoot {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_requires_responsive_monitor_not_status_file() {
    let root = TestRunRoot::new();
    assert!(probe_liveness(&root.0).await.is_err());
    let server = LivenessServer::start(&root.0).unwrap();
    assert!(LivenessServer::start(&root.0).is_err());
    assert!(probe_liveness(&root.0).await.is_ok());
    let path = root.0.clone();
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
    assert!(probe_liveness(&root.0).await.is_err());
}
