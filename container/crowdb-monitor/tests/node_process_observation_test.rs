// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "../src/node/durable.rs"]
mod durable;
#[path = "../src/node/runtime/observation.rs"]
mod observation;

use crowdb_monitor::LogProfile;
pub use crowdb_monitor::ProcessManager;
use crowdb_protocol::mgmt::{node::NodeBinding, SystemBootstrapIdentity};
use std::path::PathBuf;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct TestRoot(PathBuf);
impl TestRoot {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("node-process-observation-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(path.join("log")).unwrap();
        Self(path)
    }
    fn bind(&self) {
        durable::write(
            &self.0.join("node-binding.json"),
            &NodeBinding {
                node_id: 7,
                bootstrap: SystemBootstrapIdentity {
                    cluster_id: "owned-cluster".into(),
                    operation_id: "operation".into(),
                    configuration_digest: "digest".into(),
                },
                // Observation must work with no reachable management authority.
                management_seeds: vec!["http://127.0.0.1:1".into()],
            },
        )
        .unwrap();
    }
}
impl Drop for TestRoot {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

#[tokio::test]
async fn bound_namespace_is_observable_without_live_group0() {
    let root = TestRoot::new();
    root.bind();
    let mut manager = ProcessManager::new(
        root.0.join("log"),
        LogProfile {
            max_file_bytes: 1024 * 1024,
            max_files: 2,
            mirror_warnings_to_stderr: false,
        },
    )
    .await
    .unwrap();
    let observed = observation::processes(&root.0, &mut manager, "owned-cluster", 7).unwrap();
    assert_eq!(observed["node_id"], 7);
    assert_eq!(observed["cluster_id"], "owned-cluster");
    assert_eq!(observed["processes"], serde_json::json!({}));
}

#[tokio::test]
async fn unbound_foreign_and_invalid_binding_reject_process_observation() {
    let root = TestRoot::new();
    let mut manager = ProcessManager::new(
        root.0.join("log"),
        LogProfile {
            max_file_bytes: 1024 * 1024,
            max_files: 2,
            mirror_warnings_to_stderr: false,
        },
    )
    .await
    .unwrap();
    assert!(observation::processes(&root.0, &mut manager, "owned-cluster", 7).is_err());
    root.bind();
    for (cluster, node) in [("foreign-cluster", 7), ("owned-cluster", 8)] {
        assert!(observation::processes(&root.0, &mut manager, cluster, node).is_err());
    }
    let binding = root.0.join("node-binding.json");
    std::fs::remove_file(&binding).unwrap();
    let foreign = root.0.join("foreign.json");
    durable::write(&foreign, &serde_json::json!({})).unwrap();
    std::os::unix::fs::symlink(&foreign, &binding).unwrap();
    assert!(observation::processes(&root.0, &mut manager, "owned-cluster", 7).is_err());
}
