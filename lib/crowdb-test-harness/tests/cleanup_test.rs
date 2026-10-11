// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![cfg(target_os = "linux")]

//! Disposable cleanup exercises real process identities with fake children.

use crowdb_protocol::port::namespace::RuntimeNamespace;
use serde_json::{json, Value};
use std::process::{Child, Command};

struct TestChildren(Vec<Child>);

impl TestChildren {
    fn new() -> Self {
        Self(
            (0..3)
                .map(|_| Command::new("sleep").arg("60").spawn().unwrap())
                .collect(),
        )
    }
}

impl Drop for TestChildren {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn disposable_cleanup_reaps_matching_children_and_preserves_foreign_and_persistent_owners() {
    let mut namespace = RuntimeNamespace::ephemeral("cleanup-child-ownership").unwrap();
    let mut children = TestChildren::new();
    for child in &children.0 {
        namespace.record_process(child.id()).unwrap();
    }
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(namespace.root().join("namespace.json")).unwrap()).unwrap();
    let root = namespace.data_dir().join("isolated-cleanup-root");
    let disposable = root.join("ephemeral/owned");
    let persistent = root.join("persistent/console/N-1");
    for path in [&disposable, &persistent, &root.join("ports")] {
        std::fs::create_dir_all(path).unwrap();
    }
    let sentinel = persistent.join("operator-data");
    std::fs::write(&sentinel, b"preserve").unwrap();
    let mut owned = manifest.clone();
    owned["processes"] = json!([manifest["processes"][0], {
        "pid": children.0[1].id(), "start": "stale-process-identity"
    }]);
    std::fs::write(
        disposable.join("namespace.json"),
        serde_json::to_vec(&owned).unwrap(),
    )
    .unwrap();
    let mut durable = manifest.clone();
    durable["id"] = json!("durable-owner");
    durable["mode"] = json!("persistent");
    durable["processes"] = json!([manifest["processes"][2]]);
    std::fs::write(
        persistent.join("namespace.json"),
        serde_json::to_vec(&durable).unwrap(),
    )
    .unwrap();
    let claims = json!([
        {"mode":"ephemeral", "namespace_id": namespace.id(), "port": 7000},
        {"mode":"persistent", "namespace_id": namespace.id(), "port": 7001},
        {"mode":"persistent", "namespace_id": "durable-owner", "port": 7002}
    ]);
    let claims_path = root.join("ports/claims.json");
    std::fs::write(&claims_path, serde_json::to_vec(&claims).unwrap()).unwrap();
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tools/pixi-tasks/clean-env.sh");
    let result = Command::new("bash")
        .arg(script)
        .env("CROWDB_RUNTIME_ROOT", &root)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        children.0[0].try_wait().unwrap().is_some(),
        "recorded child survived cleanup"
    );
    assert!(
        children.0[1].try_wait().unwrap().is_none(),
        "stale identity killed a foreign child"
    );
    assert!(
        children.0[2].try_wait().unwrap().is_none(),
        "persistent child was killed"
    );
    assert!(!root.join("ephemeral").exists());
    assert_eq!(std::fs::read(sentinel).unwrap(), b"preserve");
    let retained: Value = serde_json::from_slice(&std::fs::read(claims_path).unwrap()).unwrap();
    assert_eq!(retained, json!([claims[2]]));
}
