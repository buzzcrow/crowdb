// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::mgmt::node::{NodeControl, NodeServiceAction, NodeServiceIntent};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub fn processes_once(root: &Path) -> tokio::task::JoinHandle<()> {
    processes_reply_once(root, false)
}

pub fn processes_with_kv_once(root: &Path) -> tokio::task::JoinHandle<()> {
    processes_reply_once(root, true)
}

fn processes_reply_once(root: &Path, kv_running: bool) -> tokio::task::JoinHandle<()> {
    std::fs::create_dir_all(root).unwrap();
    let listener = tokio::net::UnixListener::bind(root.join("node-control.sock")).unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.unwrap();
        assert!(matches!(
            serde_json::from_slice::<NodeControl>(&bytes).unwrap(),
            NodeControl::Processes { .. }
        ));
        let processes = if kv_running {
            serde_json::json!({"kv":42})
        } else {
            serde_json::json!({})
        };
        let reply = serde_json::json!({"ok":true,"value":{
            "processes":processes,"service_failures":{}}});
        stream
            .write_all(&serde_json::to_vec(&reply).unwrap())
            .await
            .unwrap();
    })
}

pub async fn newer_intent(
    root: &Path,
    authority: &super::node_setup::TestAuthority,
    action: NodeServiceAction,
) -> (String, NodeServiceIntent) {
    let path = root.join("node-setup.json");
    let mut journal: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let original = uuid::Uuid::new_v4().to_string();
    let step = journal["status"]["services"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|step| step["kind"] == "chunkdb")
        .unwrap();
    step["operation_id"] = serde_json::json!(original);
    std::fs::write(&path, serde_json::to_vec(&journal).unwrap()).unwrap();
    let newer = NodeServiceIntent {
        cluster_id: authority.binding.bootstrap.cluster_id.clone(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        node_id: 2,
        service_id: "chunkdb-2".into(),
        kind: "chunkdb".into(),
        action,
        configuration: "instance_id = 9".into(),
        environment: std::collections::BTreeMap::new(),
    };
    authority
        .kv
        .put(
            0,
            0,
            b"/deployment/services/2/chunkdb-2",
            &serde_json::to_vec(&newer).unwrap(),
            None,
        )
        .await
        .unwrap();
    (original, newer)
}
