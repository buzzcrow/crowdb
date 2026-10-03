// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::common::ChunkKvExtra;

#[test]
fn old_owner_registrations_do_not_invent_management_identity() {
    let old = r#"{"capacity_bytes":1,"durable_bytes":0,"request_rate":0,"hosted":[]}"#;
    let decoded: ChunkKvExtra = serde_json::from_str(old).unwrap();
    assert!(decoded.node_id.is_none());
    assert!(decoded.http_endpoint.is_none());
    let current = ChunkKvExtra {
        node_id: Some(7),
        http_endpoint: Some("http://127.0.0.1:15101".into()),
        ..decoded
    };
    let encoded = serde_json::to_vec(&current).unwrap();
    assert_eq!(serde_json::from_slice::<ChunkKvExtra>(&encoded).unwrap(), current);
}
