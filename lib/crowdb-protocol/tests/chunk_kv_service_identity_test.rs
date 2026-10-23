// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::common::ChunkKvExtra;

#[test]
fn current_owner_registration_requires_its_process_and_load_fields() {
    let current = ChunkKvExtra {
        process_epoch: 1,
        node_id: Some(7),
        http_endpoint: Some("http://127.0.0.1:15101".into()),
        ..ChunkKvExtra::default()
    };
    let encoded = serde_json::to_vec(&current).unwrap();
    assert_eq!(serde_json::from_slice::<ChunkKvExtra>(&encoded).unwrap(), current);
    for field in ["process_epoch", "partition_loads"] {
        let mut partial = serde_json::to_value(&current).unwrap();
        partial.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<ChunkKvExtra>(partial).is_err(),
            "{field}"
        );
    }
}
