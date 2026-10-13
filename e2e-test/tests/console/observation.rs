// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[tokio::test]
#[ignore = "requires the owned release KV container"]
async fn formal_server_appears_healthy_in_aggregate_snapshot() {
    let origin = crate::common::management::origin(&crate::common::management::addresses(1)[0]);
    let snapshot = crowdb_console_shared::topology::aggregate(std::slice::from_ref(&origin))
        .await
        .unwrap();
    assert_eq!(snapshot.servers.len(), 1);
    let server = &snapshot.servers[0];
    assert_eq!(server.mgmt_url, origin);
    assert!(server.error.is_none() && server.health.is_some(), "{snapshot:#?}");
}
