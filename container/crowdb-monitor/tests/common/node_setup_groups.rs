// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::node_setup::TestAuthority;
use crowdb_protocol::{
    common::GroupValue,
    key::{KvGroupKey, TextKey},
};

/// A component fixture publication, without certifying a native group runtime.
pub async fn publish_group(authority: &TestAuthority, group_id: u64) {
    authority
        .kv
        .put(
            0,
            0,
            KvGroupKey {
                store_id: 0,
                group_id,
            }
            .to_path()
            .as_bytes(),
            &serde_json::to_vec(&GroupValue {
                store_id: 0,
                group_id,
            })
            .unwrap(),
            None,
        )
        .await
        .unwrap();
}
