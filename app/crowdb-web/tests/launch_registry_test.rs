// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[test]
fn docker_web_refuses_launch_policy() {
    let state = crowdb_web::AppState::default().with_managed_ui("/tmp/ui".into());
    assert!(state
        .with_launch_registry("/tmp/unused-registry.toml".into())
        .is_err());
}
