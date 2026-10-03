// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::Operation;
use crate::state::AppState;
use crowdb_console_shared::ConsoleConfig;

#[tokio::test]
async fn reset_waits_for_accepted_work_and_blocks_new_deployment_until_cleanup_finishes() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("reset-deployment-fence");
    let state = AppState::with_runtime_root(ConsoleConfig::default(), root.path().to_owned());
    let accepted =
        Operation::claim(&state, vec!["node/1".into()]).unwrap_or_else(|(_, body)| panic!("{}", body.error));
    let waiting = tokio::spawn({
        let state = state.clone();
        async move { Operation::reset(&state).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !state.service_operations.load().contains("cluster/reset") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !waiting.is_finished(),
        "accepted deployment must finish before cleanup"
    );
    assert!(Operation::claim(&state, vec!["node/2".into()]).is_err());
    drop(accepted);
    let resetting = waiting
        .await
        .unwrap()
        .unwrap_or_else(|(_, body)| panic!("{}", body.error));
    assert!(Operation::claim(&state, vec!["listener/19910".into()]).is_err());
    let cleanup = Operation::claim_cleanup(&state, vec!["node/1".into()])
        .unwrap_or_else(|(_, body)| panic!("{}", body.error));
    drop(cleanup);
    drop(resetting);
    assert!(Operation::claim(&state, vec!["node/2".into()]).is_ok());
}
