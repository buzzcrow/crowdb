// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_client::ClientConfig;

#[test]
fn default_retry_budget_covers_the_total_operation_deadline() {
    let config = ClientConfig::default();
    let retry_window = config
        .retry_backoff
        .checked_mul(config.max_attempts.saturating_sub(1))
        .unwrap();

    assert!(retry_window >= config.operation_timeout);
    config.validate().unwrap();
}
