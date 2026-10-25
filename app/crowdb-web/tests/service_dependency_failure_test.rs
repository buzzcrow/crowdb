// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::http::StatusCode;
use crowdb_console_shared::error::Error;

#[test]
fn dependency_wait_is_a_conflict_while_execution_failures_remain_errors() {
    let (status, body) =
        crowdb_web::error::map_config_err(Error::DependenciesNotReady("DiskDB is loading".into()));
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body.0.error, "Waiting: DiskDB is loading");
    let (status, _) = crowdb_web::error::map_config_err(Error::Config("operation was superseded".into()));
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}
