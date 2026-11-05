// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::Future;

// A failed load must not inject a second full dataset's worth of reads.
pub(super) async fn after_load<F>(errors: &mut Vec<String>, verification: F) -> bool
where
    F: Future<Output = Result<(), String>>,
{
    if !errors.is_empty() {
        return false;
    }
    if let Err(error) = verification.await {
        errors.push(error);
    }
    true
}
