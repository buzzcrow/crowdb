// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

pub(super) fn parse_limit(raw: &str) -> Result<Option<u64>, String> {
    if raw.trim() == "max" {
        return Ok(None);
    }
    raw.trim()
        .parse()
        .map(Some)
        .map_err(|_| "invalid cgroup memory.max".into())
}
