// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared stale-write and owner self-fence timing defaults.

pub const DEFAULT_MAX_WRITE_REQUEST_AGE_MS: u64 = 30_000;
pub const DEFAULT_MAX_CLOCK_SKEW_MS: u64 = 1_000;
pub const DEFAULT_SELF_FENCE_MARGIN_MS: u64 = 1_000;
