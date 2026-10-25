// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared write-admission ceilings and owner self-fence timing defaults.

pub const MAX_WRITE_REQUEST_AGE_MS: u64 = 30_000;
pub const MAX_WRITE_CLOCK_SKEW_MS: u64 = 1_000;
pub const DEFAULT_SELF_FENCE_MARGIN_MS: u64 = 1_000;
/// Extra delay before an expired liveness task reads an abandoned tail.
/// Request age bounds late admission. Ordered drains must separately wait
/// for already submitted backend IO before a final frame scan or reuse.
pub const DEFAULT_FINALIZER_SCANNER_MARGIN_MS: u64 = 1_000;
