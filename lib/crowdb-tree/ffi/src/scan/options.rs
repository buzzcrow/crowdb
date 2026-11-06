// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::ScanDirection;

/// A scan request with an explicit optional cursor and bounded internal work windows.
/// `None` starts without a cursor; `Some(Vec::new())` bounds the empty key.
/// Resume truncated output with the last returned key and `start_inclusive=false`.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    pub prefix: Vec<u8>,
    pub start: Option<Vec<u8>>,
    pub start_inclusive: bool,
    /// Exclusive forward upper bound; empty means unbounded.
    pub end: Vec<u8>,
    /// Inclusive reverse lower bound; empty includes the empty key.
    pub begin: Vec<u8>,
    pub direction: ScanDirection,
    /// Total result count limit; zero means no count limit across internal windows.
    /// Result allocation remains subject to the Tree and host resource budgets.
    pub limit: usize,
    /// Total key/value byte budget; zero disables this result byte limit.
    pub byte_budget: usize,
    pub keys_only: bool,
    pub include_tombstones: bool,
    /// Absolute Unix time in milliseconds; zero disables the deadline.
    pub deadline_ms: u64,
    /// Stops before an oversized first record, returning empty, truncated output.
    pub strict_byte_budget: bool,
}
