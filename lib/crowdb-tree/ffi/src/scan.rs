// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::error::CtError;
use crate::sys;
use crate::tree::Crowdbtree;
use bytes::Bytes;

mod options;
pub(crate) mod owner;
pub use options::ScanOptions;

/// A scan result entry. `key` and `value` are zero-copy `Bytes` slices
/// into the packed result buffer, not per-entry `Vec<u8>` copies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanEntry {
    pub key: Bytes,
    pub slot: u64,
    pub value: Bytes,
    pub tombstone: bool,
}

/// Traversal order for the asynchronous packed scan surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanDirection {
    #[default]
    Forward,
    Reverse,
}

/// A snapshot-view entry (includes tombstones).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewEntry {
    pub key: Vec<u8>,
    pub slot: u64,
    pub tombstone: bool,
    pub value: Vec<u8>,
}

/// Decode entries as slices retaining the native packed allocation owner.
pub(crate) fn decode_scan_bytes(packed: Bytes, count: usize) -> Result<Vec<ScanEntry>, CtError> {
    let bytes: &[u8] = packed.as_ref();
    let mut out = Vec::with_capacity(count);
    let mut pos = 0usize;
    let rd_u32 = |b: &[u8], p: usize| -> u32 { u32::from_le_bytes([b[p], b[p + 1], b[p + 2], b[p + 3]]) };
    let rd_u64 = |b: &[u8], p: usize| -> u64 {
        let mut a = [0u8; 8];
        a.copy_from_slice(&b[p..p + 8]);
        u64::from_le_bytes(a)
    };
    for _ in 0..count {
        if pos + 4 > bytes.len() {
            return Err(CtError::Corruption);
        }
        let klen = rd_u32(bytes, pos) as usize;
        pos += 4;
        if pos + klen + 13 > bytes.len() {
            return Err(CtError::Corruption);
        }
        let key = packed.slice(pos..pos + klen);
        pos += klen;
        let slot = rd_u64(bytes, pos);
        pos += 8;
        let tombstone = bytes[pos] != 0;
        pos += 1;
        let vlen = rd_u32(bytes, pos) as usize;
        pos += 4;
        if pos + vlen > bytes.len() {
            return Err(CtError::Corruption);
        }
        let value = packed.slice(pos..pos + vlen);
        pos += vlen;
        out.push(ScanEntry {
            key,
            slot,
            value,
            tombstone,
        });
    }
    Ok(out)
}

impl Crowdbtree {
    /// Returns the greatest live key at or before the supplied upper bound.
    pub fn seek_reverse(
        &self,
        start_key: &[u8],
        start_inclusive: bool,
        begin_key: &[u8],
    ) -> Result<Option<ScanEntry>, CtError> {
        let (entries, _) = self.scan_reverse(Some(start_key), start_inclusive, begin_key, 1, 0)?;
        Ok(entries.into_iter().next())
    }

    /// Scans in descending key order under the supplied count and byte limits.
    pub fn scan_reverse(
        &self,
        start_key: Option<&[u8]>,
        start_inclusive: bool,
        begin_key: &[u8],
        limit: usize,
        byte_budget: usize,
    ) -> Result<(Vec<ScanEntry>, bool), CtError> {
        let start = start_key.unwrap_or_default();
        owner::scan(
            self.as_ptr(),
            &sys::ct_scan_options_v2 {
                struct_size: std::mem::size_of::<sys::ct_scan_options_v2>(),
                prefix: std::ptr::null(),
                prefix_len: 0,
                start: start.as_ptr(),
                start_len: start.len(),
                end: std::ptr::null(),
                end_len: 0,
                begin: begin_key.as_ptr(),
                begin_len: begin_key.len(),
                limit,
                byte_budget,
                deadline_ms: 0,
                reverse: 1,
                keys_only: 0,
                include_tombstones: 0,
                has_start_bound: i32::from(start_key.is_some()),
                start_inclusive: i32::from(start_inclusive),
                strict_byte_budget: 0,
            },
        )
    }

    /// Range scan over `prefix` (empty = whole keyspace).
    /// When `include_tombstones` is true, tombstone entries are included.
    /// `start_after` (empty = start from beginning) is an exclusive lower
    /// bound: only keys strictly greater than `start_after` are returned.
    /// `end_key` (empty = unbounded) is an exclusive upper bound: only keys
    /// strictly less than `end_key` are returned. When `keys_only` is true,
    /// values are not materialized (no overflow-chain assembly): entries
    /// carry empty values and the byte budget accounts for key bytes only.
    /// A zero `limit` removes the total count limit across internal work windows.
    /// Results still obey the byte/deadline limits and allocation budgets. Resume
    /// truncated output from the last returned key; use `scan_from` for an empty key.
    #[allow(clippy::too_many_arguments)]
    pub fn scan(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: usize,
        byte_budget: usize,
        keys_only: bool,
        deadline_ms: u64,
        include_tombstones: bool,
    ) -> Result<(Vec<ScanEntry>, bool), CtError> {
        self.scan_bound(
            prefix,
            start_after,
            !start_after.is_empty(),
            false,
            end_key,
            limit,
            byte_budget,
            keys_only,
            deadline_ms,
            include_tombstones,
            false,
        )
    }

    /// Range scan with an explicitly inclusive or exclusive lower bound.
    #[allow(clippy::too_many_arguments)]
    pub fn scan_from(
        &self,
        prefix: &[u8],
        start_key: &[u8],
        start_inclusive: bool,
        end_key: &[u8],
        limit: usize,
        byte_budget: usize,
        keys_only: bool,
        deadline_ms: u64,
        include_tombstones: bool,
    ) -> Result<(Vec<ScanEntry>, bool), CtError> {
        self.scan_bound(
            prefix,
            start_key,
            true,
            start_inclusive,
            end_key,
            limit,
            byte_budget,
            keys_only,
            deadline_ms,
            include_tombstones,
            false,
        )
    }

    /// Reads a forward window without materializing values beyond the byte budget.
    /// A 50 ms native deadline also bounds traversal through non-emitted entries.
    /// Empty output with `truncated` means no progress within the budget.
    /// # Errors
    /// Rejects a zero byte budget or propagates storage failures.
    pub fn scan_from_bounded(
        &self,
        start: &[u8],
        inclusive: bool,
        end: &[u8],
        limit: usize,
        byte_budget: usize,
    ) -> Result<(Vec<ScanEntry>, bool), CtError> {
        let deadline_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .saturating_add(50);
        let deadline_ms = u64::try_from(deadline_ms).unwrap_or(u64::MAX);
        self.scan_bound(
            b"",
            start,
            true,
            inclusive,
            end,
            limit,
            byte_budget,
            false,
            deadline_ms,
            false,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_bound(
        &self,
        prefix: &[u8],
        start_key: &[u8],
        has_start_bound: bool,
        start_inclusive: bool,
        end_key: &[u8],
        limit: usize,
        byte_budget: usize,
        keys_only: bool,
        deadline_ms: u64,
        include_tombstones: bool,
        strict_byte_budget: bool,
    ) -> Result<(Vec<ScanEntry>, bool), CtError> {
        owner::scan(
            self.as_ptr(),
            &sys::ct_scan_options_v2 {
                struct_size: std::mem::size_of::<sys::ct_scan_options_v2>(),
                prefix: prefix.as_ptr(),
                prefix_len: prefix.len(),
                start: start_key.as_ptr(),
                start_len: start_key.len(),
                end: end_key.as_ptr(),
                end_len: end_key.len(),
                begin: std::ptr::null(),
                begin_len: 0,
                limit,
                byte_budget,
                deadline_ms,
                reverse: 0,
                keys_only: i32::from(keys_only),
                include_tombstones: i32::from(include_tombstones),
                has_start_bound: i32::from(has_start_bound),
                start_inclusive: i32::from(start_inclusive),
                strict_byte_budget: i32::from(strict_byte_budget),
            },
        )
    }
}
