// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use std::os::raw::c_int;

use crate::error::{check, take_buf, CtError};
use crate::sys;
use crate::tree::Crowdbtree;

/// A scan result entry. `key` and `value` are zero-copy `Bytes` slices
/// into the packed result buffer, not per-entry `Vec<u8>` copies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanEntry {
    pub key: Bytes,
    pub slot: u64,
    pub value: Bytes,
    pub tombstone: bool,
}

/// A snapshot-view entry (includes tombstones).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewEntry {
    pub key: Vec<u8>,
    pub slot: u64,
    pub tombstone: bool,
    pub value: Vec<u8>,
}

/// Unpack the C++ packed scan result buffer into `ScanEntry`s.
/// `bytes` is converted to a single `Bytes` once (zero-copy move of the
/// `Vec<u8>` allocation via `Bytes::from`), then each entry's `key` and
/// `value` is a `packed.slice(range)` — zero-copy, sharing the same
/// allocation. All slices keep it alive until the last one is dropped.
/// Replaces the prior per-entry `.to_vec()` copies (2N copies to 0).
pub(crate) fn decode_scan(bytes: Vec<u8>, count: usize) -> Result<Vec<ScanEntry>, CtError> {
    let packed = Bytes::from(bytes);
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
        let mut found: c_int = 0;
        let mut key = sys::ct_buf {
            data: std::ptr::null_mut(),
            len: 0,
        };
        let mut value = sys::ct_buf {
            data: std::ptr::null_mut(),
            len: 0,
        };
        let mut slot = 0_u64;
        check(unsafe {
            sys::ct_seek_reverse(
                self.as_ptr(),
                start_key.as_ptr(),
                start_key.len(),
                if start_inclusive { 1 } else { 0 },
                begin_key.as_ptr(),
                begin_key.len(),
                &mut found,
                &mut key,
                &mut slot,
                &mut value,
            )
        })?;
        let key = take_buf(key);
        let value = take_buf(value);
        Ok((found != 0).then(|| ScanEntry {
            key: Bytes::from(key),
            slot,
            value: Bytes::from(value),
            tombstone: false,
        }))
    }

    /// Range scan over `prefix` (empty = whole keyspace).
    /// When `include_tombstones` is true, tombstone entries are included.
    /// `start_after` (empty = start from beginning) is an exclusive lower
    /// bound: only keys strictly greater than `start_after` are returned.
    /// `end_key` (empty = unbounded) is an exclusive upper bound: only keys
    /// strictly less than `end_key` are returned. When `keys_only` is true,
    /// values are not materialized (no overflow-chain assembly): entries
    /// carry empty values and the byte budget accounts for key bytes only.
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
    ) -> Result<(Vec<ScanEntry>, bool), CtError> {
        let mut buf = sys::ct_buf {
            data: std::ptr::null_mut(),
            len: 0,
        };
        let mut count = 0u64;
        let mut truncated: c_int = 0;
        let status = unsafe {
            if has_start_bound {
                sys::ct_scan_from(
                    self.as_ptr(),
                    prefix.as_ptr(),
                    prefix.len(),
                    start_key.as_ptr(),
                    start_key.len(),
                    if start_inclusive { 1 } else { 0 },
                    end_key.as_ptr(),
                    end_key.len(),
                    limit,
                    byte_budget,
                    if keys_only { 1 } else { 0 },
                    deadline_ms,
                    if include_tombstones { 1 } else { 0 },
                    &mut buf,
                    &mut count,
                    &mut truncated,
                )
            } else {
                sys::ct_scan(
                    self.as_ptr(),
                    prefix.as_ptr(),
                    prefix.len(),
                    start_key.as_ptr(),
                    start_key.len(),
                    end_key.as_ptr(),
                    end_key.len(),
                    limit,
                    byte_budget,
                    if keys_only { 1 } else { 0 },
                    deadline_ms,
                    if include_tombstones { 1 } else { 0 },
                    &mut buf,
                    &mut count,
                    &mut truncated,
                )
            }
        };
        check(status)?;
        let bytes = take_buf(buf);
        let entries = decode_scan(bytes, count as usize)?;
        Ok((entries, truncated != 0))
    }
}
