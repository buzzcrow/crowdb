// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Utility functions and extension traits for diskdb proto types.
//!
//! These are domain methods on types defined in the proto files
//! (`DiskId`, `HwStatus`, `ZoneAllocationState`, `ZoneValue`). They
//! live here (not in the proto) because they encode business logic,
//! not wire format.

use crate::common::{DiskId, HwStatus};
use crate::diskdb::rpc::{RecoveryScanProgressValue, ZoneAllocationState, ZoneValue};

// ── DiskId helpers ──────────────────────────────────────────────

/// Convenience constructor for `DiskId`.
#[must_use]
pub fn disk_id(high: u64, low: u64) -> DiskId {
    DiskId { high, low }
}

/// Extension trait for `DiskId` display helpers.
pub trait DiskIdExt {
    /// Display format: `{high:016x}-{low:016x}`.
    fn to_display_string(&self) -> String;

    /// Parse a display-format string (`{high:016x}-{low:016x}`) into a
    /// `DiskId`. Also accepts a bare 32-char hex string (no dash).
    ///
    /// # Errors
    /// Returns an error string if the input is malformed.
    fn from_display_string(s: &str) -> Result<DiskId, String>;
}

impl DiskIdExt for DiskId {
    fn to_display_string(&self) -> String {
        format!("{:016x}-{:016x}", self.high, self.low)
    }

    fn from_display_string(s: &str) -> Result<DiskId, String> {
        let (high_hex, low_hex) = if let Some((h, l)) = s.split_once('-') {
            (h, l)
        } else if s.len() == 32 && s.is_ascii() {
            // ASCII-only guarantees byte 16 is a char boundary.
            let (h, l) = (&s[..16], &s[16..]);
            (h, l)
        } else {
            return Err(format!(
                "invalid DiskId string: expected 32 hex chars or high-low pair, got {s}"
            ));
        };
        let high = u64::from_str_radix(high_hex, 16).map_err(|e| format!("invalid high: {e}"))?;
        let low = u64::from_str_radix(low_hex, 16).map_err(|e| format!("invalid low: {e}"))?;
        Ok(DiskId { high, low })
    }
}

// ── HwStatus helpers ────────────────────────────────────────────

/// Extension trait for `HwStatus` domain logic.
pub trait HwStatusExt {
    /// Whether allocations are allowed at this status.
    fn allows_allocate(&self) -> bool;

    /// Whether frees are allowed at this status.
    fn allows_free(&self) -> bool;
}

impl HwStatusExt for HwStatus {
    fn allows_allocate(&self) -> bool {
        matches!(self, HwStatus::Up)
    }

    fn allows_free(&self) -> bool {
        matches!(self, HwStatus::Up | HwStatus::Maintenance | HwStatus::Suspect)
    }
}

/// Compute the effective status for a disk: the most restrictive
/// (highest-priority) of node, group, and disk status.
#[must_use]
pub fn effective_status(node: HwStatus, group: HwStatus, disk: HwStatus) -> HwStatus {
    node.max(group).max(disk)
}

// ── ZoneAllocationState helpers ─────────────────────────────────

/// Extension trait for `ZoneAllocationState` atomic interop.
pub trait ZoneAllocationStateExt {
    /// Decode a `u8` (e.g. from an `AtomicU8` load) into a state.
    /// Unknown values map to `Full` (defensive — corruption/bug).
    #[must_use]
    fn from_u8(v: u8) -> Self;
}

impl ZoneAllocationStateExt for ZoneAllocationState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::ZoneAllocActive,
            1 => Self::ZoneAllocAvailable,
            // 2 and unknown both map to Full; explicit arm documents the known value.
            #[allow(clippy::match_same_arms)]
            2 => Self::ZoneAllocFull,
            _ => Self::ZoneAllocFull,
        }
    }
}

// ── ZoneValue CRC integrity ─────────────────────────────────────

/// Extension trait for `ZoneValue` CRC32 integrity.
///
/// The CRC32 is computed over `usage_bitmap` + `compact_ts` and stored
/// in the dedicated `crc32` field (proto field 7). The baseline
/// `ZoneValue` written during disk-add init has an empty bitmap,
/// `compact_ts = 0`, `snapshot_slot = 0`. A corrupted `compact_ts`
/// would break the watermark logic, so it is integrity-protected
/// alongside the bitmap.
/// LZ4 block with a little-endian decoded length of 4096 bytes.
pub const EMPTY_ZONE_BITMAP_LZ4: &[u8] = &[
    0, 16, 0, 0, 31, 0, 1, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 245,
    96, 0, 0, 0, 0, 0, 0,
];
const ZONE_RECORD_PREFIX: [u8; 8] = u64::MAX.to_le_bytes();

pub trait ZoneValueExt {
    /// Decode a bitmap with the disk geometry as its allocation bound.
    ///
    /// # Errors
    /// Rejects unsupported codecs, malformed LZ4 and lengths outside the disk geometry.
    fn bitmap_bytes(&self, block_count: u32) -> Result<Vec<u8>, String>;
    /// Store the supplied raw bitmap as LZ4, reusing the standard empty value.
    fn compress_bitmap(&mut self);

    /// Compute and set `crc32` from `usage_bitmap` + `compact_ts`.
    fn compute_checksum(&mut self);

    /// Verify `crc32` matches `usage_bitmap` + `compact_ts`.
    #[must_use]
    fn verify_checksum(&self) -> bool;

    /// Serialize to bytes (bincode).
    ///
    /// # Panics
    /// Panics if bincode serialization fails (cannot fail for this type).
    #[must_use]
    fn to_bytes(&self) -> Vec<u8>;

    /// Deserialize from bytes (bincode).
    ///
    /// # Errors
    /// Returns `Err` if the bytes cannot be deserialized into a `ZoneValue`.
    fn from_bytes(bytes: &[u8]) -> Result<ZoneValue, String>;
}

impl ZoneValueExt for ZoneValue {
    fn compress_bitmap(&mut self) {
        if self.usage_bitmap.len() == 4096 && self.usage_bitmap.iter().all(|byte| *byte == 0) {
            self.usage_bitmap = EMPTY_ZONE_BITMAP_LZ4.to_vec();
        } else {
            self.usage_bitmap = lz4_flex::compress_prepend_size(&self.usage_bitmap);
        }
        self.bitmap_codec = 1;
    }

    fn bitmap_bytes(&self, block_count: u32) -> Result<Vec<u8>, String> {
        if self.bitmap_codec != 1 {
            return Err("unsupported zone bitmap codec".into());
        }
        let header: [u8; 4] = self
            .usage_bitmap
            .get(..4)
            .ok_or("truncated LZ4 bitmap")?
            .try_into()
            .map_err(|_| "truncated LZ4 bitmap")?;
        let size = (block_count as usize).div_ceil(64) * 8;
        if u32::from_le_bytes(header) as usize != size {
            return Err("LZ4 bitmap length does not match disk geometry".into());
        }
        lz4_flex::decompress_size_prepended(&self.usage_bitmap).map_err(|error| error.to_string())
    }

    fn compute_checksum(&mut self) {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[self.bitmap_codec]);
        hasher.update(&self.usage_bitmap);
        hasher.update(&self.compact_slot.to_le_bytes());
        self.crc32 = hasher.finalize();
    }

    fn verify_checksum(&self) -> bool {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[self.bitmap_codec]);
        hasher.update(&self.usage_bitmap);
        hasher.update(&self.compact_slot.to_le_bytes());
        self.crc32 == hasher.finalize()
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = ZONE_RECORD_PREFIX.to_vec();
        bytes.extend_from_slice(&bincode::serialize(self).expect("serialize ZoneValue"));
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<ZoneValue, String> {
        let record = bytes
            .strip_prefix(&ZONE_RECORD_PREFIX)
            .ok_or("unsupported zone record format")?;
        let value: Self = bincode::deserialize(record).map_err(|error| error.to_string())?;
        if value.bitmap_codec != 1 {
            return Err("unsupported zone bitmap codec".into());
        }
        Ok(value)
    }
}

// ── RecoveryScanProgressValue serialization ─────────────────────

/// Extension trait for `RecoveryScanProgressValue` bincode
/// serialization (same wire format as other diskdb data-group
/// values).
pub trait RecoveryScanProgressValueExt {
    /// Serialize to bytes (bincode).
    ///
    /// # Panics
    /// Panics if bincode serialization fails (cannot fail for this type).
    #[must_use]
    fn to_bytes(&self) -> Vec<u8>;

    /// Deserialize from bytes (bincode).
    ///
    /// # Errors
    /// Returns `Err` if the bytes cannot be deserialized.
    fn from_bytes(bytes: &[u8]) -> Result<RecoveryScanProgressValue, String>;
}

impl RecoveryScanProgressValueExt for RecoveryScanProgressValue {
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("serialize RecoveryScanProgressValue")
    }

    fn from_bytes(bytes: &[u8]) -> Result<RecoveryScanProgressValue, String> {
        bincode::deserialize(bytes).map_err(|e| e.to_string())
    }
}
