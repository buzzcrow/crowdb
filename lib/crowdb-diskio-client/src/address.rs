// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Checked semantic `DiskIO` addresses.

use crate::{DiskioError, DiskioResult};

/// Globally unique 128-bit disk identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DiskId {
    pub high: u64,
    pub low: u64,
}

impl DiskId {
    #[must_use]
    pub const fn new(high: u64, low: u64) -> Self {
        Self { high, low }
    }
}

/// One allocated segment expressed in bytes relative to its zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentTarget {
    pub(crate) disk_id: DiskId,
    pub(crate) zone_index: u32,
    pub(crate) segment_base: u64,
    pub(crate) segment_capacity: u64,
    pub(crate) unit_size: u32,
}

impl SegmentTarget {
    /// Build and validate an allocated segment target.
    ///
    /// # Errors
    ///
    /// Returns [`DiskioError::InvalidInput`] for zero units or arithmetic
    /// overflow.
    pub fn new(
        disk_id: DiskId,
        zone_index: u32,
        unit_offset: u64,
        unit_count: u32,
        unit_size: u32,
    ) -> DiskioResult<Self> {
        if unit_size == 0 {
            return Err(DiskioError::InvalidInput("unit size must be nonzero".into()));
        }
        if unit_count == 0 {
            return Err(DiskioError::InvalidInput(
                "segment unit count must be nonzero".into(),
            ));
        }
        let unit_size_u64 = u64::from(unit_size);
        let segment_base = unit_offset
            .checked_mul(unit_size_u64)
            .ok_or_else(|| DiskioError::InvalidInput("segment base overflows u64".into()))?;
        let segment_capacity = u64::from(unit_count)
            .checked_mul(unit_size_u64)
            .ok_or_else(|| DiskioError::InvalidInput("segment capacity overflows u64".into()))?;
        Ok(Self {
            disk_id,
            zone_index,
            segment_base,
            segment_capacity,
            unit_size,
        })
    }

    #[must_use]
    pub const fn disk_id(self) -> DiskId {
        self.disk_id
    }

    #[must_use]
    pub const fn capacity(self) -> u64 {
        self.segment_capacity
    }

    /// Validate a segment-relative range without admitting an RPC.
    ///
    /// # Errors
    ///
    /// Returns [`DiskioError::InvalidInput`] when the range overflows or lies
    /// outside the segment.
    pub fn validate_range(self, offset: u64, length: usize) -> DiskioResult<()> {
        self.checked_range(offset, length).map(|_| ())
    }

    pub(crate) fn checked_range(self, offset: u64, length: usize) -> DiskioResult<(u64, u32)> {
        if self.unit_size == 0 {
            return Err(DiskioError::InvalidInput("unit size must be nonzero".into()));
        }
        let length_u64 = u64::try_from(length)
            .map_err(|_| DiskioError::InvalidInput("operation length exceeds u64".into()))?;
        let length_u32 = u32::try_from(length)
            .map_err(|_| DiskioError::InvalidInput("operation length exceeds DiskIO wire limit".into()))?;
        let end = offset
            .checked_add(length_u64)
            .ok_or_else(|| DiskioError::InvalidInput("segment-relative range overflows u64".into()))?;
        if end > self.segment_capacity {
            return Err(DiskioError::InvalidInput(format!(
                "segment-relative end {end} exceeds capacity {}",
                self.segment_capacity
            )));
        }
        let zone_offset = self
            .segment_base
            .checked_add(offset)
            .ok_or_else(|| DiskioError::InvalidInput("zone offset overflows u64".into()))?;
        Ok((zone_offset, length_u32))
    }
}
