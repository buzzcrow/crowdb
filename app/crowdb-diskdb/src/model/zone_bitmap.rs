// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Empty zones retain geometry without allocating bitmap words.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use crowdb_protocol::{UsageBitmap, ZoneValueExt};

pub struct ZoneBitmap {
    bitmap: ArcSwapOption<UsageBitmap>,
    block_count: u32,
}

impl ZoneBitmap {
    #[must_use]
    pub fn new(block_count: u32) -> Self {
        Self {
            bitmap: ArcSwapOption::empty(),
            block_count,
        }
    }

    #[must_use]
    pub fn restore(bytes: &[u8], block_count: u32) -> Self {
        if bytes.iter().all(|byte| *byte == 0) {
            return Self::new(block_count);
        }
        Self {
            bitmap: ArcSwapOption::from(Some(Arc::new(UsageBitmap::restore_for_block_count(
                bytes,
                block_count,
            )))),
            block_count,
        }
    }

    /// Restore a zone bitmap, keeping the standard empty bitmap unmaterialized.
    ///
    /// # Errors
    /// Rejects invalid compressed data or a bitmap length inconsistent with geometry.
    pub fn from_value(
        value: &crowdb_protocol::diskdb::rpc::ZoneValue,
        block_count: u32,
    ) -> Result<Self, String> {
        if value.bitmap_codec == 1
            && block_count == 32768
            && value.usage_bitmap == crowdb_protocol::EMPTY_ZONE_BITMAP_LZ4
        {
            return Ok(Self::new(block_count));
        }
        Ok(Self::restore(&value.bitmap_bytes(block_count)?, block_count))
    }

    #[must_use]
    pub fn compressed_snapshot(&self) -> Vec<u8> {
        if self.block_count == 32768 && self.bitmap.load().is_none() {
            return crowdb_protocol::EMPTY_ZONE_BITMAP_LZ4.to_vec();
        }
        let mut value = crowdb_protocol::diskdb::rpc::ZoneValue {
            usage_bitmap: self.snapshot(),
            ..Default::default()
        };
        value.compress_bitmap();
        value.usage_bitmap
    }

    fn materialize(&self) -> Arc<UsageBitmap> {
        if let Some(bitmap) = self.bitmap.load_full() {
            return bitmap;
        }
        let candidate = Arc::new(UsageBitmap::new(self.block_count));
        let previous = self
            .bitmap
            .compare_and_swap(&None::<Arc<UsageBitmap>>, Some(Arc::clone(&candidate)));
        previous.as_ref().map_or(candidate, Arc::clone)
    }

    #[must_use]
    pub fn word_count(&self) -> usize {
        (self.block_count as usize).div_ceil(64)
    }

    #[must_use]
    /// Read a bitmap word without allocating an empty bitmap.
    ///
    /// # Panics
    /// Panics if the index is outside the bitmap geometry.
    pub fn load_word(&self, index: usize) -> u64 {
        assert!(index < self.word_count());
        self.bitmap
            .load()
            .as_ref()
            .map_or(0, |bitmap| bitmap.load_word(index))
    }

    /// Atomically replace a word after materializing the bitmap.
    ///
    /// # Errors
    /// Returns the current word when it differs from `expected`.
    ///
    /// # Panics
    /// Panics if the index is outside the bitmap geometry.
    pub fn cas_word(&self, index: usize, expected: u64, new: u64) -> Result<u64, u64> {
        self.materialize().cas_word(index, expected, new)
    }

    #[must_use]
    pub fn cas_bit(&self, index: u32, set: bool) -> bool {
        self.materialize().cas_bit(index, set)
    }

    #[must_use]
    pub fn range_set(&self, offset: u32, count: u32) -> bool {
        self.materialize().range_set(offset, count)
    }

    #[must_use]
    pub fn range_clear(&self, offset: u32, count: u32) -> bool {
        self.bitmap
            .load()
            .as_ref()
            .map_or(count == 0, |bitmap| bitmap.range_clear(offset, count))
    }

    #[must_use]
    /// Read a bit without materializing an empty bitmap.
    ///
    /// # Panics
    /// Panics if the offset is outside the bitmap geometry.
    pub fn is_set(&self, offset: u32) -> bool {
        assert!(offset < self.block_count);
        self.bitmap
            .load()
            .as_ref()
            .is_some_and(|bitmap| bitmap.is_set(offset))
    }

    #[must_use]
    pub fn count_set(&self) -> u64 {
        self.bitmap.load().as_ref().map_or(0, |bitmap| bitmap.count_set())
    }

    /// An absent bitmap has the same durable meaning as all zero words.
    #[must_use]
    pub fn snapshot(&self) -> Vec<u8> {
        self.bitmap
            .load()
            .as_ref()
            .map_or_else(|| vec![0; self.word_count() * 8], |bitmap| bitmap.snapshot())
    }

    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn is_materialized_for_tests(&self) -> bool {
        self.bitmap.load().is_some()
    }
}
