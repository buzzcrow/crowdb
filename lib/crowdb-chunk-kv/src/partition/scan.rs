// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{Partition, ScanPage};
use crate::{ChunkKvError, JournalPosition, Result};

impl Partition {
    /// Scans with a hard pre-materialization bound, including the first value.
    /// # Errors
    /// Rejects invalid bounds and unavailable storage. An oversized first entry
    /// returns an empty truncated page without materializing its value.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_forward_bounded(
        &self,
        ownership_epoch: u64,
        start: &[u8],
        inclusive: bool,
        end: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
        minimum: Option<JournalPosition>,
    ) -> Result<ScanPage> {
        self.scan_forward_bound(
            ownership_epoch,
            Some(start),
            inclusive,
            end,
            limit,
            byte_budget,
            minimum,
            true,
        )
        .await
    }

    /// Returns a bounded forward page over the half-open interval
    /// `[start_key, end_key)`, clipped to this partition.
    ///
    /// The lower bound is inclusive and the upper bound is exclusive. Bounds
    /// outside the partition are clipped; a non-intersecting interval returns
    /// no entries.
    ///
    /// # Errors
    ///
    /// Returns a typed epoch, lifecycle, bound, or tree-read error.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_forward(
        &self,
        ownership_epoch: u64,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
        min_journal_position: Option<JournalPosition>,
    ) -> Result<ScanPage> {
        self.scan_forward_bound(
            ownership_epoch,
            start_key,
            true,
            end_key,
            limit,
            byte_budget,
            min_journal_position,
            false,
        )
        .await
    }

    /// Returns a bounded forward continuation strictly after `start_after`.
    ///
    /// This is distinct from the inclusive initial range lower bound so a
    /// continuation cannot repeat its last emitted key.
    ///
    /// # Errors
    ///
    /// Returns a typed epoch, lifecycle, bound, or tree-read error.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_forward_after(
        &self,
        ownership_epoch: u64,
        start_after: &[u8],
        end_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
        min_journal_position: Option<JournalPosition>,
    ) -> Result<ScanPage> {
        self.scan_forward_bound(
            ownership_epoch,
            Some(start_after),
            false,
            end_key,
            limit,
            byte_budget,
            min_journal_position,
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn scan_forward_bound(
        &self,
        ownership_epoch: u64,
        start_key: Option<&[u8]>,
        start_inclusive: bool,
        end_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
        min_journal_position: Option<JournalPosition>,
        strict: bool,
    ) -> Result<ScanPage> {
        self.validate_epoch(ownership_epoch)?;
        self.validate_read_lifecycle()?;
        if limit == 0 || byte_budget == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "scan count and byte bounds must be nonzero".into(),
            ));
        }
        if start_key.is_some_and(|key| key.len() > self.config.max_key_bytes)
            || end_key.is_some_and(|key| key.len() > self.config.max_key_bytes)
        {
            return Err(ChunkKvError::InvalidRequest(
                "scan bound exceeds configured key limit".into(),
            ));
        }
        if let (Some(start), Some(end)) = (start_key, end_key) {
            if start >= end {
                return Err(ChunkKvError::InvalidRequest(
                    "scan interval is empty or reversed".into(),
                ));
            }
        }
        if let Some(position) = min_journal_position {
            self.wait_applied(position).await?;
        }
        let range = self.range.load_full();
        let partition_start = range.start.as_deref();
        let partition_end = range.end.as_deref();
        if start_key
            .zip(partition_end)
            .is_some_and(|(start, end)| start >= end)
            || end_key
                .zip(partition_start)
                .is_some_and(|(end, start)| end <= start)
        {
            self.metrics.forward_scan(0);
            return Ok(ScanPage {
                entries: Vec::new(),
                truncated: false,
            });
        }
        let clipped_start = match (start_key, partition_start) {
            (Some(start), Some(partition_start)) if start < partition_start => None,
            (start, _) => start,
        };
        let clipped_end = match (end_key, partition_end) {
            (Some(end), Some(partition_end)) => Some(end.min(partition_end)),
            (Some(end), None) => Some(end),
            (None, end) => end,
        };
        let (entries, truncated) = if strict {
            self.tree
                .scan_forward_bounded(clipped_start, start_inclusive, clipped_end, limit, byte_budget)
                .await?
        } else {
            self.tree
                .scan_forward(clipped_start, start_inclusive, clipped_end, limit, byte_budget)
                .await?
        };
        if entries.iter().any(|entry| !range.contains(&entry.key)) {
            return Err(ChunkKvError::TreeCorruption(
                "tree scan returned a key outside the partition".into(),
            ));
        }
        self.metrics.forward_scan(entries.len());
        Ok(ScanPage { entries, truncated })
    }

    /// Returns a bounded descending page clipped to this partition.
    ///
    /// `start_before` is exclusive and `begin_key` is inclusive. Bounds
    /// outside the partition are clipped; a non-intersecting interval returns
    /// no entries.
    ///
    /// # Errors
    ///
    /// Returns a typed epoch, lifecycle, bound, or tree-read error.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_reverse(
        &self,
        ownership_epoch: u64,
        start_before: Option<&[u8]>,
        begin_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
        min_journal_position: Option<JournalPosition>,
    ) -> Result<ScanPage> {
        self.validate_epoch(ownership_epoch)?;
        self.validate_read_lifecycle()?;
        if limit == 0 || byte_budget == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "scan count and byte bounds must be nonzero".into(),
            ));
        }
        if start_before.is_some_and(|key| key.len() > self.config.max_key_bytes)
            || begin_key.is_some_and(|key| key.len() > self.config.max_key_bytes)
        {
            return Err(ChunkKvError::InvalidRequest(
                "scan bound exceeds configured key limit".into(),
            ));
        }
        if begin_key
            .zip(start_before)
            .is_some_and(|(begin, start)| begin >= start)
        {
            return Err(ChunkKvError::InvalidRequest(
                "scan interval is empty or reversed".into(),
            ));
        }
        if let Some(position) = min_journal_position {
            self.wait_applied(position).await?;
        }
        let range = self.range.load_full();
        let partition_start = range.start.as_deref();
        let partition_end = range.end.as_deref();
        if start_before
            .zip(partition_start)
            .is_some_and(|(start, partition_start)| start <= partition_start)
            || begin_key
                .zip(partition_end)
                .is_some_and(|(begin, partition_end)| begin >= partition_end)
        {
            self.metrics.reverse_scan(0);
            return Ok(ScanPage {
                entries: Vec::new(),
                truncated: false,
            });
        }
        let clipped_start = match (start_before, partition_end) {
            (Some(start), Some(partition_end)) => Some(start.min(partition_end)),
            (Some(start), None) => Some(start),
            (None, end) => end,
        };
        let clipped_begin = match (begin_key, partition_start) {
            (Some(begin), Some(partition_start)) => Some(begin.max(partition_start)),
            (Some(begin), None) => Some(begin),
            (None, start) => start,
        };
        let (entries, truncated) = self
            .tree
            .scan_reverse(clipped_start, clipped_begin, limit, byte_budget)
            .await?;
        if entries.iter().any(|entry| !range.contains(&entry.key))
            || entries.windows(2).any(|pair| pair[0].key <= pair[1].key)
        {
            return Err(ChunkKvError::TreeCorruption(
                "tree reverse scan returned invalid key order or range".into(),
            ));
        }
        self.metrics.reverse_scan(entries.len());
        Ok(ScanPage { entries, truncated })
    }
}
