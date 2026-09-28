use std::{ops::Range, sync::Arc};

use super::{
    DeletionVectorError, DeletionVectorLimits, DeletionVectorReference, DeletionVectorStats, FileBlockStore,
    FileLocation, FileRecord,
};

/// Checksum-validated deleted positions for one referenced data file.
#[derive(Debug)]
pub struct DeletionVectorPositions {
    referenced: FileLocation,
    ranges: Vec<Range<u64>>,
    stats: DeletionVectorStats,
}

impl DeletionVectorPositions {
    #[must_use]
    pub fn range_count(&self) -> usize {
        self.ranges.len()
    }

    #[must_use]
    pub fn stats(&self) -> DeletionVectorStats {
        self.stats
    }

    #[must_use]
    pub fn contains(&self, referenced: &FileLocation, position: u64) -> bool {
        if referenced != &self.referenced {
            return false;
        }
        let index = self.ranges.partition_point(|range| range.end <= position);
        self.ranges
            .get(index)
            .is_some_and(|range| range.contains(&position))
    }

    /// Tests whether this vector preserves every deletion in the previous vector.
    #[must_use]
    pub fn covers(&self, previous: &Self) -> bool {
        if self.referenced != previous.referenced {
            return false;
        }
        let mut index = 0;
        for prior in &previous.ranges {
            while self
                .ranges
                .get(index)
                .is_some_and(|range| range.end <= prior.start)
            {
                index += 1;
            }
            if !self
                .ranges
                .get(index)
                .is_some_and(|range| range.start <= prior.start && range.end >= prior.end)
            {
                return false;
            }
        }
        true
    }
}

/// Reads bounded coalesced ranges, returning no positions before checksum validation.
/// # Errors
/// Rejects malformed vectors and range limits outside 1..=1,000,000 or exceeded by the vector.
pub async fn read_deletion_vector_positions(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
    reference: &DeletionVectorReference,
    limits: DeletionVectorLimits,
    ranges: usize,
) -> Result<DeletionVectorPositions, DeletionVectorError> {
    if !(1..=1_000_000).contains(&ranges) {
        return Err(DeletionVectorError::Bounds);
    }
    let mut collector = Some(Collector {
        ranges: Vec::new(),
        limit: ranges,
    });
    let stats = super::decode(store, record, reference, limits, &mut collector).await?;
    let collector = collector.ok_or(DeletionVectorError::Invalid)?;
    Ok(DeletionVectorPositions {
        referenced: reference.referenced.clone(),
        ranges: collector.ranges,
        stats,
    })
}

pub(super) struct Collector {
    ranges: Vec<Range<u64>>,
    limit: usize,
}

pub(super) fn append(
    collector: &mut Option<Collector>,
    start: u64,
    end: u64,
) -> Result<(), DeletionVectorError> {
    let Some(collector) = collector else {
        return Ok(());
    };
    if let Some(previous) = collector.ranges.last_mut() {
        if start < previous.end {
            return Err(DeletionVectorError::Invalid);
        }
        if start == previous.end {
            previous.end = end;
            return Ok(());
        }
    }
    if collector.ranges.len() == collector.limit {
        return Err(DeletionVectorError::Bounds);
    }
    collector.ranges.push(start..end);
    Ok(())
}
