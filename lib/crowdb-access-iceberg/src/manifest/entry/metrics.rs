use std::collections::BTreeMap;

use crate::file::{AvroMetricValue, AvroScalar};

use super::ManifestEntryError;

#[derive(Debug, Default, Eq, PartialEq)]
pub struct ManifestMetrics {
    pub column_sizes: Option<BTreeMap<i32, i64>>,
    pub value_counts: Option<BTreeMap<i32, i64>>,
    pub null_value_counts: Option<BTreeMap<i32, i64>>,
    pub nan_value_counts: Option<BTreeMap<i32, i64>>,
    pub lower_bounds: Option<BTreeMap<i32, Vec<u8>>>,
    pub upper_bounds: Option<BTreeMap<i32, Vec<u8>>>,
}

pub(super) fn decode(values: &[AvroScalar<'_>]) -> Result<ManifestMetrics, ManifestEntryError> {
    let mut budget = Budget {
        entries: 4096,
        bytes: 1024 * 1024,
    };
    let metrics = ManifestMetrics {
        column_sizes: budget.counts(values[0])?,
        value_counts: budget.counts(values[1])?,
        null_value_counts: budget.counts(values[2])?,
        nan_value_counts: budget.counts(values[3])?,
        lower_bounds: budget.bounds(values[4])?,
        upper_bounds: budget.bounds(values[5])?,
    };
    if let Some(counts) = &metrics.value_counts {
        for (id, count) in counts {
            let nulls = metrics
                .null_value_counts
                .as_ref()
                .and_then(|values| values.get(id))
                .copied()
                .unwrap_or(0);
            let nans = metrics
                .nan_value_counts
                .as_ref()
                .and_then(|values| values.get(id))
                .copied()
                .unwrap_or(0);
            if !nulls.checked_add(nans).is_some_and(|total| total <= *count) {
                return Err(ManifestEntryError::Field);
            }
        }
    }
    Ok(metrics)
}

struct Budget {
    entries: usize,
    bytes: usize,
}

impl Budget {
    fn counts(&mut self, value: AvroScalar<'_>) -> Result<Option<BTreeMap<i32, i64>>, ManifestEntryError> {
        self.map(value, |value| {
            let AvroMetricValue::Long(value) = value else {
                return Err(ManifestEntryError::Field);
            };
            if value < 0 {
                return Err(ManifestEntryError::Field);
            }
            Ok(value)
        })
    }

    fn bounds(
        &mut self,
        value: AvroScalar<'_>,
    ) -> Result<Option<BTreeMap<i32, Vec<u8>>>, ManifestEntryError> {
        self.map(value, |value| {
            let AvroMetricValue::Bytes(value) = value else {
                return Err(ManifestEntryError::Field);
            };
            Ok(value.to_vec())
        })
    }

    fn map<Value>(
        &mut self,
        value: AvroScalar<'_>,
        convert: impl Fn(AvroMetricValue<'_>) -> Result<Value, ManifestEntryError>,
    ) -> Result<Option<BTreeMap<i32, Value>>, ManifestEntryError> {
        if value == AvroScalar::Null {
            return Ok(None);
        }
        let AvroScalar::MetricMap(map) = value else {
            return Err(ManifestEntryError::Field);
        };
        let mut values = BTreeMap::new();
        map.visit(self.entries, 1024 * 1024, |id, value| {
            if id <= 0 || values.contains_key(&id) {
                return Err(ManifestEntryError::Field);
            }
            self.entries = self
                .entries
                .checked_sub(1)
                .ok_or(crate::file::AvroContainerError::Bounds)?;
            let bytes = match value {
                AvroMetricValue::Long(_) => 8,
                AvroMetricValue::Bytes(value) => value.len(),
            };
            self.bytes = self
                .bytes
                .checked_sub(bytes)
                .ok_or(crate::file::AvroContainerError::Bounds)?;
            values.insert(id, convert(value)?);
            Ok(())
        })?;
        Ok(Some(values))
    }
}
