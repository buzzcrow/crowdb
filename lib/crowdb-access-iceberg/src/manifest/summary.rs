use super::{
    entry::bounds, ManifestContext, ManifestEntryError as Error, ManifestListEntry, PartitionTransform,
    PartitionValue, PrimitiveType,
};
use crate::file::{AvroContainerError, AvroScalar};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionSummary {
    pub contains_null: bool,
    pub contains_nan: Option<bool>,
    pub lower_bound: Option<Vec<u8>>,
    pub upper_bound: Option<Vec<u8>>,
}

impl PartitionSummary {
    pub(super) fn decode(values: &[AvroScalar<'_>]) -> Result<Self, super::ManifestListError> {
        use super::ManifestListError::Field;
        let AvroScalar::Boolean(contains_null) = values[0] else {
            return Err(Field);
        };
        let contains_nan = match values[1] {
            AvroScalar::Null => None,
            AvroScalar::Boolean(value) => Some(value),
            _ => return Err(Field),
        };
        let read = |value| match value {
            AvroScalar::Null => Ok(None),
            AvroScalar::Bytes(value) => Ok(Some(value.to_vec())),
            _ => Err(Field),
        };
        Ok(Self {
            contains_null,
            contains_nan,
            lower_bound: read(values[2])?,
            upper_bound: read(values[3])?,
        })
    }
}

impl ManifestListEntry {
    /// Binds optional summaries to historical spec order, types and independent byte limits.
    /// # Errors
    /// Rejects wrong specs, counts, NaN flags, bound encodings, ordering and void bounds.
    pub fn validate_partition_summaries(&self, context: &ManifestContext) -> Result<(), Error> {
        if self.partition_spec_id != context.spec_id() {
            return Err(Error::Field);
        }
        let Some(summaries) = &self.partitions else {
            return Ok(());
        };
        if summaries.len() != context.partitions().len() || summaries.len() > 256 {
            return Err(Error::Field);
        }
        let mut remaining = 1024 * 1024_usize;
        for (summary, field) in summaries.iter().zip(context.partitions()) {
            for bytes in [&summary.lower_bound, &summary.upper_bound].into_iter().flatten() {
                remaining = remaining
                    .checked_sub(bytes.len())
                    .ok_or(AvroContainerError::Bounds)?;
            }
            if field.transform == PartitionTransform::Void
                && (summary.lower_bound.is_some()
                    || summary.upper_bound.is_some()
                    || summary.contains_nan == Some(true))
            {
                return Err(Error::Field);
            }
            if let Some(kind) = &field.result {
                if summary.contains_nan == Some(true)
                    && !matches!(kind, PrimitiveType::Float | PrimitiveType::Double)
                {
                    return Err(Error::Field);
                }
                bounds::validate(
                    kind,
                    summary.lower_bound.as_deref(),
                    summary.upper_bound.as_deref(),
                )?;
            }
        }
        Ok(())
    }
}

pub(super) struct SummaryState {
    nulls: Vec<bool>,
    nans: Vec<bool>,
}

impl SummaryState {
    pub(super) fn new(list: &ManifestListEntry, context: &ManifestContext) -> Result<Self, Error> {
        list.validate_partition_summaries(context)?;
        let count = list.partitions.as_ref().map_or(0, Vec::len);
        Ok(Self {
            nulls: vec![false; count],
            nans: vec![false; count],
        })
    }

    pub(super) fn observe(
        &mut self,
        list: &ManifestListEntry,
        context: &ManifestContext,
        values: &[(i32, PartitionValue)],
    ) -> Result<(), Error> {
        let Some(summaries) = &list.partitions else {
            return Ok(());
        };
        if values.len() != summaries.len() {
            return Err(Error::Field);
        }
        for (index, ((summary, field), (id, value))) in
            summaries.iter().zip(context.partitions()).zip(values).enumerate()
        {
            if *id != field.id {
                return Err(Error::Field);
            }
            if *value == PartitionValue::Null {
                self.nulls[index] = true;
                if !summary.contains_null {
                    return Err(Error::Field);
                }
                continue;
            }
            let nan = match value {
                PartitionValue::Float(value) => f32::from_bits(*value).is_nan(),
                PartitionValue::Double(value) => f64::from_bits(*value).is_nan(),
                _ => false,
            };
            if nan {
                self.nans[index] = true;
                if summary.contains_nan == Some(false) {
                    return Err(Error::Field);
                }
                continue;
            }
            if let Some(kind) = &field.result {
                let encoded = encode(value)?;
                bounds::validate(kind, summary.lower_bound.as_deref(), Some(&encoded))?;
                bounds::validate(kind, Some(&encoded), summary.upper_bound.as_deref())?;
            }
        }
        Ok(())
    }

    pub(super) fn finish(&self, list: &ManifestListEntry, context: &ManifestContext) -> Result<(), Error> {
        if let Some(summaries) = &list.partitions {
            for (index, (summary, field)) in summaries.iter().zip(context.partitions()).enumerate() {
                if summary.contains_null != self.nulls[index]
                    || field.result.is_some()
                        && summary
                            .contains_nan
                            .is_some_and(|expected| expected != self.nans[index])
                {
                    return Err(Error::Field);
                }
            }
        }
        Ok(())
    }
}

fn encode(value: &PartitionValue) -> Result<Vec<u8>, Error> {
    Ok(match value {
        PartitionValue::Boolean(value) => vec![u8::from(*value)],
        PartitionValue::Int(value) => value.to_le_bytes().to_vec(),
        PartitionValue::Long(value) => value.to_le_bytes().to_vec(),
        PartitionValue::Float(value) => value.to_le_bytes().to_vec(),
        PartitionValue::Double(value) => value.to_le_bytes().to_vec(),
        PartitionValue::String(value) => value.as_bytes().to_vec(),
        PartitionValue::Bytes(value) => value.clone(),
        _ => return Err(Error::Field),
    })
}
