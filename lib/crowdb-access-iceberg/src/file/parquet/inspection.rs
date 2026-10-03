//! Optional footer fields retained for metadata-only inspection.
use super::{compact::Value, ParquetMetadataError as Error};
use std::collections::BTreeMap;

#[derive(Debug, Default, Eq, PartialEq)]
pub struct ParquetFooterInfo {
    pub offset: u64,
    pub length: usize,
    pub version: i64,
    pub writer: Option<String>,
    pub properties: Vec<(String, Option<String>)>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParquetColumnStatistics {
    pub nulls: Option<i64>,
    pub distinct: Option<i64>,
    pub lower: Option<Vec<u8>>,
    pub upper: Option<Vec<u8>>,
    pub lower_exact: Option<bool>,
    pub upper_exact: Option<bool>,
}
pub(super) fn footer(
    fields: &BTreeMap<i16, Value<'_>>,
    offset: u64,
    length: usize,
) -> Result<ParquetFooterInfo, Error> {
    let mut properties = Vec::new();
    if let Some(values) = fields.get(&5) {
        for value in values.list(12)? {
            let pair = value.fields()?;
            properties.push((
                text(pair.get(&1).ok_or(Error::Invalid)?)?,
                pair.get(&2).map(text).transpose()?,
            ));
        }
    }
    Ok(ParquetFooterInfo {
        offset,
        length,
        version: fields.get(&1).ok_or(Error::Invalid)?.integer(5)?,
        writer: fields.get(&6).map(text).transpose()?,
        properties,
    })
}
pub(super) fn statistics(value: &Value<'_>) -> Result<ParquetColumnStatistics, Error> {
    let fields = value.fields()?;
    let count = |id| {
        fields
            .get(&id)
            .map(|v| {
                v.integer(6)
                    .and_then(|n| if n >= 0 { Ok(n) } else { Err(Error::Invalid) })
            })
            .transpose()
    };
    let bytes = |id| fields.get(&id).map(|v| v.bytes().map(<[u8]>::to_vec)).transpose();
    let boolean = |id| {
        fields
            .get(&id)
            .map(|v| {
                if let Value::Boolean(b) = v {
                    Ok(*b)
                } else {
                    Err(Error::Invalid)
                }
            })
            .transpose()
    };
    Ok(ParquetColumnStatistics {
        nulls: count(3)?,
        distinct: count(4)?,
        lower: bytes(6)?,
        upper: bytes(5)?,
        lower_exact: boolean(8)?,
        upper_exact: boolean(7)?,
    })
}
fn text(value: &Value<'_>) -> Result<String, Error> {
    std::str::from_utf8(value.bytes()?)
        .map(str::to_owned)
        .map_err(|_| Error::Invalid)
}
