use super::super::{compact, ParquetMetadataLimits};
use super::{Error, ParquetPageLimits};

pub(super) struct Header {
    pub(super) kind: i64,
    pub(super) values: usize,
    pub(super) encoding: i64,
    pub(super) compressed: usize,
    pub(super) decoded: usize,
    pub(super) is_compressed: bool,
    pub(super) crc: Option<u32>,
    pub(super) definition_encoding: i64,
    pub(super) level_bytes: usize,
    pub(super) nulls: Option<usize>,
}

impl Header {
    pub(super) fn decode(bytes: &[u8], limits: ParquetPageLimits) -> Result<(Self, usize), Error> {
        let (value, consumed) = compact::prefix(
            bytes,
            ParquetMetadataLimits {
                footer_bytes: 64 * 1024,
                values: 1000,
                depth: 8,
                schema_elements: 1,
                row_groups: 1,
            },
        )?;
        let fields = value.fields()?;
        let number = |id| fields.get(&id).ok_or(Error::Invalid)?.integer(5);
        let kind = number(1)?;
        let decoded = usize::try_from(number(2)?).map_err(|_| Error::Invalid)?;
        let compressed = usize::try_from(number(3)?).map_err(|_| Error::Invalid)?;
        if decoded > limits.bytes || compressed > limits.bytes {
            return Err(Error::Bounds);
        }
        let detail = fields
            .get(&match kind {
                0 => 5,
                2 => 7,
                3 => 8,
                _ => return Err(Error::Unsupported),
            })
            .ok_or(Error::Invalid)?
            .fields()?;
        let item = |id| detail.get(&id).ok_or(Error::Invalid)?.integer(5);
        let values = usize::try_from(item(1)?).map_err(|_| Error::Invalid)?;
        if values == 0 || values > limits.values {
            return Err(Error::Bounds);
        }
        let encoding = item(if kind == 3 { 4 } else { 2 })?;
        if kind == 2 && !matches!(encoding, 0 | 2) {
            return Err(Error::Invalid);
        }
        if kind == 0 && (!matches!(item(3)?, 3 | 4) || !matches!(item(4)?, 3 | 4)) {
            return Err(Error::Invalid);
        }
        let mut nulls = None;
        let mut level_bytes = 0;
        let is_compressed = if kind == 3 {
            let count = usize::try_from(item(2)?).map_err(|_| Error::Invalid)?;
            level_bytes = usize::try_from(item(5)?).map_err(|_| Error::Invalid)?;
            nulls = Some(count);
            if count > values
                || item(3)? != i64::try_from(values).map_err(|_| Error::Bounds)?
                || level_bytes > compressed
                || level_bytes > decoded
                || item(6)? != 0
            {
                return Err(Error::Invalid);
            }
            detail
                .get(&7)
                .map(compact::Value::boolean)
                .transpose()?
                .unwrap_or(true)
        } else {
            true
        };
        let crc = fields
            .get(&4)
            .map(|value| {
                let value = i32::try_from(value.integer(5)?).map_err(|_| Error::Invalid)?;
                Ok::<_, Error>(u32::from_ne_bytes(value.to_ne_bytes()))
            })
            .transpose()?;
        Ok((
            Self {
                kind,
                values,
                encoding: if kind == 2 { 0 } else { encoding },
                compressed,
                decoded,
                is_compressed,
                crc,
                definition_encoding: if kind == 0 { item(3)? } else { 3 },
                level_bytes,
                nulls,
            },
            consumed,
        ))
    }
}
