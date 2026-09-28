use super::Error;
mod delta;
mod dictionary;
mod scalar;

#[derive(Clone, Copy)]
pub(super) struct Physical {
    kind: i32,
    width: usize,
}

impl Physical {
    pub(super) fn new(field: &crate::file::ParquetSchemaElement) -> Result<Self, Error> {
        let kind = field.physical_type.ok_or(Error::Invalid)?;
        let width = match kind {
            0 | 6 => 0,
            1 | 4 => 4,
            2 | 5 => 8,
            7 => usize::try_from(field.type_length.ok_or(Error::Invalid)?)
                .ok()
                .filter(|width| (1..=1024 * 1024).contains(width))
                .ok_or(Error::Bounds)?,
            _ => return Err(Error::Unsupported),
        };
        Ok(Self { kind, width })
    }
}

#[derive(Debug)]
pub(crate) enum ColumnValue {
    Null,
    Long(i64),
    Boolean(bool),
    Float(u32),
    Double(u64),
    Bytes(Vec<u8>),
}

pub(super) fn decode(
    mut bytes: &[u8],
    encoding: i64,
    physical: Physical,
    count: usize,
    dictionary: Option<&[ColumnValue]>,
    limit: usize,
) -> Result<Vec<ColumnValue>, Error> {
    validate_encoding(encoding, physical.kind)?;
    if count == 0 && bytes.is_empty() {
        return Ok(Vec::new());
    }
    if count
        .checked_mul(std::mem::size_of::<ColumnValue>())
        .filter(|bytes| *bytes <= limit)
        .is_none()
    {
        return Err(Error::Bounds);
    }
    let mut result = Vec::new();
    let mut remaining = limit;
    match encoding {
        5 => {
            for value in delta::integers(&mut bytes, count, if physical.kind == 1 { 32 } else { 64 })? {
                push(&mut result, ColumnValue::Long(value), &mut remaining)?;
            }
        }
        6 | 7 => {
            result = delta_strings(&mut bytes, encoding, count, limit)?;
            if physical.kind == 7
                && result
                    .iter()
                    .any(|value| !matches!(value, ColumnValue::Bytes(bytes) if bytes.len() == physical.width))
            {
                return Err(Error::Invalid);
            }
        }
        9 => {
            result = scalar::split(bytes, physical, count, limit)?;
            bytes = &[];
        }
        0 => {
            result = scalar::plain(&mut bytes, physical, count, limit)?;
        }
        2 | 8 => {
            let dictionary = dictionary.ok_or(Error::Invalid)?;
            let width = *take(&mut bytes, 1)?.first().ok_or(Error::Invalid)?;
            result = dictionary::decode(&mut bytes, width, count, dictionary, limit)?;
        }
        3 => {
            let length = usize::try_from(u32::from_le_bytes(
                take(&mut bytes, 4)?.try_into().map_err(|_| Error::Invalid)?,
            ))
            .map_err(|_| Error::Bounds)?;
            let mut encoded = take(&mut bytes, length)?;
            result = dictionary::decode(
                &mut encoded,
                1,
                count,
                &[ColumnValue::Boolean(false), ColumnValue::Boolean(true)],
                limit,
            )?;
            if !encoded.is_empty() {
                return Err(Error::Invalid);
            }
        }
        _ => return Err(Error::Unsupported),
    }
    if !bytes.is_empty() {
        return Err(Error::Invalid);
    }
    Ok(result)
}

fn push(values: &mut Vec<ColumnValue>, value: ColumnValue, remaining: &mut usize) -> Result<(), Error> {
    charge(&value, remaining)?;
    values.push(value);
    Ok(())
}

fn push_copy(values: &mut Vec<ColumnValue>, value: &ColumnValue, remaining: &mut usize) -> Result<(), Error> {
    charge(value, remaining)?;
    values.push(match value {
        ColumnValue::Null => ColumnValue::Null,
        ColumnValue::Long(value) => ColumnValue::Long(*value),
        ColumnValue::Boolean(value) => ColumnValue::Boolean(*value),
        ColumnValue::Float(value) => ColumnValue::Float(*value),
        ColumnValue::Double(value) => ColumnValue::Double(*value),
        ColumnValue::Bytes(bytes) => ColumnValue::Bytes(bytes.clone()),
    });
    Ok(())
}

fn charge(value: &ColumnValue, remaining: &mut usize) -> Result<(), Error> {
    let size = std::mem::size_of::<ColumnValue>()
        + match value {
            ColumnValue::Long(_)
            | ColumnValue::Null
            | ColumnValue::Boolean(_)
            | ColumnValue::Float(_)
            | ColumnValue::Double(_) => 0,
            ColumnValue::Bytes(bytes) => bytes.len(),
        };
    *remaining = remaining.checked_sub(size).ok_or(Error::Bounds)?;
    Ok(())
}

fn validate_encoding(encoding: i64, physical: i32) -> Result<(), Error> {
    if matches!(encoding, 0 | 2 | 8)
        || (encoding == 5 && matches!(physical, 1 | 2))
        || (encoding == 9 && matches!(physical, 1 | 2 | 4 | 5 | 7))
        || (encoding == 6 && physical == 6)
        || (encoding == 7 && matches!(physical, 6 | 7))
        || (encoding == 3 && physical == 0)
    {
        Ok(())
    } else {
        Err(Error::Unsupported)
    }
}

fn integer(bytes: &[u8]) -> Result<ColumnValue, Error> {
    Ok(ColumnValue::Long(match bytes.len() {
        4 => i64::from(i32::from_le_bytes(bytes.try_into().map_err(|_| Error::Invalid)?)),
        8 => i64::from_le_bytes(bytes.try_into().map_err(|_| Error::Invalid)?),
        _ => return Err(Error::Invalid),
    }))
}

fn delta_strings(
    bytes: &mut &[u8],
    encoding: i64,
    count: usize,
    mut remaining: usize,
) -> Result<Vec<ColumnValue>, Error> {
    let prefixes = if encoding == 7 {
        Some(delta::integers(bytes, count, 64)?)
    } else {
        None
    };
    let lengths = delta::integers(bytes, count, 64)?;
    let mut result = Vec::new();
    for (index, length) in lengths.into_iter().enumerate() {
        let length = usize::try_from(length).map_err(|_| Error::Invalid)?;
        let prefix = prefixes.as_ref().map_or(Ok(0), |prefixes| {
            usize::try_from(prefixes[index]).map_err(|_| Error::Invalid)
        })?;
        if prefix
            .checked_add(length)
            .filter(|length| *length <= remaining.saturating_sub(std::mem::size_of::<ColumnValue>()))
            .is_none()
        {
            return Err(Error::Bounds);
        }
        let mut value = match result.last() {
            Some(ColumnValue::Bytes(previous)) => previous.get(..prefix).ok_or(Error::Invalid)?.to_vec(),
            None if prefix == 0 => vec![],
            _ => return Err(Error::Invalid),
        };
        value.extend(take(bytes, length)?);
        push(&mut result, ColumnValue::Bytes(value), &mut remaining)?;
    }
    Ok(result)
}

fn bits(bytes: &[u8], offset: usize, width: u8) -> Result<u64, Error> {
    let mut value = 0;
    for bit in 0..usize::from(width) {
        let position = offset + bit;
        value |= u64::from((bytes.get(position / 8).ok_or(Error::Invalid)? >> (position % 8)) & 1) << bit;
    }
    Ok(value)
}

fn unsigned(bytes: &mut &[u8]) -> Result<u64, Error> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let byte = take(bytes, 1)?[0];
        if shift == 63 && byte > 1 {
            return Err(Error::Invalid);
        }
        value |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return Ok(value);
        }
    }
    Err(Error::Invalid)
}

fn take<'data>(bytes: &mut &'data [u8], length: usize) -> Result<&'data [u8], Error> {
    let value = bytes.get(..length).ok_or(Error::Invalid)?;
    *bytes = &bytes[length..];
    Ok(value)
}
