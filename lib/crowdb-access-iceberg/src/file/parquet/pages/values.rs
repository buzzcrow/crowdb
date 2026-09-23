use super::Error;
mod delta;

#[derive(Clone, Debug)]
pub(crate) enum ColumnValue {
    Long(i64),
    Bytes(Vec<u8>),
}

pub(super) fn decode(
    mut bytes: &[u8],
    encoding: i64,
    physical: i32,
    count: usize,
    dictionary: Option<&[ColumnValue]>,
    limit: usize,
) -> Result<Vec<ColumnValue>, Error> {
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
        5 if physical == 2 => {
            for value in delta::integers(&mut bytes, count)? {
                push(&mut result, ColumnValue::Long(value), &mut remaining)?;
            }
        }
        6 | 7 if physical == 6 => {
            result = delta_strings(&mut bytes, encoding, count, limit)?;
        }
        9 if physical == 2 => {
            result = split(bytes, count, limit)?;
            bytes = &[];
        }
        0 => {
            for _ in 0..count {
                let value = if physical == 2 {
                    ColumnValue::Long(i64::from_le_bytes(
                        take(&mut bytes, 8)?.try_into().map_err(|_| Error::Invalid)?,
                    ))
                } else {
                    let length =
                        i32::from_le_bytes(take(&mut bytes, 4)?.try_into().map_err(|_| Error::Invalid)?);
                    let length = usize::try_from(length)
                        .ok()
                        .filter(|length| *length <= 1152)
                        .ok_or(Error::Bounds)?;
                    ColumnValue::Bytes(take(&mut bytes, length)?.to_vec())
                };
                push(&mut result, value, &mut remaining)?;
            }
        }
        2 | 8 => {
            let dictionary = dictionary.ok_or(Error::Invalid)?;
            let width = *take(&mut bytes, 1)?.first().ok_or(Error::Invalid)?;
            if width > 32 {
                return Err(Error::Invalid);
            }
            while result.len() < count {
                let header = unsigned(&mut bytes)?;
                let run = usize::try_from(header >> 1)
                    .ok()
                    .filter(|value| *value > 0)
                    .ok_or(Error::Invalid)?;
                if header & 1 == 0 {
                    if run > count - result.len() {
                        return Err(Error::Invalid);
                    }
                    let packed = take(&mut bytes, usize::from(width).div_ceil(8))?;
                    if width % 8 != 0 && packed.last().is_some_and(|byte| *byte >> (width % 8) != 0) {
                        return Err(Error::Invalid);
                    }
                    let id = bits(packed, 0, width)?;
                    let value = dictionary
                        .get(usize::try_from(id).map_err(|_| Error::Invalid)?)
                        .ok_or(Error::Invalid)?;
                    for _ in 0..run {
                        push(&mut result, value.clone(), &mut remaining)?;
                    }
                } else {
                    let run = run.checked_mul(8).ok_or(Error::Invalid)?;
                    if run > count - result.len() + 7 {
                        return Err(Error::Invalid);
                    }
                    let packed = take(
                        &mut bytes,
                        run.checked_mul(usize::from(width)).ok_or(Error::Invalid)? / 8,
                    )?;
                    for index in 0..run.min(count - result.len()) {
                        let id = bits(packed, index * usize::from(width), width)?;
                        let value = dictionary
                            .get(usize::try_from(id).map_err(|_| Error::Invalid)?)
                            .ok_or(Error::Invalid)?;
                        push(&mut result, value.clone(), &mut remaining)?;
                    }
                }
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
    let size = std::mem::size_of::<ColumnValue>()
        + match &value {
            ColumnValue::Long(_) => 0,
            ColumnValue::Bytes(bytes) => bytes.len(),
        };
    *remaining = remaining.checked_sub(size).ok_or(Error::Bounds)?;
    values.push(value);
    Ok(())
}

fn split(bytes: &[u8], count: usize, mut remaining: usize) -> Result<Vec<ColumnValue>, Error> {
    if bytes.len() != count.checked_mul(8).ok_or(Error::Invalid)? {
        return Err(Error::Invalid);
    }
    let mut result = Vec::new();
    for index in 0..count {
        let mut value = [0; 8];
        for (stream, value) in value.iter_mut().enumerate() {
            *value = bytes[stream * count + index];
        }
        push(
            &mut result,
            ColumnValue::Long(i64::from_le_bytes(value)),
            &mut remaining,
        )?;
    }
    Ok(result)
}

fn delta_strings(
    bytes: &mut &[u8],
    encoding: i64,
    count: usize,
    mut remaining: usize,
) -> Result<Vec<ColumnValue>, Error> {
    let prefixes = if encoding == 7 {
        Some(delta::integers(bytes, count)?)
    } else {
        None
    };
    let lengths = delta::integers(bytes, count)?;
    let mut result = Vec::new();
    for (index, length) in lengths.into_iter().enumerate() {
        let length = usize::try_from(length).map_err(|_| Error::Invalid)?;
        let prefix = prefixes.as_ref().map_or(Ok(0), |prefixes| {
            usize::try_from(prefixes[index]).map_err(|_| Error::Invalid)
        })?;
        if prefix
            .checked_add(length)
            .filter(|length| *length <= 1152)
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
