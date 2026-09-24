use super::{values::ColumnValue, Error, Header};

pub(super) fn presence(bytes: &mut &[u8], header: &Header, maximum: u8) -> Result<Option<Vec<bool>>, Error> {
    if maximum == 0 {
        if header.nulls.is_some_and(|nulls| nulls != 0) || header.level_bytes != 0 {
            return Err(Error::Invalid);
        }
        return Ok(None);
    }
    let width = u8::try_from(u8::BITS - maximum.leading_zeros()).map_err(|_| Error::Invalid)?;
    let length = if header.kind == 3 {
        header.level_bytes
    } else if header.definition_encoding == 3 {
        usize::try_from(u32::from_le_bytes(
            take(bytes, 4)?.try_into().map_err(|_| Error::Invalid)?,
        ))
        .map_err(|_| Error::Bounds)?
    } else {
        header
            .values
            .checked_mul(usize::from(width))
            .ok_or(Error::Bounds)?
            .div_ceil(8)
    };
    let mut encoded = take(bytes, length)?;
    let mut result = Vec::with_capacity(header.values);
    if header.definition_encoding == 4 {
        for index in 0..header.values {
            let mut level = 0;
            for bit in 0..width {
                let offset = index * usize::from(width) + usize::from(bit);
                level = (level << 1) | ((encoded[offset / 8] >> (7 - offset % 8)) & 1);
            }
            observe(&mut result, level, maximum)?;
        }
        encoded = &[];
    } else {
        while result.len() < header.values {
            let run = unsigned(&mut encoded)?;
            let count = usize::try_from(run >> 1).map_err(|_| Error::Bounds)?;
            if count == 0 || count > i32::MAX as usize {
                return Err(Error::Invalid);
            }
            if run & 1 == 0 {
                if count > header.values - result.len() {
                    return Err(Error::Invalid);
                }
                let level = take(&mut encoded, 1)?[0];
                for _ in 0..count {
                    observe(&mut result, level, maximum)?;
                }
            } else {
                let count = count.checked_mul(8).ok_or(Error::Bounds)?;
                if count > header.values - result.len() + 7 {
                    return Err(Error::Invalid);
                }
                let packed = take(&mut encoded, count * usize::from(width) / 8)?;
                for index in 0..count.min(header.values - result.len()) {
                    let mut level = 0;
                    for bit in 0..width {
                        let offset = index * usize::from(width) + usize::from(bit);
                        level |= ((packed[offset / 8] >> (offset % 8)) & 1) << bit;
                    }
                    observe(&mut result, level, maximum)?;
                }
            }
        }
    }
    if !encoded.is_empty()
        || header
            .nulls
            .is_some_and(|nulls| nulls != result.iter().filter(|present| !**present).count())
    {
        return Err(Error::Invalid);
    }
    Ok(Some(result))
}

pub(super) fn materialized_limit(count: usize, limit: usize) -> Result<usize, Error> {
    count
        .checked_mul(std::mem::size_of::<ColumnValue>())
        .filter(|bytes| *bytes <= limit)
        .ok_or(Error::Bounds)
}

fn observe(result: &mut Vec<bool>, level: u8, maximum: u8) -> Result<(), Error> {
    if level > maximum {
        return Err(Error::Invalid);
    }
    result.push(level == maximum);
    Ok(())
}

fn unsigned(bytes: &mut &[u8]) -> Result<u32, Error> {
    let mut value = 0;
    for shift in (0..35).step_by(7) {
        let byte = take(bytes, 1)?[0];
        if shift == 28 && byte > 15 {
            return Err(Error::Invalid);
        }
        value |= u32::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return Ok(value);
        }
    }
    Err(Error::Invalid)
}

fn take<'data>(bytes: &mut &'data [u8], count: usize) -> Result<&'data [u8], Error> {
    let result = bytes.get(..count).ok_or(Error::Invalid)?;
    *bytes = &bytes[count..];
    Ok(result)
}
