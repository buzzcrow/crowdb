use super::{integer, push, take, ColumnValue, Error, Physical};

pub(super) fn plain(
    bytes: &mut &[u8],
    physical: Physical,
    count: usize,
    mut remaining: usize,
) -> Result<Vec<ColumnValue>, Error> {
    let mut result = Vec::new();
    if physical.kind == 0 {
        let packed = take(bytes, count.div_ceil(8))?;
        for index in 0..count {
            push(
                &mut result,
                ColumnValue::Boolean((packed[index / 8] >> (index % 8)) & 1 != 0),
                &mut remaining,
            )?;
        }
        return Ok(result);
    }
    for _ in 0..count {
        let length = if physical.kind == 6 {
            usize::try_from(i32::from_le_bytes(
                take(bytes, 4)?.try_into().map_err(|_| Error::Invalid)?,
            ))
            .map_err(|_| Error::Invalid)?
        } else {
            physical.width
        };
        if matches!(physical.kind, 6 | 7)
            && length > remaining.saturating_sub(std::mem::size_of::<ColumnValue>())
        {
            return Err(Error::Bounds);
        }
        push(
            &mut result,
            value(take(bytes, length)?, physical.kind)?,
            &mut remaining,
        )?;
    }
    Ok(result)
}

pub(super) fn split(
    bytes: &[u8],
    physical: Physical,
    count: usize,
    mut remaining: usize,
) -> Result<Vec<ColumnValue>, Error> {
    if bytes.len() != count.checked_mul(physical.width).ok_or(Error::Invalid)? {
        return Err(Error::Invalid);
    }
    let mut result = Vec::new();
    let mut fixed = [0; 8];
    let mut dynamic;
    let buffer = if physical.width <= fixed.len() {
        &mut fixed[..physical.width]
    } else {
        dynamic = vec![0; physical.width];
        dynamic.as_mut_slice()
    };
    for index in 0..count {
        for (stream, value) in buffer.iter_mut().enumerate() {
            *value = bytes[stream * count + index];
        }
        if physical.kind == 7 && physical.width > remaining.saturating_sub(std::mem::size_of::<ColumnValue>())
        {
            return Err(Error::Bounds);
        }
        push(&mut result, value(buffer, physical.kind)?, &mut remaining)?;
    }
    Ok(result)
}

fn value(bytes: &[u8], kind: i32) -> Result<ColumnValue, Error> {
    Ok(match kind {
        1 | 2 => integer(bytes)?,
        4 => ColumnValue::Float(u32::from_le_bytes(bytes.try_into().map_err(|_| Error::Invalid)?)),
        5 => ColumnValue::Double(u64::from_le_bytes(bytes.try_into().map_err(|_| Error::Invalid)?)),
        6 | 7 => ColumnValue::Bytes(bytes.to_vec()),
        _ => return Err(Error::Unsupported),
    })
}
