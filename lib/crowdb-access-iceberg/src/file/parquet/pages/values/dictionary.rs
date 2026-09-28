use super::{bits, push_copy, take, unsigned, ColumnValue, Error};

pub(super) fn decode(
    bytes: &mut &[u8],
    width: u8,
    count: usize,
    dictionary: &[ColumnValue],
    mut remaining: usize,
) -> Result<Vec<ColumnValue>, Error> {
    if width > 32 {
        return Err(Error::Invalid);
    }
    let mut result = Vec::new();
    while result.len() < count {
        let header = unsigned(bytes)?;
        let run = usize::try_from(header >> 1)
            .ok()
            .filter(|value| *value > 0 && i32::try_from(*value).is_ok())
            .ok_or(Error::Invalid)?;
        if header & 1 == 0 {
            if run > count - result.len() {
                return Err(Error::Invalid);
            }
            let packed = take(bytes, usize::from(width).div_ceil(8))?;
            if width % 8 != 0 && packed.last().is_some_and(|byte| *byte >> (width % 8) != 0) {
                return Err(Error::Invalid);
            }
            let id = bits(packed, 0, width)?;
            let value = dictionary
                .get(usize::try_from(id).map_err(|_| Error::Invalid)?)
                .ok_or(Error::Invalid)?;
            for _ in 0..run {
                push_copy(&mut result, value, &mut remaining)?;
            }
        } else {
            let run = run.checked_mul(8).ok_or(Error::Invalid)?;
            if run > count - result.len() + 7 {
                return Err(Error::Invalid);
            }
            let packed = take(
                bytes,
                run.checked_mul(usize::from(width)).ok_or(Error::Invalid)? / 8,
            )?;
            for index in 0..run.min(count - result.len()) {
                let id = bits(packed, index * usize::from(width), width)?;
                let value = dictionary
                    .get(usize::try_from(id).map_err(|_| Error::Invalid)?)
                    .ok_or(Error::Invalid)?;
                push_copy(&mut result, value, &mut remaining)?;
            }
        }
    }
    Ok(result)
}
