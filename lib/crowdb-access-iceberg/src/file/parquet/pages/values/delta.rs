use super::{bits, take, unsigned, Error};

pub(super) fn integers(bytes: &mut &[u8], count: usize, integer_bits: u8) -> Result<Vec<i64>, Error> {
    let block = usize::try_from(unsigned(bytes)?).map_err(|_| Error::Bounds)?;
    let blocks = usize::try_from(unsigned(bytes)?).map_err(|_| Error::Bounds)?;
    let total = usize::try_from(unsigned(bytes)?).map_err(|_| Error::Bounds)?;
    if block == 0
        || block > 65_536
        || block % 128 != 0
        || blocks == 0
        || blocks > block
        || block % blocks != 0
        || (block / blocks) % 32 != 0
        || total != count
        || total == 0
    {
        return Err(Error::Invalid);
    }
    let mut previous = signed(bytes)?;
    if integer_bits == 32 && i32::try_from(previous).is_err() {
        return Err(Error::Invalid);
    }
    let mut values = vec![previous];
    while values.len() < total {
        let minimum = signed(bytes)?;
        if integer_bits == 32 && i32::try_from(minimum).is_err() {
            return Err(Error::Invalid);
        }
        let widths = take(bytes, blocks)?;
        let per_block = block / blocks;
        for width in widths {
            if values.len() == total {
                break;
            }
            if *width > integer_bits {
                return Err(Error::Invalid);
            }
            let packed = take(bytes, per_block * usize::from(*width) / 8)?;
            for index in 0..per_block.min(total - values.len()) {
                let difference = bits(packed, index * usize::from(*width), *width)?;
                previous = previous
                    .wrapping_add(minimum)
                    .wrapping_add(i64::from_ne_bytes(difference.to_ne_bytes()));
                if integer_bits == 32 {
                    previous = i64::from(i32::from_le_bytes(
                        previous.to_le_bytes()[..4]
                            .try_into()
                            .map_err(|_| Error::Invalid)?,
                    ));
                }
                values.push(previous);
            }
        }
    }
    Ok(values)
}

fn signed(bytes: &mut &[u8]) -> Result<i64, Error> {
    let value = unsigned(bytes)?;
    Ok(i64::try_from(value >> 1).map_err(|_| Error::Invalid)?
        ^ -i64::try_from(value & 1).map_err(|_| Error::Invalid)?)
}
