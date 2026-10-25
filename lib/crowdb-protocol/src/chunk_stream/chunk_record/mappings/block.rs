// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{Invalid, StreamChunkRecordError, BLOCK_ENTRIES};

pub(super) fn encode_block(lengths: &[u32], bytes: &mut Vec<u8>) -> Result<(), StreamChunkRecordError> {
    if lengths.is_empty() || lengths.len() > BLOCK_ENTRIES || lengths.contains(&0) {
        return Err(Invalid("mapping block lengths"));
    }
    bytes.extend_from_slice(
        &u16::try_from(lengths.len())
            .map_err(|_| Invalid("mapping block count"))?
            .to_le_bytes(),
    );
    if lengths.iter().all(|length| *length == lengths[0]) {
        bytes.push(0);
        bytes.extend_from_slice(&lengths[0].to_le_bytes());
    } else if lengths
        .iter()
        .map(|length| ((32 - length.leading_zeros()) as usize).div_ceil(7))
        .sum::<usize>()
        < lengths.len() * 4
    {
        bytes.push(1);
        for length in lengths {
            let mut value = *length;
            while value >= 128 {
                bytes.push(value.to_le_bytes()[0] | 128);
                value >>= 7;
            }
            bytes.push(value.to_le_bytes()[0]);
        }
    } else {
        bytes.push(2);
        for length in lengths {
            bytes.extend_from_slice(&length.to_le_bytes());
        }
    }
    Ok(())
}

pub(super) fn decode_block(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u32>, StreamChunkRecordError> {
    let header = bytes
        .get(*cursor..cursor.checked_add(3).ok_or(Invalid("mapping cursor overflow"))?)
        .ok_or(Invalid("truncated mapping block"))?;
    *cursor += 3;
    let count = usize::from(u16::from_le_bytes([header[0], header[1]]));
    if count == 0 || count > BLOCK_ENTRIES {
        return Err(Invalid("mapping block count"));
    }
    let mut lengths = Vec::with_capacity(count);
    match header[2] {
        0 => lengths.resize(count, read_u32(bytes, cursor)?),
        1 => {
            for _ in 0..count {
                lengths.push(read_varint(bytes, cursor)?);
            }
        }
        2 => {
            for _ in 0..count {
                lengths.push(read_u32(bytes, cursor)?);
            }
        }
        _ => return Err(Invalid("mapping encoding")),
    }
    if lengths.contains(&0) {
        return Err(Invalid("empty append"));
    }
    Ok(lengths)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, StreamChunkRecordError> {
    let value = bytes
        .get(*cursor..cursor.checked_add(4).ok_or(Invalid("mapping cursor overflow"))?)
        .ok_or(Invalid("truncated mapping length"))?;
    *cursor += 4;
    Ok(u32::from_le_bytes(
        value.try_into().map_err(|_| Invalid("mapping length"))?,
    ))
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u32, StreamChunkRecordError> {
    let mut result = 0;
    for ordinal in 0..5 {
        let value = *bytes.get(*cursor).ok_or(Invalid("truncated mapping varint"))?;
        *cursor += 1;
        if ordinal == 4 && value > 15 {
            return Err(Invalid("mapping varint overflow"));
        }
        result |= u32::from(value & 127) << (ordinal * 7);
        if value < 128 {
            if ordinal > 0 && value == 0 {
                return Err(Invalid("noncanonical mapping varint"));
            }
            return Ok(result);
        }
    }
    Err(Invalid("mapping varint overflow"))
}
