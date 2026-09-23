use super::PuffinMetadataError;
use lz4_flex::frame::FrameDecoder;
use std::io::Read;

pub(super) fn decode(bytes: &[u8], limit: usize) -> Result<Vec<u8>, PuffinMetadataError> {
    if bytes.len() < 19 || bytes[..4] != [4, 34, 77, 24] || bytes[4] & 0x08 == 0 || bytes[4] & 1 != 0 {
        return Err(PuffinMetadataError::Invalid);
    }
    let declared = u64::from_le_bytes(
        bytes[6..14]
            .try_into()
            .map_err(|_| PuffinMetadataError::Invalid)?,
    );
    if declared > limit as u64 {
        return Err(PuffinMetadataError::Bounds);
    }
    single_frame(bytes)?;
    let mut decoded = Vec::with_capacity(usize::try_from(declared).map_err(|_| PuffinMetadataError::Bounds)?);
    FrameDecoder::new(bytes)
        .take(limit as u64 + 1)
        .read_to_end(&mut decoded)
        .map_err(|_| PuffinMetadataError::Invalid)?;
    if decoded.len() > limit {
        return Err(PuffinMetadataError::Bounds);
    }
    if decoded.len() as u64 != declared {
        return Err(PuffinMetadataError::Invalid);
    }
    Ok(decoded)
}

fn single_frame(bytes: &[u8]) -> Result<(), PuffinMetadataError> {
    let mut offset = 15_usize;
    loop {
        let next = offset.checked_add(4).ok_or(PuffinMetadataError::Invalid)?;
        let length = u32::from_le_bytes(
            bytes
                .get(offset..next)
                .ok_or(PuffinMetadataError::Invalid)?
                .try_into()
                .map_err(|_| PuffinMetadataError::Invalid)?,
        );
        offset = next;
        if length == 0 {
            offset += usize::from(bytes[4] & 4 != 0) * 4;
            return if offset == bytes.len() {
                Ok(())
            } else {
                Err(PuffinMetadataError::Invalid)
            };
        }
        let length = usize::try_from(length & 0x7fff_ffff).map_err(|_| PuffinMetadataError::Invalid)?;
        offset = offset
            .checked_add(length)
            .and_then(|offset| offset.checked_add(usize::from(bytes[4] & 16 != 0) * 4))
            .filter(|offset| *offset <= bytes.len())
            .ok_or(PuffinMetadataError::Invalid)?;
    }
}
