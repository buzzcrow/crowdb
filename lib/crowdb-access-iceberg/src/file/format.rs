use std::sync::Arc;

use super::{ByteRange, ContentFormat, FileBlockStore, FileIoError, FileReader, FileRecord, FormatHint};

#[derive(Debug, thiserror::Error)]
pub enum FormatProbeError {
    #[error(transparent)]
    Storage(#[from] FileIoError),
    #[error("invalid or unsupported file container")]
    Container,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PuffinFooter {
    pub payload: FormatHint,
    pub compressed: bool,
}

/// Locates Puffin footer payload without decoding or allocating the footer.
/// This validates framing only, not JSON, compression or blob descriptors.
/// # Errors
/// Rejects invalid magic, reserved flags, nonpositive lengths and escaped bounds.
pub async fn probe_puffin_footer(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
) -> Result<PuffinFooter, FormatProbeError> {
    if record.format != ContentFormat::Puffin || record.length < 21 {
        return Err(FormatProbeError::Container);
    }
    let header = read_fixed::<4>(store.clone(), record, 0).await?;
    let trailer = read_fixed::<12>(store.clone(), record, record.length - 12).await?;
    if &header != b"PFA1" || &trailer[8..] != b"PFA1" || trailer[4] & !1 != 0 || trailer[5..8] != [0; 3] {
        return Err(FormatProbeError::Container);
    }
    let length = i32::from_le_bytes(trailer[..4].try_into().map_err(|_| FormatProbeError::Container)?);
    let length = u64::try_from(length)
        .ok()
        .filter(|length| *length > 0)
        .ok_or(FormatProbeError::Container)?;
    let offset = (record.length - 12)
        .checked_sub(length)
        .filter(|offset| *offset >= 8)
        .ok_or(FormatProbeError::Container)?;
    if &read_fixed::<4>(store, record, offset - 4).await? != b"PFA1" {
        return Err(FormatProbeError::Container);
    }
    Ok(PuffinFooter {
        payload: FormatHint { offset, length },
        compressed: trailer[4] == 1,
    })
}

/// Locates plaintext Parquet metadata from canonical bytes, ignoring stored hints.
/// This checks container framing, not Thrift metadata or data-page semantics.
/// # Errors
/// Rejects unsupported formats, invalid magic, empty or out-of-file metadata.
pub async fn probe_parquet_footer(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
) -> Result<FormatHint, FormatProbeError> {
    if record.format != ContentFormat::Parquet || record.length < 13 {
        return Err(FormatProbeError::Container);
    }
    let header = read_fixed::<4>(store.clone(), record, 0).await?;
    let trailer = read_fixed::<8>(store, record, record.length - 8).await?;
    if &header != b"PAR1" || &trailer[4..] != b"PAR1" {
        return Err(FormatProbeError::Container);
    }
    let length = u64::from(u32::from_le_bytes(
        trailer[..4].try_into().map_err(|_| FormatProbeError::Container)?,
    ));
    let offset = record
        .length
        .checked_sub(8)
        .and_then(|end| end.checked_sub(length))
        .filter(|offset| *offset >= 4 && length > 0)
        .ok_or(FormatProbeError::Container)?;
    Ok(FormatHint { offset, length })
}

async fn read_fixed<const SIZE: usize>(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
    start: u64,
) -> Result<[u8; SIZE], FormatProbeError> {
    let end = start
        .checked_add(SIZE as u64)
        .ok_or(FormatProbeError::Container)?;
    let mut reader = FileReader::new(store, record.clone(), Some(ByteRange { start, end }), SIZE)?;
    let mut result = [0; SIZE];
    let mut filled = 0;
    while let Some(bytes) = reader.next().await? {
        let next = filled + bytes.len();
        result
            .get_mut(filled..next)
            .ok_or(FormatProbeError::Container)?
            .copy_from_slice(&bytes);
        filled = next;
    }
    if filled != SIZE {
        return Err(FormatProbeError::Container);
    }
    Ok(result)
}
