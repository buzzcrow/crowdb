use std::sync::Arc;

use super::{
    read_fixed, ByteRange, ContentFormat, FileBlockStore, FileReader, FileRecord, FormatHint,
    FormatProbeError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrcFooter {
    pub footer: FormatHint,
    pub postscript: FormatHint,
    pub metadata_length: u64,
    pub compression: u64,
    pub compression_block_size: u64,
}

/// Resolves ORC footer framing from a postscript of at most 255 bytes.
/// Footer protobuf, compression and stripe semantics are not decoded here.
/// # Errors
/// Rejects malformed protobuf framing, missing footer size and escaped bounds.
pub async fn probe_orc_footer(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
) -> Result<OrcFooter, FormatProbeError> {
    if record.format != ContentFormat::Orc
        || record.length < 5
        || &read_fixed::<3>(store.clone(), record, 0).await? != b"ORC"
    {
        return Err(FormatProbeError::Container);
    }
    let length = u64::from(read_fixed::<1>(store.clone(), record, record.length - 1).await?[0]);
    let offset = (record.length - 1)
        .checked_sub(length)
        .filter(|offset| *offset >= 3 && length > 0)
        .ok_or(FormatProbeError::Container)?;
    let mut reader = FileReader::new(
        store,
        record.clone(),
        Some(ByteRange {
            start: offset,
            end: record.length - 1,
        }),
        255,
    )?;
    let mut bytes = Vec::with_capacity(255);
    while let Some(frame) = reader.next().await? {
        bytes.extend_from_slice(&frame);
    }
    let fields = Postscript::parse(&bytes)?;
    let footer_length = fields
        .footer
        .filter(|length| *length > 0)
        .ok_or(FormatProbeError::Container)?;
    let footer_offset = offset
        .checked_sub(footer_length)
        .ok_or(FormatProbeError::Container)?;
    if footer_offset
        .checked_sub(fields.metadata)
        .map_or(true, |start| start < 3)
    {
        return Err(FormatProbeError::Container);
    }
    Ok(OrcFooter {
        footer: FormatHint {
            offset: footer_offset,
            length: footer_length,
        },
        postscript: FormatHint { offset, length },
        metadata_length: fields.metadata,
        compression: fields.compression,
        compression_block_size: fields.block_size,
    })
}

#[derive(Default)]
struct Postscript {
    footer: Option<u64>,
    metadata: u64,
    compression: u64,
    block_size: u64,
}

impl Postscript {
    fn parse(mut bytes: &[u8]) -> Result<Self, FormatProbeError> {
        let mut result = Self::default();
        while !bytes.is_empty() {
            let tag = varint(&mut bytes)?;
            let field = tag >> 3;
            let wire = tag & 7;
            if field == 0 || field > 0x1fff_ffff {
                return Err(FormatProbeError::Container);
            }
            match field {
                1 | 2 | 3 | 5 => {
                    if wire != 0 {
                        return Err(FormatProbeError::Container);
                    }
                    let value = varint(&mut bytes)?;
                    match field {
                        1 => result.footer = Some(value),
                        2 => result.compression = value,
                        3 => result.block_size = value,
                        _ => result.metadata = value,
                    }
                }
                8000 => {
                    if wire != 2 {
                        return Err(FormatProbeError::Container);
                    }
                    let length = varint(&mut bytes)?;
                    if take(&mut bytes, length)? != b"ORC" {
                        return Err(FormatProbeError::Container);
                    }
                }
                _ => skip(&mut bytes, wire)?,
            }
        }
        Ok(result)
    }
}

fn varint(bytes: &mut &[u8]) -> Result<u64, FormatProbeError> {
    let mut result = 0;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.first().ok_or(FormatProbeError::Container)?;
        *bytes = &bytes[1..];
        if shift == 63 && byte > 1 {
            return Err(FormatProbeError::Container);
        }
        result |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return Ok(result);
        }
    }
    Err(FormatProbeError::Container)
}

fn take<'a>(bytes: &mut &'a [u8], length: u64) -> Result<&'a [u8], FormatProbeError> {
    let length = usize::try_from(length).map_err(|_| FormatProbeError::Container)?;
    let result = bytes.get(..length).ok_or(FormatProbeError::Container)?;
    *bytes = &bytes[length..];
    Ok(result)
}

fn skip(bytes: &mut &[u8], wire: u64) -> Result<(), FormatProbeError> {
    match wire {
        0 => {
            varint(bytes)?;
        }
        1 => {
            take(bytes, 8)?;
        }
        2 => {
            let length = varint(bytes)?;
            take(bytes, length)?;
        }
        5 => {
            take(bytes, 4)?;
        }
        _ => return Err(FormatProbeError::Container),
    }
    Ok(())
}
