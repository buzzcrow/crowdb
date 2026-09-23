use std::{io::Read, sync::Arc};

use super::{compact, ParquetColumnChunk, ParquetMetadataError as Error, ParquetMetadataLimits};
use crate::file::{ByteRange, FileBlockStore, FileReader, FileRecord};

mod values;
pub(crate) use values::ColumnValue as ParquetColumnValue;
use values::{decode, ColumnValue};

#[derive(Clone, Copy, Debug)]
pub struct ParquetPageLimits {
    pub bytes: usize,
    pub values: usize,
    pub pages: usize,
}

pub(crate) struct ParquetColumnReader {
    store: Arc<dyn FileBlockStore>,
    record: FileRecord,
    column: ParquetColumnChunk,
    physical: i32,
    limits: ParquetPageLimits,
    offset: u64,
    seen: u64,
    pages: usize,
    dictionary: Option<Vec<ColumnValue>>,
    values: std::vec::IntoIter<ColumnValue>,
}

impl ParquetColumnReader {
    pub(crate) fn new(
        store: Arc<dyn FileBlockStore>,
        record: &FileRecord,
        column: &ParquetColumnChunk,
        physical: i32,
        limits: ParquetPageLimits,
    ) -> Result<Self, Error> {
        if limits.bytes == 0
            || limits.bytes > 8 * 1024 * 1024
            || limits.values == 0
            || limits.values > 1_048_576
            || limits.pages == 0
            || limits.pages > 1_000_000
            || !matches!(physical, 2 | 6)
            || column
                .offset
                .checked_add(column.length)
                .filter(|end| *end <= record.length)
                .is_none()
        {
            return Err(Error::Bounds);
        }
        Ok(Self {
            store,
            record: record.clone(),
            column: column.clone(),
            physical,
            limits,
            offset: column.offset,
            seen: 0,
            pages: 0,
            dictionary: None,
            values: vec![].into_iter(),
        })
    }

    pub(crate) async fn next(&mut self) -> Result<Option<ColumnValue>, Error> {
        loop {
            if let Some(value) = self.values.next() {
                return Ok(Some(value));
            }
            if self.offset == self.column.offset + self.column.length {
                return if self.seen == self.column.values {
                    Ok(None)
                } else {
                    Err(Error::Invalid)
                };
            }
            if self.pages >= self.limits.pages {
                return Err(Error::Bounds);
            }
            self.pages += 1;
            let start = self.offset;
            let (header, payload) = self.page().await?;
            let values = decode(
                &payload,
                header.encoding,
                self.physical,
                header.values,
                self.dictionary.as_deref(),
                self.limits.bytes,
            )?;
            if header.kind == 2 {
                if self.dictionary.is_some()
                    || self.seen != 0
                    || start != self.column.offset
                    || start >= self.column.data_offset
                {
                    return Err(Error::Invalid);
                }
                self.dictionary = Some(values);
            } else {
                if self.seen == 0 && start != self.column.data_offset {
                    return Err(Error::Invalid);
                }
                self.seen = self
                    .seen
                    .checked_add(header.values as u64)
                    .filter(|seen| *seen <= self.column.values)
                    .ok_or(Error::Invalid)?;
                self.values = values.into_iter();
            }
        }
    }

    async fn page(&mut self) -> Result<(Header, Vec<u8>), Error> {
        let end = self.column.offset + self.column.length;
        let bytes = read(
            self.store.clone(),
            &self.record,
            self.offset,
            end.min(self.offset.saturating_add(64 * 1024)),
        )
        .await?;
        let (header, consumed) = Header::decode(&bytes, self.limits)?;
        let payload_start = self.offset.checked_add(consumed as u64).ok_or(Error::Invalid)?;
        let payload_end = payload_start
            .checked_add(header.compressed as u64)
            .filter(|value| *value <= end)
            .ok_or(Error::Invalid)?;
        let payload = read(self.store.clone(), &self.record, payload_start, payload_end).await?;
        if header.crc.is_some_and(|crc| crc32fast::hash(&payload) != crc) {
            return Err(Error::Invalid);
        }
        let decoded = decompress(
            &payload,
            header.decoded,
            if header.is_compressed {
                self.column.compression
            } else {
                0
            },
        )?;
        self.offset = payload_end;
        Ok((header, decoded))
    }
}

struct Header {
    kind: i64,
    values: usize,
    encoding: i64,
    compressed: usize,
    decoded: usize,
    is_compressed: bool,
    crc: Option<u32>,
}

impl Header {
    fn decode(bytes: &[u8], limits: ParquetPageLimits) -> Result<(Self, usize), Error> {
        let (value, consumed) = compact::prefix(
            bytes,
            ParquetMetadataLimits {
                footer_bytes: 64 * 1024,
                values: 1000,
                depth: 8,
                schema_elements: 1,
                row_groups: 1,
            },
        )?;
        let fields = value.fields()?;
        let number = |id| fields.get(&id).ok_or(Error::Invalid)?.integer(5);
        let kind = number(1)?;
        let decoded = usize::try_from(number(2)?).map_err(|_| Error::Invalid)?;
        let compressed = usize::try_from(number(3)?).map_err(|_| Error::Invalid)?;
        if decoded > limits.bytes || compressed > limits.bytes {
            return Err(Error::Bounds);
        }
        let detail = fields
            .get(&match kind {
                0 => 5,
                2 => 7,
                3 => 8,
                _ => return Err(Error::Unsupported),
            })
            .ok_or(Error::Invalid)?
            .fields()?;
        let item = |id| detail.get(&id).ok_or(Error::Invalid)?.integer(5);
        let values = usize::try_from(item(1)?).map_err(|_| Error::Invalid)?;
        if values == 0 || values > limits.values {
            return Err(Error::Bounds);
        }
        let encoding = item(if kind == 3 { 4 } else { 2 })?;
        if kind == 2 && !matches!(encoding, 0 | 2) {
            return Err(Error::Invalid);
        }
        if kind == 0 && (!matches!(item(3)?, 3 | 4) || !matches!(item(4)?, 3 | 4)) {
            return Err(Error::Invalid);
        }
        let is_compressed = if kind == 3 {
            if item(2)? != 0
                || item(3)? != i64::try_from(values).map_err(|_| Error::Bounds)?
                || item(5)? != 0
                || item(6)? != 0
            {
                return Err(Error::Invalid);
            }
            detail
                .get(&7)
                .map(compact::Value::boolean)
                .transpose()?
                .unwrap_or(true)
        } else {
            true
        };
        let crc = fields
            .get(&4)
            .map(|value| {
                let value = i32::try_from(value.integer(5)?).map_err(|_| Error::Invalid)?;
                Ok::<_, Error>(u32::from_ne_bytes(value.to_ne_bytes()))
            })
            .transpose()?;
        Ok((
            Self {
                kind,
                values,
                encoding: if kind == 2 { 0 } else { encoding },
                compressed,
                decoded,
                is_compressed,
                crc,
            },
            consumed,
        ))
    }
}

async fn read(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, Error> {
    let mut reader = FileReader::new(store, record.clone(), Some(ByteRange { start, end }), 16 * 1024)?;
    let mut bytes = Vec::new();
    while let Some(frame) = reader.next().await? {
        bytes.extend(frame);
    }
    Ok(bytes)
}

fn decompress(bytes: &[u8], decoded: usize, codec: i32) -> Result<Vec<u8>, Error> {
    let mut output = vec![0; decoded];
    match codec {
        0 if bytes.len() == decoded => output.copy_from_slice(bytes),
        1 => {
            if snap::raw::decompress_len(bytes).map_err(|_| Error::Invalid)? != decoded
                || snap::raw::Decoder::new()
                    .decompress(bytes, &mut output)
                    .map_err(|_| Error::Invalid)?
                    != decoded
            {
                return Err(Error::Invalid);
            }
        }
        7 => {
            if lz4_flex::block::decompress_into(bytes, &mut output).map_err(|_| Error::Invalid)? != decoded {
                return Err(Error::Invalid);
            }
        }
        2 => {
            let stream = flate2::read::MultiGzDecoder::new(bytes);
            return bounded_decode(stream, decoded);
        }
        6 => {
            let mut stream = zstd::stream::read::Decoder::with_buffer(bytes).map_err(|_| Error::Invalid)?;
            stream.window_log_max(23).map_err(|_| Error::Bounds)?;
            return bounded_decode(stream, decoded);
        }
        0 => return Err(Error::Invalid),
        _ => return Err(Error::Unsupported),
    }
    Ok(output)
}

fn bounded_decode(reader: impl Read, length: usize) -> Result<Vec<u8>, Error> {
    let mut output = Vec::new();
    reader
        .take(length as u64 + 1)
        .read_to_end(&mut output)
        .map_err(|_| Error::Invalid)?;
    if output.len() != length {
        return Err(Error::Invalid);
    }
    Ok(output)
}
