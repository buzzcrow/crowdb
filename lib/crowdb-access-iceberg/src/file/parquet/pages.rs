use std::{io::Read, sync::Arc};

use super::{ParquetColumnChunk, ParquetMetadataError as Error, ParquetSchemaElement};
use crate::file::{ByteRange, FileBlockStore, FileReader, FileRecord};

mod header;
mod levels;
#[cfg(feature = "test-util")]
mod testing;
mod values;
use header::Header;
#[cfg(feature = "test-util")]
pub use testing::{
    read_parquet_integer_column_for_tests, read_parquet_nullable_integer_column_for_tests,
    read_parquet_scalar_column_for_tests,
};
pub(crate) use values::ColumnValue as ParquetColumnValue;
use values::{decode, ColumnValue, Physical};

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
    physical: Physical,
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
        field: &ParquetSchemaElement,
        limits: ParquetPageLimits,
    ) -> Result<Self, Error> {
        if column.repeated {
            return Err(Error::Unsupported);
        }
        if limits.bytes == 0
            || column.definition_level > 32
            || limits.bytes > 8 * 1024 * 1024
            || limits.values == 0
            || limits.values > 1_048_576
            || limits.pages == 0
            || limits.pages > 1_000_000
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
            physical: Physical::new(field)?,
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
            let values = self.decode_values(&header, &payload)?;
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

    fn decode_values(&self, header: &Header, mut payload: &[u8]) -> Result<Vec<ColumnValue>, Error> {
        levels::materialized_limit(header.values, self.limits.bytes)?;
        let presence = if header.kind == 2 {
            None
        } else {
            levels::presence(&mut payload, header, self.column.definition_level)?
        };
        let count = presence.as_ref().map_or(header.values, |levels| {
            levels.iter().filter(|value| **value).count()
        });
        let null_bytes = levels::materialized_limit(header.values - count, self.limits.bytes)?;
        let mut values = decode(
            payload,
            header.encoding,
            self.physical,
            count,
            self.dictionary.as_deref(),
            self.limits.bytes - null_bytes,
        )?;
        let Some(presence) = presence else {
            return Ok(values);
        };
        let mut remaining = values.len();
        values.resize_with(header.values, || ColumnValue::Null);
        for (index, present) in presence.into_iter().enumerate().rev() {
            if present {
                remaining = remaining.checked_sub(1).ok_or(Error::Invalid)?;
                let value = std::mem::replace(&mut values[remaining], ColumnValue::Null);
                values[index] = value;
            }
        }
        Ok(values)
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
        let prefix = header.level_bytes;
        let data = decompress(
            &payload[prefix..],
            header.decoded - prefix,
            if header.is_compressed {
                self.column.compression
            } else {
                0
            },
        )?;
        let decoded = if prefix == 0 {
            data
        } else {
            let mut decoded = Vec::with_capacity(header.decoded);
            decoded.extend_from_slice(&payload[..prefix]);
            decoded.extend(data);
            decoded
        };
        self.offset = payload_end;
        Ok((header, decoded))
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
