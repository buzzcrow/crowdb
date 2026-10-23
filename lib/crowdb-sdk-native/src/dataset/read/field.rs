// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{runtime::RequestLease, Error, Result};
use crowdb_access_dataset::{DatasetUploads, FieldLocator, UploadRecord, VerifiedUploadPart};
use crowdb_rpc_ffi::OwnedPayload;
use md5::{Digest, Md5};

enum Source {
    Inline(Option<OwnedPayload>),
    Upload {
        location: Vec<u8>,
        metadata: Option<UploadRecord>,
        part: u32,
        stream: Option<Box<VerifiedUploadPart>>,
    },
}

pub(super) struct FieldReader {
    pub(super) index: usize,
    pub(super) offset: u64,
    source: Source,
    length: u64,
    expected: [u8; 16],
    digest: Md5,
}

impl FieldReader {
    pub(super) fn new(index: usize, locator: FieldLocator) -> Result<Self> {
        let (source, length, expected) = match locator {
            FieldLocator::Inline { value, md5 } => {
                let length = value.len() as u64;
                (
                    Source::Inline((!value.is_empty()).then(|| OwnedPayload::from(value))),
                    length,
                    md5,
                )
            }
            FieldLocator::Chunk {
                location,
                length,
                md5,
            } => (
                Source::Upload {
                    location,
                    metadata: None,
                    part: 0,
                    stream: None,
                },
                length,
                md5,
            ),
            FieldLocator::Tombstone => return Err(Error::Integrity),
        };
        Ok(Self {
            index,
            offset: 0,
            source,
            length,
            expected,
            digest: Md5::new(),
        })
    }

    pub(super) async fn next(
        &mut self,
        uploads: &DatasetUploads,
        request: &RequestLease,
    ) -> Result<Option<OwnedPayload>> {
        let payload = match &mut self.source {
            Source::Inline(value) => value.take(),
            Source::Upload {
                location,
                metadata,
                part,
                stream,
            } => {
                if metadata.is_none() {
                    let record = request
                        .run(async { Ok(uploads.metadata(location).await?) })
                        .await?;
                    if record.length != self.length || record.md5 != self.expected {
                        return Err(Error::Integrity);
                    }
                    *metadata = Some(record);
                }
                let record = metadata.as_ref().ok_or(Error::Integrity)?;
                loop {
                    if u64::from(*part) >= record.length.div_ceil(u64::from(record.part_size)) {
                        break None;
                    }
                    if stream.is_none() {
                        *stream = Some(
                            request
                                .run(async { Ok(Box::new(uploads.read_part_stream(location, *part).await?)) })
                                .await?,
                        );
                    }
                    let reader = stream.as_mut().ok_or(Error::Integrity)?;
                    if let Some(payload) = request.run(async { Ok(reader.next().await?) }).await? {
                        break Some(payload);
                    }
                    *stream = None;
                    *part += 1;
                }
            }
        };
        if payload.is_none()
            && (self.offset != self.length
                || <[u8; 16]>::from(self.digest.clone().finalize()) != self.expected)
        {
            return Err(Error::Integrity);
        }
        Ok(payload)
    }

    pub(super) fn delivered(&mut self, bytes: &[u8]) -> Result<()> {
        self.offset = self
            .offset
            .checked_add(bytes.len() as u64)
            .ok_or(Error::BufferRange)?;
        if self.offset > self.length {
            return Err(Error::Integrity);
        }
        self.digest.update(bytes);
        Ok(())
    }
}
