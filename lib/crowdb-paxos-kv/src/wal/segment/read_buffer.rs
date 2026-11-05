// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::io;

use bytes::Bytes;

use crate::wal::WalFile;

const READ_AHEAD: usize = 1024 * 1024;

#[derive(Default)]
pub(super) struct SegmentReadBuffer {
    bytes: Bytes,
    offset: u64,
}

impl SegmentReadBuffer {
    pub(super) async fn read(
        &mut self,
        file: &mut WalFile,
        file_len: u64,
        offset: u64,
        length: usize,
    ) -> io::Result<Bytes> {
        if let Some(start) = offset
            .checked_sub(self.offset)
            .and_then(|start| usize::try_from(start).ok())
        {
            if start <= self.bytes.len() && length <= self.bytes.len() - start {
                return Ok(self.bytes.slice(start..start + length));
            }
        }
        let available = file_len
            .checked_sub(offset)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "WAL read starts past EOF"))?;
        if u64::try_from(length).unwrap_or(u64::MAX) > available {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "WAL read extends past EOF",
            ));
        }
        let read_ahead = usize::try_from(available.min(READ_AHEAD as u64)).expect("bounded read size");
        let mut bytes = vec![0; length.max(read_ahead)];
        file.read_exact_at(&mut bytes, offset).await?;
        self.bytes = Bytes::from(bytes);
        self.offset = offset;
        Ok(self.bytes.slice(..length))
    }
}
