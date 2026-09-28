use std::io::{self, Read};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crate::file::FileReader;

use super::scan::JsonScan;

pub(super) struct JsonReader {
    reader: FileReader,
    handle: tokio::runtime::Handle,
    cancelled: Arc<AtomicBool>,
    frame: Vec<u8>,
    offset: usize,
    scan: JsonScan,
}

impl JsonReader {
    pub(super) fn new(
        reader: FileReader,
        handle: tokio::runtime::Handle,
        cancelled: Arc<AtomicBool>,
        max_depth: usize,
    ) -> Self {
        Self {
            reader,
            handle,
            cancelled,
            frame: Vec::new(),
            offset: 0,
            scan: JsonScan::new(max_depth),
        }
    }

    pub(super) fn finish(&self) -> io::Result<()> {
        self.scan.finish()
    }
}

impl Read for JsonReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::other("JSON sealing cancelled"));
        }
        if self.offset == self.frame.len() {
            self.frame = self
                .handle
                .block_on(self.reader.next())
                .map_err(io::Error::other)?
                .unwrap_or_default();
            self.offset = 0;
            self.scan.push(&self.frame)?;
        }
        let count = output.len().min(self.frame.len() - self.offset);
        output[..count].copy_from_slice(&self.frame[self.offset..self.offset + count]);
        self.offset += count;
        Ok(count)
    }
}
