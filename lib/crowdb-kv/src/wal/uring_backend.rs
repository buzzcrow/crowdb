// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Buffered regular-file WAL backend using a shared `io_uring` owner.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crowdb_tree_ffi::{Uring, UringFile};

use super::io_backend::OpenOptions;

pub(crate) struct UringBackendFile {
    file: UringFile,
    path: PathBuf,
}

impl UringBackendFile {
    pub fn open(ring: &Arc<Uring>, path: &Path, options: &OpenOptions) -> io::Result<Self> {
        Ok(Self {
            file: ring.open(path, &options.to_std())?,
            path: path.to_path_buf(),
        })
    }

    pub async fn write_at(&self, data: &[u8], offset: u64) -> io::Result<usize> {
        self.write_vectored_at(&[io::IoSlice::new(data)], offset).await
    }

    pub async fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        self.file.write_vectored_at(bufs, offset).await
    }

    pub async fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.file.read_at(buf, offset).await
    }

    pub async fn read_exact_at(&self, buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        let mut filled = 0;
        while filled < buf.len() {
            let read = self.file.read_at(&mut buf[filled..], offset).await?;
            if read == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            filled += read;
            offset += read as u64;
        }
        Ok(())
    }

    pub async fn fdatasync(&self) -> io::Result<()> {
        self.file.sync_data().await
    }

    pub async fn fsync(&self) -> io::Result<()> {
        self.file.sync_all().await
    }

    pub fn len(&self) -> io::Result<u64> {
        self.file.metadata().map(|metadata| metadata.len())
    }

    pub fn truncate(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }
}
