// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Safe owner for the standalone buffered-file io_uring C ABI.

use std::ffi::c_void;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::sys;

type Completion = (oneshot::Sender<i32>, Arc<AtomicBool>);

unsafe extern "C" fn complete(context: *mut c_void, result: i32) {
    let completion = unsafe { Box::from_raw(context.cast::<Completion>()) };
    completion.1.store(true, Ordering::Release);
    let _ = completion.0.send(result);
}

struct CompletionGuard {
    done: Arc<AtomicBool>,
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        while !self.done.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
    }
}

fn completion() -> (oneshot::Receiver<i32>, CompletionGuard, *mut c_void) {
    let (tx, rx) = oneshot::channel();
    let done = Arc::new(AtomicBool::new(false));
    let context = Box::into_raw(Box::new((tx, Arc::clone(&done)))).cast();
    (rx, CompletionGuard { done }, context)
}

fn result_to_io(result: i32) -> io::Result<usize> {
    if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else {
        Ok(result as usize)
    }
}

/// One process-local, single-pipeline io_uring engine.
pub struct Uring {
    raw: *mut sys::ct_uring,
}

unsafe impl Send for Uring {}
unsafe impl Sync for Uring {}

impl Uring {
    /// Construct a ring, or report that liburing/kernel support is unavailable.
    ///
    /// # Errors
    /// Returns `Unsupported` when the native ring cannot be initialized.
    pub fn new(entries: u32) -> io::Result<Arc<Self>> {
        let raw = unsafe { sys::ct_uring_create(entries) };
        if raw.is_null() {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring is unavailable or ring setup was rejected",
            ))
        } else {
            Ok(Arc::new(Self { raw }))
        }
    }

    /// Open and register one buffered regular file.
    ///
    /// # Errors
    /// Returns filesystem or ring registration errors.
    pub fn open(self: &Arc<Self>, path: &Path, options: &std::fs::OpenOptions) -> io::Result<UringFile> {
        let file = options.open(path)?;
        let fd = file.as_raw_fd();
        let result = unsafe { sys::ct_uring_register_fd(self.raw, fd) };
        result_to_io(result)?;
        Ok(UringFile {
            ring: Arc::clone(self),
            file,
        })
    }
}

impl Drop for Uring {
    fn drop(&mut self) {
        unsafe { sys::ct_uring_destroy(self.raw) };
    }
}

/// Buffered regular file registered with a [`Uring`].
pub struct UringFile {
    ring: Arc<Uring>,
    file: std::fs::File,
}

impl UringFile {
    fn fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }

    /// Positional read. Cancellation waits for the native CQE before releasing `buf`.
    ///
    /// # Errors
    /// Returns the CQE errno or a completion-channel failure.
    pub async fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let (rx, guard, context) = completion();
        unsafe {
            sys::ct_uring_submit_read(
                self.ring.raw,
                self.fd(),
                buf.as_mut_ptr(),
                buf.len(),
                offset,
                complete,
                context,
            );
        }
        let result = rx
            .await
            .map_err(|_| io::Error::other("io_uring completion channel closed"))?;
        drop(guard);
        result_to_io(result)
    }

    /// Positional vectored write. Cancellation waits for the CQE before
    /// releasing the borrowed buffers.
    ///
    /// # Errors
    /// Returns the CQE errno or a completion-channel failure.
    pub async fn write_vectored_at(&self, bufs: &[std::io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        let lengths: Vec<usize> = bufs.iter().map(|buf| buf.len()).collect();
        let (rx, guard, context) = completion();
        {
            // The C++ submission copies the iovec descriptors synchronously;
            // only the buffers themselves remain borrowed until the CQE.
            let bases: Vec<*const u8> = bufs.iter().map(|buf| buf.as_ptr()).collect();
            unsafe {
                sys::ct_uring_submit_writev(
                    self.ring.raw,
                    self.fd(),
                    bases.as_ptr(),
                    lengths.as_ptr(),
                    bases.len(),
                    offset,
                    complete,
                    context,
                );
            }
        }
        let result = rx
            .await
            .map_err(|_| io::Error::other("io_uring completion channel closed"))?;
        drop(guard);
        result_to_io(result)
    }

    async fn sync(&self, data_only: bool) -> io::Result<()> {
        let (rx, guard, context) = completion();
        unsafe {
            sys::ct_uring_submit_sync(self.ring.raw, self.fd(), i32::from(data_only), complete, context);
        }
        let result = rx
            .await
            .map_err(|_| io::Error::other("io_uring completion channel closed"))?;
        drop(guard);
        result_to_io(result).map(|_| ())
    }

    /// Flush file data.
    ///
    /// # Errors
    /// Returns the CQE errno.
    pub async fn sync_data(&self) -> io::Result<()> {
        self.sync(true).await
    }

    /// Flush file data and metadata.
    ///
    /// # Errors
    /// Returns the CQE errno.
    pub async fn sync_all(&self) -> io::Result<()> {
        self.sync(false).await
    }

    /// Return current regular-file metadata.
    ///
    /// # Errors
    /// Returns a filesystem metadata error.
    pub fn metadata(&self) -> io::Result<std::fs::Metadata> {
        self.file.metadata()
    }

    /// Change the file length.
    ///
    /// # Errors
    /// Returns a filesystem error.
    pub fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
}

impl Drop for UringFile {
    fn drop(&mut self) {
        unsafe { sys::ct_uring_unregister_fd(self.ring.raw, self.fd()) };
    }
}
