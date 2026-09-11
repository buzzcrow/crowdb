// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::ptr::NonNull;
use std::sync::Arc;

use crate::error::{check, CtError};
use crate::sys;

/// Owning handle for a backend injected into [`Options`]. The C++ tree retains
/// the underlying backend independently when opened, so this handle may be
/// dropped immediately after `Crowdbtree::open` returns.
pub struct PageStore {
    pub(crate) ptr: NonNull<sys::ct_page_store>,
}

impl PageStore {
    /// Create an in-memory injected backend. Chunk-backed construction is
    /// provided by the chunk-KV integration crate so local binaries do not
    /// reference its archive member.
    pub fn open_mem(iu_size: u32) -> Result<Self, CtError> {
        let mut out = std::ptr::null_mut();
        check(unsafe { sys::ct_page_store_open_mem(iu_size, &mut out) })?;
        Ok(Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
        })
    }

    pub(crate) fn as_ptr(&self) -> *mut sys::ct_page_store {
        self.ptr.as_ptr()
    }
}

impl std::fmt::Debug for PageStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageStore").finish_non_exhaustive()
    }
}

unsafe impl Send for PageStore {}
unsafe impl Sync for PageStore {}

impl Drop for PageStore {
    fn drop(&mut self) {
        unsafe { sys::ct_page_store_free(self.ptr.as_ptr()) };
    }
}

/// Compression selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Lz4,
}

/// Durable backend selection, mirrors `ct_options::backend`.
/// Ignored when `Options::path` is `None` (in-memory).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PageStoreBackend {
    /// File-based page store, no alignment.
    #[default]
    File,
    /// Block device: 4K aligned, `O_DIRECT` for a real SSD/SCM
    /// deployment target.
    Block,
    /// Mem block device: in-memory, no alignment.
    MemBlock,
}

/// Durability barrier policy, mirrors `ct_sync_mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// fdatasync after every flush (default, production).
    #[default]
    Full,
    /// No fsync (tests/CI only).
    Skip,
    /// fsync once per snapshot commit.
    Batch,
}

impl SyncMode {
    pub(crate) fn as_u8(self) -> u8 {
        match self {
            Self::Full => 0,
            Self::Skip => 1,
            Self::Batch => 2,
        }
    }
}

/// Immutable half-open key policy selected when a tree is created. `None`
/// endpoints are explicitly unbounded; `Some(Vec::new())` is the empty key.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum KeyRange {
    #[default]
    Unbounded,
    Bounded {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    },
}

/// Engine configuration. `path = None` selects an in-memory store.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Optional injected durable backend. When set, `path` and `backend` are
    /// ignored by the C++ constructor.
    pub page_store: Option<Arc<PageStore>>,
    pub key_range: KeyRange,
    pub path: Option<String>,
    pub iu_size: u32,
    pub frame_bytes: u32,
    pub buffer_pool_bytes: u64,
    pub compression_lz4: bool,
    pub max_inline_value: u64,
    pub backend: PageStoreBackend,
    /// Block size for array-of-blocks mode (0 = default 64 MiB).
    pub block_size: u64,
    /// Store ID for block file naming.
    pub store_id: u32,
    /// Group ID, maps to PxGroupId in CrowDB.
    pub group_id: u32,
    /// Durability barrier policy.
    pub sync_mode: SyncMode,
    /// C++ engine log directory (empty = no file logging).
    pub log_dir: String,
    /// spdlog level name ("info", "debug", etc.).
    pub log_level: String,
    /// C++ log filename prefix (empty = "crowdb-tree").
    pub log_file_prefix: String,
    /// Max C++ log file size in MiB before rotation.
    pub log_max_file_mb: usize,
    /// Number of rotated C++ log files to keep.
    pub log_max_files: usize,
}
