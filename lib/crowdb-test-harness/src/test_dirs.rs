// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Project-local runtime namespaces — no system temp folder.
//!
//! All generated data, config, logs, coordination state, and artifacts live
//! under `<workspace_root>/.crowdb-runtime/`.
//!
//! [`TestDir`] is a drop guard that auto-deletes on success but preserves
//! the directory when the test panics (via `std::thread::panicking()`),
//! so failed-test data is always available for debugging.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crowdb_protocol::port::namespace::{RuntimeNamespace, RuntimeNamespaceError};
use crowdb_protocol::ServicePort;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Find the workspace root by walking up from `CARGO_MANIFEST_DIR` until
/// a `pixi.toml` marker file is found. Falls back to `CARGO_MANIFEST_DIR`
/// if the marker is never found (should not happen in this workspace).
#[must_use]
pub fn workspace_root() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    while !dir.join("pixi.toml").exists() {
        if !dir.pop() {
            return PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        }
    }
    dir
}

/// The single repository-local root for generated CROWDB runtime state.
#[must_use]
pub fn runtime_root() -> PathBuf {
    create_dir(crowdb_protocol::port::namespace::runtime_root())
}

/// Disposable test and development environments.
#[must_use]
pub fn ephemeral_root() -> PathBuf {
    create_dir(runtime_root().join("ephemeral"))
}

/// Durable local clusters. Ordinary cleanup must preserve this directory.
#[must_use]
pub fn persistent_root() -> PathBuf {
    create_dir(runtime_root().join("persistent"))
}

/// Exported benchmark and diagnostic artifacts.
#[must_use]
pub fn artifacts_root() -> PathBuf {
    create_dir(runtime_root().join("artifacts"))
}

/// Cross-process port coordination state.
#[must_use]
pub fn ports_root() -> PathBuf {
    create_dir(runtime_root().join("ports"))
}

fn create_dir(path: PathBuf) -> PathBuf {
    let _ = std::fs::create_dir_all(&path);
    path
}

/// Compatibility path for callers not yet migrated to [`TestRuntime`].
#[must_use]
pub fn test_data_dir() -> PathBuf {
    create_dir(
        ephemeral_root()
            .join(format!("legacy-process-{}", std::process::id()))
            .join("data"),
    )
}

/// Compatibility log path for callers not yet migrated to [`TestRuntime`].
#[must_use]
pub fn test_log_dir() -> PathBuf {
    create_dir(
        ephemeral_root()
            .join(format!("legacy-process-{}", std::process::id()))
            .join("log"),
    )
}

fn unique_suffix() -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{}", std::process::id(), n)
}

/// One complete ephemeral runtime environment.
pub struct TestRuntime {
    namespace: RuntimeNamespace,
}

impl TestRuntime {
    /// Create an isolated runtime namespace below `.crowdb-runtime/ephemeral`.
    pub fn new(tag: &str) -> Result<Self, RuntimeNamespaceError> {
        Ok(Self {
            namespace: RuntimeNamespace::ephemeral(tag)?,
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        self.namespace.root()
    }

    #[must_use]
    pub fn data_dir(&self) -> PathBuf {
        self.namespace.data_dir()
    }

    #[must_use]
    pub fn config_dir(&self) -> PathBuf {
        self.namespace.config_dir()
    }

    #[must_use]
    pub fn log_dir(&self) -> PathBuf {
        self.namespace.log_dir()
    }

    #[must_use]
    pub fn artifacts_dir(&self) -> PathBuf {
        self.namespace.artifacts_dir()
    }

    /// Return a stable root for one logical service identity.
    pub fn service_dir(&self, service: &str, identity: &str) -> Result<PathBuf, RuntimeNamespaceError> {
        self.namespace.service_dir(service, identity)
    }

    /// Return one stable port for a logical listener in this environment.
    pub fn assign_port(&mut self, service: ServicePort, instance: u16) -> Result<u16, RuntimeNamespaceError> {
        self.namespace.assign_port(service, instance)
    }

    /// Return one stable port for an arbitrary logical service identity.
    pub fn assign_named_port(
        &mut self,
        service: ServicePort,
        identity: &str,
    ) -> Result<u16, RuntimeNamespaceError> {
        self.namespace.assign_named_port(service, identity)
    }

    /// Record a child process for targeted runtime cleanup.
    pub fn record_process(&mut self, pid: u32) -> Result<(), RuntimeNamespaceError> {
        self.namespace.record_process(pid)
    }

    /// Preserve the namespace tree after this guard is dropped.
    #[must_use]
    pub fn keep(mut self) -> PathBuf {
        self.namespace.preserve();
        self.namespace.root().to_path_buf()
    }
}

/// A project-local test directory that auto-deletes on success but
/// preserves on failure (panic). Drop-in replacement for
/// `tempfile::TempDir` — provides `path()` and can be stored in struct
/// fields.
///
/// Created under `.crowdb-runtime/ephemeral/<tag>-<pid>-<counter>/data`.
/// identify which test created the directory when inspecting leftover
/// data from a failed test.
///
/// # Panics
/// Panics if directory creation fails (disk full, permission denied).
pub struct TestDir {
    path: PathBuf,
    cleanup: bool,
}

impl TestDir {
    /// Create a new unique directory under the ephemeral runtime root.
    ///
    /// # Errors
    /// Returns `io::Error` if `create_dir_all` fails.
    pub fn new(tag: &str) -> std::io::Result<Self> {
        let path = ephemeral_root()
            .join(format!("{tag}-{}", unique_suffix()))
            .join("data");
        std::fs::create_dir_all(&path)?;
        Ok(Self { path, cleanup: true })
    }

    /// The directory path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Consume the guard and return the path without scheduling cleanup.
    /// The directory will persist indefinitely.
    #[must_use]
    pub fn keep(mut self) -> PathBuf {
        self.cleanup = false;
        self.path.clone()
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if self.cleanup && !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

/// Create a [`TestDir`] under the ephemeral runtime root with the given tag.
/// Convenience wrapper for `TestDir::new(tag).unwrap()`.
///
/// # Panics
/// Panics if directory creation fails.
#[must_use]
pub fn tempdir_in_test_data(tag: &str) -> TestDir {
    TestDir::new(tag).unwrap_or_else(|e| panic!("create test dir '{tag}': {e}"))
}
