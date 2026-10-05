// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Local configuration for the standalone UI before Group 0 bootstrap.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crowdb_console_shared::error::{Error, Result};
use crowdb_console_shared::ConsoleConfig;

use crate::state::AppState;

static NEXT_WRITE: AtomicU64 = AtomicU64::new(0);

mod readiness;
mod recovery;

impl AppState {
    /// Open the standalone UI workspace, creating an empty configuration on first use.
    ///
    /// # Errors
    /// Reports unreadable or malformed existing configuration and failed durable writes.
    pub fn open_standalone(directory: PathBuf) -> Result<Self> {
        fs::create_dir_all(&directory)?;
        let directory = fs::canonicalize(directory)?;
        let path = directory.join("config.json");
        let config = match fs::read(&path) {
            Ok(body) => serde_json::from_slice::<ConsoleConfig>(&body)
                .map_err(|error| Error::Config(format!("{}: {error}", path.display())))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => ConsoleConfig::default(),
            Err(error) => return Err(error.into()),
        };
        let mut state = Self::with_runtime_root(config, directory);
        state.config_path = Some(path);
        state.persist()?;
        Ok(state)
    }

    pub(crate) fn persist_standalone(&self) -> Result<()> {
        let Some(path) = &self.config_path else {
            return Ok(());
        };
        // Retain the existing read guard through publication so a mutation cannot
        // overtake an older snapshot while that snapshot is being written.
        let config = self
            .config
            .read()
            .map_err(|error| Error::Config(error.to_string()))?;
        let body = serde_json::to_vec_pretty(&*config).map_err(|error| Error::Config(error.to_string()))?;
        let temporary = path.with_extension(format!(
            "json.{}.{}.tmp",
            std::process::id(),
            NEXT_WRITE.fetch_add(1, Ordering::Relaxed)
        ));
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)?;
            file.write_all(&body)?;
            file.sync_all()?;
            fs::rename(&temporary, path)?;
            fs::File::open(self.runtime_root.as_ref())?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
}
