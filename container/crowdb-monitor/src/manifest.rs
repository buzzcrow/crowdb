use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::layout::is_clean_absolute;

const MANIFEST_VERSION: u32 = 1;
const MAX_STEPS: usize = 64;
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("bootstrap storage failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("bootstrap manifest cannot be decoded: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("bootstrap manifest is invalid: {0}")]
    Invalid(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestState {
    Initializing,
    Ready,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestStep {
    name: String,
    complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapManifest {
    version: u32,
    state: ManifestState,
    profile_digest: String,
    config_digest: String,
    deployment_id: Uuid,
    steps: Vec<ManifestStep>,
}

impl BootstrapManifest {
    #[must_use]
    pub fn state(&self) -> ManifestState {
        self.state
    }

    #[must_use]
    pub fn deployment_id(&self) -> Uuid {
        self.deployment_id
    }

    #[must_use]
    pub fn next_step(&self) -> Option<&str> {
        self.steps
            .iter()
            .find(|step| !step.complete)
            .map(|step| step.name.as_str())
    }

    /// # Errors
    /// Rejects a step outside the persisted bootstrap plan.
    pub fn operation_id(&self, step: &str) -> Result<[u8; 16], ManifestError> {
        if !self.steps.iter().any(|entry| entry.name == step) {
            return invalid("operation step is not part of the bootstrap plan");
        }
        let mut digest = Sha256::new();
        digest.update(b"crowdb-monitor-bootstrap-operation-v1\0");
        digest.update(self.deployment_id.as_bytes());
        digest.update(step.as_bytes());
        let result = digest.finalize();
        let mut identity = [0; 16];
        identity.copy_from_slice(&result[..16]);
        Ok(identity)
    }

    fn validate(
        &self,
        profile_digest: &str,
        config_digest: &str,
        steps: &[&str],
    ) -> Result<(), ManifestError> {
        if self.version != MANIFEST_VERSION
            || self.deployment_id.is_nil()
            || self.profile_digest != profile_digest
            || self.config_digest != config_digest
            || self.steps.len() != steps.len()
        {
            return invalid("version, identity, profile, configuration, or step plan changed");
        }
        let mut pending = false;
        for (actual, expected) in self.steps.iter().zip(steps) {
            if actual.name != *expected || (pending && actual.complete) {
                return invalid("bootstrap step order or completion is invalid");
            }
            pending |= !actual.complete;
        }
        if self.state == ManifestState::Ready && pending {
            return invalid("ready manifest has incomplete steps");
        }
        Ok(())
    }
}

pub struct BootstrapSession {
    directory: PathBuf,
    manifest: BootstrapManifest,
}

impl BootstrapSession {
    /// # Errors
    /// Rejects missing, non-empty uninitialized, symlinked, or incompatible data roots.
    pub fn open(
        data_root: &Path,
        profile_bytes: &[u8],
        config_bytes: &[u8],
        steps: &[&str],
    ) -> Result<Self, ManifestError> {
        validate_plan(steps)?;
        if !is_clean_absolute(data_root) || !fs::symlink_metadata(data_root)?.file_type().is_dir() {
            return invalid("data root must be an existing absolute directory");
        }
        let profile_digest = digest_hex(profile_bytes);
        let config_digest = digest_hex(config_bytes);
        let directory = data_root.join("bootstrap");
        let path = directory.join("manifest.json");
        if let Ok(metadata) = fs::symlink_metadata(&directory) {
            if !metadata.file_type().is_dir() {
                return invalid("bootstrap path must be a directory, not a link");
            }
        }
        if path.exists() {
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.file_type().is_file() || metadata.permissions().mode() & 0o777 != 0o600 {
                return invalid("manifest must be a regular mode-0600 file");
            }
            if metadata.len() > MAX_MANIFEST_BYTES {
                return invalid("manifest exceeds the size bound");
            }
            let manifest: BootstrapManifest = serde_json::from_slice(&fs::read(&path)?)?;
            manifest.validate(&profile_digest, &config_digest, steps)?;
            return Ok(Self { directory, manifest });
        }
        if fs::read_dir(data_root)?.next().is_some() {
            return invalid("non-empty data root has no bootstrap manifest");
        }
        fs::create_dir(&directory)?;
        File::open(data_root)?.sync_all()?;
        let manifest = BootstrapManifest {
            version: MANIFEST_VERSION,
            state: ManifestState::Initializing,
            profile_digest,
            config_digest,
            deployment_id: Uuid::new_v4(),
            steps: steps
                .iter()
                .map(|name| ManifestStep {
                    name: (*name).to_owned(),
                    complete: false,
                })
                .collect(),
        };
        persist(&directory, &manifest)?;
        Ok(Self { directory, manifest })
    }

    #[must_use]
    pub fn manifest(&self) -> &BootstrapManifest {
        &self.manifest
    }

    /// # Errors
    /// Rejects out-of-order, unknown, or post-ready steps and failed durable writes.
    pub fn complete_step(&mut self, step: &str) -> Result<(), ManifestError> {
        if self.manifest.state == ManifestState::Ready {
            return invalid("ready bootstrap cannot execute creation steps");
        }
        let Some(next) = self.manifest.next_step() else {
            return invalid("all bootstrap steps are already complete");
        };
        if next != step {
            return invalid("bootstrap step is out of order");
        }
        let mut updated = self.manifest.clone();
        let Some(entry) = updated.steps.iter_mut().find(|entry| entry.name == step) else {
            return invalid("bootstrap step is missing from the manifest");
        };
        entry.complete = true;
        persist(&self.directory, &updated)?;
        self.manifest = updated;
        Ok(())
    }

    /// # Errors
    /// Rejects incomplete bootstrap work and failed durable writes.
    pub fn mark_ready(&mut self) -> Result<(), ManifestError> {
        if self.manifest.state == ManifestState::Ready {
            return Ok(());
        }
        if self.manifest.next_step().is_some() {
            return invalid("bootstrap cannot become ready with incomplete steps");
        }
        let mut updated = self.manifest.clone();
        updated.state = ManifestState::Ready;
        persist(&self.directory, &updated)?;
        self.manifest = updated;
        Ok(())
    }
}

fn validate_plan(steps: &[&str]) -> Result<(), ManifestError> {
    if steps.is_empty() || steps.len() > MAX_STEPS {
        return invalid("bootstrap step count is outside supported bounds");
    }
    let mut names = BTreeSet::new();
    for name in steps {
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || !names.insert(*name)
        {
            return invalid("bootstrap step name is invalid or duplicated");
        }
    }
    Ok(())
}

fn digest_hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        write!(text, "{byte:02x}").expect("writing to String cannot fail");
    }
    text
}

fn persist(directory: &Path, manifest: &BootstrapManifest) -> Result<(), ManifestError> {
    let bytes = serde_json::to_vec(manifest)?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return invalid("manifest exceeds the size bound");
    }
    let temporary = directory.join(format!(".manifest-{}.tmp", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    let result = (|| {
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, directory.join("manifest.json"))?;
        File::open(directory)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(ManifestError::Io)
}

fn invalid<T>(message: impl Into<String>) -> Result<T, ManifestError> {
    Err(ManifestError::Invalid(message.into()))
}
