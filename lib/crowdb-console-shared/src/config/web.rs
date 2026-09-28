use std::net::IpAddr;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

const VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WebMode {
    Docker,
    BareMetal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebProcessConfig {
    pub version: u32,
    pub mode: WebMode,
    pub bind: String,
    pub port: u16,
    pub group0_management_seeds: Vec<String>,
    pub ui_root: PathBuf,
    pub monitor_status: Option<PathBuf>,
    pub log_dir: PathBuf,
    pub log_max_file_mb: usize,
    pub log_max_files: usize,
    pub request_timeout_ms: Option<u64>,
}

impl WebProcessConfig {
    /// # Errors
    /// Rejects unknown fields, embedded topology, invalid paths, or missing Group 0 seeds.
    pub fn load(path: &Path) -> Result<Self> {
        let body = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&body).map_err(|error| Error::Config(error.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// # Errors
    /// Rejects unsupported versions, invalid listeners, and inconsistent mode-specific fields.
    pub fn validate(&self) -> Result<()> {
        if self.version != VERSION || self.port == 0 || self.bind.parse::<IpAddr>().is_err() {
            return invalid("web process version or listener is invalid");
        }
        if self.group0_management_seeds.is_empty() || self.group0_management_seeds.len() > 16 {
            return invalid("one to sixteen Group 0 management seeds are required");
        }
        for seed in &self.group0_management_seeds {
            let url =
                reqwest::Url::parse(seed).map_err(|_| Error::Config("management seed is invalid".into()))?;
            if url.scheme() != "http"
                || url.host_str().is_none()
                || url.path() != "/"
                || url.query().is_some()
                || url.fragment().is_some()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return invalid("management seed must be an unauthenticated HTTP origin");
            }
        }
        if !clean_absolute(&self.ui_root) || !clean_absolute(&self.log_dir) {
            return invalid("web UI and log paths must be clean absolute paths");
        }
        match (self.mode, &self.monitor_status) {
            (WebMode::Docker, Some(path)) if clean_absolute(path) => {}
            (WebMode::BareMetal, None) => {}
            _ => return invalid("monitor status path does not match web mode"),
        }
        if !(1..=1024).contains(&self.log_max_file_mb)
            || !(1..=16).contains(&self.log_max_files)
            || self
                .request_timeout_ms
                .is_some_and(|timeout| !(100..=300_000).contains(&timeout))
        {
            return invalid("web log or request limits are invalid");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchRegistry {
    pub version: u32,
    #[serde(default, rename = "launch")]
    pub launches: Vec<LaunchRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchRecord {
    pub node_id: u64,
    pub service_id: String,
    pub host: String,
    pub ssh_credential_ref: Option<String>,
    pub ssh_user: Option<String>,
    #[serde(default = "ssh_port")]
    pub ssh_port: u16,
    pub binary_path: PathBuf,
    pub service_config_path: PathBuf,
    pub workspace: PathBuf,
    pub auto_start: bool,
    #[serde(default)]
    pub args: Vec<String>,
    pub readiness_url: Option<String>,
}

const fn ssh_port() -> u16 {
    22
}

impl LaunchRegistry {
    /// # Errors
    /// Rejects unknown fields, cluster topology, invalid launch records, or duplicate identities.
    pub fn load(path: &Path) -> Result<Self> {
        let body = std::fs::read_to_string(path)?;
        let registry: Self = toml::from_str(&body).map_err(|error| Error::Config(error.to_string()))?;
        registry.validate()?;
        Ok(registry)
    }

    /// # Errors
    /// Rejects unsafe local launch paths, inline secrets, and duplicate identities.
    pub fn validate(&self) -> Result<()> {
        if self.version != VERSION {
            return invalid("launch registry version is unsupported");
        }
        let mut identities = std::collections::BTreeSet::new();
        for launch in &self.launches {
            if launch.node_id == 0
                || launch.service_id.is_empty()
                || launch.host.is_empty()
                || !launch
                    .service_id
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                || !identities.insert((launch.node_id, launch.service_id.as_str()))
                || !clean_absolute(&launch.binary_path)
                || !clean_absolute(&launch.service_config_path)
                || !clean_absolute(&launch.workspace)
                || launch.ssh_port == 0
                || launch.ssh_user.as_deref().is_some_and(str::is_empty)
                || (!launch.is_local() && launch.ssh_user.is_none())
                || launch
                    .args
                    .iter()
                    .any(|arg| arg.contains('\0') || arg == "--config" || arg.starts_with("--config="))
                || launch.ssh_credential_ref.as_deref().is_some_and(|value| {
                    value.is_empty()
                        || std::path::Path::new(value)
                            .components()
                            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
                        || !value
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || b"-._/".contains(&byte))
                })
            {
                return invalid("launch registry record is invalid");
            }
            if let Some(url) = &launch.readiness_url {
                let parsed = reqwest::Url::parse(url)
                    .map_err(|_| Error::Config("launch readiness URL is invalid".into()))?;
                if parsed.scheme() != "http"
                    || parsed.host_str().is_none()
                    || !parsed.username().is_empty()
                    || parsed.password().is_some()
                    || parsed.query().is_some()
                    || parsed.fragment().is_some()
                {
                    return invalid("launch readiness URL must be unauthenticated HTTP");
                }
            }
        }
        Ok(())
    }
}

impl LaunchRecord {
    #[must_use]
    pub fn is_local(&self) -> bool {
        self.ssh_user.is_none() && matches!(self.host.as_str(), "localhost" | "127.0.0.1" | "::1")
    }

    /// Native service arguments; the referenced config is always selected explicitly.
    #[must_use]
    pub fn command_args(&self) -> Vec<String> {
        let mut args = vec![
            "--config".into(),
            self.service_config_path.to_string_lossy().into_owned(),
        ];
        args.extend(self.args.iter().cloned());
        args
    }
}

fn clean_absolute(path: &Path) -> bool {
    path.is_absolute()
        && path
            .components()
            .all(|component| !matches!(component, Component::CurDir | Component::ParentDir))
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(Error::Config(message.into()))
}
