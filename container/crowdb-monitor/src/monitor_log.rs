use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use thiserror::Error;

use crate::process::log::RotatingLog;
use crate::LogProfile;

#[derive(Debug, Error)]
pub enum MonitorLogError {
    #[error("monitor log I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("monitor log serialization failed: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("monitor clock is invalid")]
    Clock,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorEventKind {
    Starting,
    Ready,
    Draining,
    Stopped,
    ChildStarted,
    ChildStopped,
    ProbeFailed,
    ChildExited,
    ChildStartFailed,
    Restarting,
    RestartBudgetReset,
    ListenerFenceFailed,
    RestartExhausted,
    BootstrapStepStarted,
    BootstrapStepCompleted,
    BootstrapFailed,
}

impl MonitorEventKind {
    fn is_warning(self) -> bool {
        matches!(
            self,
            Self::ProbeFailed
                | Self::ChildExited
                | Self::ChildStartFailed
                | Self::Restarting
                | Self::ListenerFenceFailed
                | Self::RestartExhausted
                | Self::BootstrapFailed
        )
    }
}

#[derive(Debug, Serialize)]
pub struct MonitorEvent<'a> {
    pub kind: MonitorEventKind,
    pub service: Option<&'a str>,
    pub pid: Option<u32>,
    pub attempt: Option<u32>,
}

#[derive(Serialize)]
struct StampedEvent<'a> {
    timestamp_ms: u64,
    level: &'static str,
    #[serde(flatten)]
    event: &'a MonitorEvent<'a>,
}

pub struct MonitorLog {
    output: RotatingLog,
    mirror_warnings_to_stderr: bool,
}

impl MonitorLog {
    /// # Errors
    /// Rejects a symlinked or non-directory monitor log path.
    pub async fn open(log_root: &Path, policy: LogProfile) -> Result<Self, MonitorLogError> {
        let directory = log_root.join("monitor");
        match tokio::fs::symlink_metadata(&directory).await {
            Ok(metadata) if !metadata.file_type().is_dir() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "monitor log path is not a directory",
                )
                .into());
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                tokio::fs::create_dir(&directory).await?;
            }
            Err(error) => return Err(error.into()),
        }
        let mirror_warnings_to_stderr = policy.mirror_warnings_to_stderr;
        let output = RotatingLog::open(&directory, "monitor.log", policy).await?;
        Ok(Self {
            output,
            mirror_warnings_to_stderr,
        })
    }

    /// # Errors
    /// Returns clock, encoding, or durable log write failures.
    pub async fn record(&mut self, event: &MonitorEvent<'_>) -> Result<(), MonitorLogError> {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| MonitorLogError::Clock)?
            .as_millis()
            .try_into()
            .map_err(|_| MonitorLogError::Clock)?;
        let warning = event.kind.is_warning();
        let mut body = serde_json::to_vec(&StampedEvent {
            timestamp_ms,
            level: if warning { "warn" } else { "info" },
            event,
        })?;
        body.push(b'\n');
        self.output.write_record(&body).await?;
        self.output.sync().await?;
        if warning && self.mirror_warnings_to_stderr {
            eprint!("{}", String::from_utf8_lossy(&body));
        }
        Ok(())
    }
    /// Additive diagnostic record; legacy lifecycle event fields stay unchanged.
    pub(crate) async fn record_process_exit(
        &mut self,
        service: &str,
        pid: u32,
        observation: &serde_json::Value,
    ) -> Result<(), MonitorLogError> {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| MonitorLogError::Clock)?
            .as_millis();
        let warning = observation.get("success").and_then(serde_json::Value::as_bool) != Some(true);
        let mut body = serde_json::to_vec(&serde_json::json!({
            "timestamp_ms": timestamp_ms,
            "level": if warning { "warn" } else { "info" },
            "kind": "child_exit_observed",
            "service": service,
            "pid": pid,
            "attempt": null,
            "process_exit": observation,
        }))?;
        body.push(b'\n');
        self.output.write_record(&body).await?;
        self.output.sync().await?;
        if warning && self.mirror_warnings_to_stderr {
            eprint!("{}", String::from_utf8_lossy(&body));
        }
        Ok(())
    }
}
