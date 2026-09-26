pub(crate) mod log;

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use rustix::process::{kill_process, Pid, Signal};
use thiserror::Error;
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use crate::{LogProfile, MonitorEvent, MonitorEventKind, MonitorLog, MonitorLogError, ServiceProfile};

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("child process I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("process log task failed: {0}")]
    Log(#[from] tokio::task::JoinError),
    #[error("monitor lifecycle log failed: {0}")]
    MonitorLog(#[from] MonitorLogError),
    #[error("process state is invalid: {0}")]
    Invalid(&'static str),
}

struct ManagedProcess {
    child: Child,
    logger: JoinHandle<io::Result<()>>,
}

pub struct ProcessManager {
    processes: BTreeMap<String, ManagedProcess>,
    log_root: PathBuf,
    log_policy: LogProfile,
    events: MonitorLog,
}

impl ProcessManager {
    /// # Errors
    /// Rejects an unavailable monitor lifecycle log.
    pub async fn new(log_root: PathBuf, log_policy: LogProfile) -> Result<Self, ProcessError> {
        let mut events = MonitorLog::open(&log_root, log_policy.clone()).await?;
        events
            .record(&MonitorEvent {
                kind: MonitorEventKind::Starting,
                service: None,
                pid: Some(std::process::id()),
                attempt: None,
            })
            .await?;
        Ok(Self {
            processes: BTreeMap::new(),
            log_root,
            log_policy,
            events,
        })
    }

    /// # Errors
    /// Rejects overlapping child ownership or failed spawn/log setup.
    pub async fn start(
        &mut self,
        service: &ServiceProfile,
        environment: &BTreeMap<String, String>,
    ) -> Result<u32, ProcessError> {
        if self.processes.contains_key(&service.id) {
            return Err(ProcessError::Invalid("service already has an owned process"));
        }
        let log_directory = self.log_root.join(&service.id);
        std::fs::create_dir_all(&log_directory)?;
        let mut command = Command::new(&service.program);
        command
            .args(&service.args)
            .envs(&service.env)
            .envs(environment)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        let pid = child
            .id()
            .ok_or(ProcessError::Invalid("spawned child has no PID"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or(ProcessError::Invalid("child stdout is unavailable"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or(ProcessError::Invalid("child stderr is unavailable"))?;
        let policy = self.log_policy.clone();
        let logger =
            tokio::spawn(async move { Box::pin(log::pump(stdout, stderr, &log_directory, policy)).await });
        self.processes
            .insert(service.id.clone(), ManagedProcess { child, logger });
        if let Err(error) = self
            .events
            .record(&MonitorEvent {
                kind: MonitorEventKind::ChildStarted,
                service: Some(&service.id),
                pid: Some(pid),
                attempt: None,
            })
            .await
        {
            let _ = self.stop(&service.id, Duration::from_secs(2)).await;
            return Err(error.into());
        }
        Ok(pid)
    }

    #[must_use]
    pub fn pid(&self, id: &str) -> Option<u32> {
        self.processes.get(id).and_then(|process| process.child.id())
    }

    /// # Errors
    /// Returns process observation failures. A completed process remains owned until stopped.
    pub fn alive(&mut self, id: &str) -> Result<bool, ProcessError> {
        let process = self
            .processes
            .get_mut(id)
            .ok_or(ProcessError::Invalid("service is not owned"))?;
        if process.logger.is_finished() {
            return Ok(false);
        }
        Ok(process.child.try_wait()?.is_none())
    }

    /// # Errors
    /// Sends TERM, waits for the owned PID, escalates to KILL after the deadline, and joins logs.
    pub async fn stop(&mut self, id: &str, grace: Duration) -> Result<(), ProcessError> {
        let mut process = self
            .processes
            .remove(id)
            .ok_or(ProcessError::Invalid("service is not owned"))?;
        if process.child.try_wait()?.is_none() {
            if let Some(pid) = process
                .child
                .id()
                .and_then(|value| i32::try_from(value).ok())
                .and_then(Pid::from_raw)
            {
                if let Err(error) = kill_process(pid, Signal::TERM) {
                    if error != rustix::io::Errno::SRCH {
                        return Err(io::Error::from_raw_os_error(error.raw_os_error()).into());
                    }
                }
            }
            if timeout(grace, process.child.wait()).await.is_err() {
                process.child.start_kill()?;
                process.child.wait().await?;
            }
        }
        let log_result = timeout(Duration::from_secs(5), &mut process.logger).await;
        if let Ok(joined) = log_result {
            joined?.map_err(ProcessError::Io)?;
        } else {
            process.logger.abort();
            return Err(ProcessError::Invalid("child log pipes did not close after exit"));
        }
        self.events
            .record(&MonitorEvent {
                kind: MonitorEventKind::ChildStopped,
                service: Some(id),
                pid: None,
                attempt: None,
            })
            .await?;
        Ok(())
    }

    /// # Errors
    /// Stops all owned children in reverse start order.
    pub async fn stop_all(&mut self, order: &[String], grace: Duration) -> Result<(), ProcessError> {
        let mut first_error = None;
        for id in order.iter().rev() {
            if self.processes.contains_key(id) {
                if let Err(error) = self.stop(id, grace).await {
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    /// # Errors
    /// Rejects a still-serving endpoint after its previous process was reaped.
    pub async fn wait_listener_closed(&self, address: &str, deadline: Duration) -> Result<(), ProcessError> {
        let address = address
            .parse::<std::net::SocketAddr>()
            .map_err(|_| ProcessError::Invalid("listener address is invalid"))?;
        let until = tokio::time::Instant::now() + deadline;
        loop {
            if timeout(
                Duration::from_millis(200),
                tokio::net::TcpStream::connect(address),
            )
            .await
            .is_ok_and(|result| result.is_err())
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= until {
                return Err(ProcessError::Invalid("listener remains owned after process exit"));
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// # Errors
    /// Returns failed durable monitor event writes.
    pub async fn record_event(&mut self, event: &MonitorEvent<'_>) -> Result<(), ProcessError> {
        self.events.record(event).await?;
        Ok(())
    }
}
