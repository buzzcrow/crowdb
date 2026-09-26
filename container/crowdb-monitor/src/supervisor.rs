use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use thiserror::Error;
use tokio::time::{sleep, Instant};
use uuid::Uuid;

use crate::{
    DeploymentProfile, MonitorEvent, MonitorEventKind, MonitorPhase, MonitorStatus, ProbeError,
    ProbeExecutor, ProcessError, ProcessManager, ProfileError, ServiceProfile, ServiceStatus, StatusError,
    StatusStore,
};

const STARTUP_DEADLINE: Duration = Duration::from_secs(30);
const PROBE_RETRY_DELAY: Duration = Duration::from_millis(100);
const STOP_GRACE: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("deployment profile is invalid: {0}")]
    Profile(#[from] ProfileError),
    #[error("process operation failed: {0}")]
    Process(#[from] ProcessError),
    #[error("probe failed: {0}")]
    Probe(#[from] ProbeError),
    #[error("status update failed: {0}")]
    Status(#[from] StatusError),
    #[error("supervision state is invalid: {0}")]
    Invalid(&'static str),
}

pub struct Supervisor {
    profile: DeploymentProfile,
    order: Vec<String>,
    processes: ProcessManager,
    probes: ProbeExecutor,
    status_store: StatusStore,
    status: MonitorStatus,
    environment: BTreeMap<String, BTreeMap<String, String>>,
    probe_failures: BTreeMap<String, u32>,
    healthy_since: BTreeMap<String, Instant>,
    bootstrapped: bool,
}

impl Supervisor {
    /// # Errors
    /// Rejects invalid profiles or unavailable log/status directories.
    pub async fn new(
        profile: DeploymentProfile,
        deployment_id: Uuid,
        log_root: &Path,
        run_root: &Path,
    ) -> Result<Self, SupervisorError> {
        profile.validate()?;
        let order = profile
            .services_in_start_order()?
            .iter()
            .map(|service| service.id.clone())
            .collect();
        let processes = ProcessManager::new(log_root.to_owned(), profile.logs.clone()).await?;
        let probes = ProbeExecutor::new()?;
        let status_store = StatusStore::new(run_root)?;
        let mut status = MonitorStatus::new(deployment_id, MonitorPhase::Initializing);
        status_store.publish(&mut status)?;
        Ok(Self {
            profile,
            order,
            processes,
            probes,
            status_store,
            status,
            environment: BTreeMap::new(),
            probe_failures: BTreeMap::new(),
            healthy_since: BTreeMap::new(),
            bootstrapped: false,
        })
    }

    #[must_use]
    pub fn status(&self) -> &MonitorStatus {
        &self.status
    }

    pub fn monitor_log_mut(&mut self) -> &mut crate::MonitorLog {
        self.processes.monitor_log_mut()
    }

    /// # Errors
    /// Starts one service only after its dependencies are healthy and waits for its probe.
    pub async fn start_service(
        &mut self,
        id: &str,
        environment: BTreeMap<String, String>,
    ) -> Result<(), SupervisorError> {
        if matches!(self.status.phase, MonitorPhase::Draining | MonitorPhase::Failed) {
            return Err(SupervisorError::Invalid(
                "supervisor is no longer admitting children",
            ));
        }
        let service = self.service(id)?.clone();
        if self.status.services.contains_key(id) {
            return Err(SupervisorError::Invalid("service is already started"));
        }
        if service.dependencies.iter().any(|dependency| {
            !self
                .status
                .services
                .get(dependency)
                .is_some_and(|status| status.healthy)
        }) {
            return Err(SupervisorError::Invalid("service dependencies are not healthy"));
        }
        let pid = self.processes.start(&service, &environment).await?;
        if let Err(error) = self.wait_for_probe(&service).await {
            self.processes
                .record_event(&MonitorEvent {
                    kind: MonitorEventKind::ProbeFailed,
                    service: Some(id),
                    pid: Some(pid),
                    attempt: None,
                })
                .await?;
            self.processes.stop(id, STOP_GRACE).await?;
            return Err(error);
        }
        self.environment.insert(id.to_owned(), environment);
        self.healthy_since.insert(id.to_owned(), Instant::now());
        self.status.services.insert(
            id.to_owned(),
            ServiceStatus {
                pid: Some(pid),
                generation: 1,
                healthy: true,
                restart_attempts: 0,
            },
        );
        self.status_store.publish(&mut self.status)?;
        Ok(())
    }

    /// # Errors
    /// Refuses to publish readiness until every profile service is healthy.
    pub async fn mark_ready(&mut self) -> Result<(), SupervisorError> {
        if self.status.services.len() != self.order.len()
            || self.status.services.values().any(|service| !service.healthy)
        {
            return Err(SupervisorError::Invalid("not all services are healthy"));
        }
        self.bootstrapped = true;
        self.status.phase = MonitorPhase::Ready;
        self.status_store.publish(&mut self.status)?;
        self.processes
            .record_event(&MonitorEvent {
                kind: MonitorEventKind::Ready,
                service: None,
                pid: None,
                attempt: None,
            })
            .await?;
        Ok(())
    }

    /// # Errors
    /// Returns failed probes, process errors, or exhausted restart budgets.
    pub async fn poll_once(&mut self) -> Result<(), SupervisorError> {
        if matches!(self.status.phase, MonitorPhase::Draining | MonitorPhase::Failed) {
            return Err(SupervisorError::Invalid("supervisor is not running"));
        }
        for id in self.order.clone() {
            if !self.processes.owns(&id) {
                continue;
            }
            let service = self.service(&id)?.clone();
            let alive = self.processes.alive(&id)?;
            let healthy = alive && self.probes.probe_service(&service).await.is_ok();
            if healthy {
                self.probe_failures.insert(id.clone(), 0);
                if self.healthy_since.get(&id).is_some_and(|since| {
                    since.elapsed() >= Duration::from_millis(service.restart.stable_after_ms)
                }) {
                    if let Some(state) = self.status.services.get_mut(&id) {
                        if state.restart_attempts != 0 {
                            state.restart_attempts = 0;
                            self.status_store.publish(&mut self.status)?;
                            let pid = self.processes.pid(&id);
                            self.processes
                                .record_event(&MonitorEvent {
                                    kind: MonitorEventKind::RestartBudgetReset,
                                    service: Some(&id),
                                    pid,
                                    attempt: None,
                                })
                                .await?;
                        }
                    }
                }
                if let Some(state) = self.status.services.get_mut(&id) {
                    if !state.healthy {
                        state.healthy = true;
                        if self.bootstrapped && self.status.services.values().all(|service| service.healthy) {
                            self.status.phase = MonitorPhase::Ready;
                            self.processes
                                .record_event(&MonitorEvent {
                                    kind: MonitorEventKind::Ready,
                                    service: None,
                                    pid: None,
                                    attempt: None,
                                })
                                .await?;
                        }
                        self.status_store.publish(&mut self.status)?;
                    }
                }
                continue;
            }
            let failures = self.probe_failures.entry(id.clone()).or_default();
            self.healthy_since.remove(&id);
            *failures = failures.saturating_add(1);
            if *failures == 1 {
                self.processes
                    .record_event(&MonitorEvent {
                        kind: if alive {
                            MonitorEventKind::ProbeFailed
                        } else {
                            MonitorEventKind::ChildExited
                        },
                        service: Some(&id),
                        pid: self.processes.pid(&id),
                        attempt: None,
                    })
                    .await?;
            }
            self.status.phase = MonitorPhase::Restarting;
            if let Some(state) = self.status.services.get_mut(&id) {
                state.healthy = false;
            }
            self.status_store.publish(&mut self.status)?;
            if !alive || *failures >= service.probe.failure_threshold {
                self.recover(&id).await?;
            }
            break;
        }
        self.status_store.publish(&mut self.status)?;
        Ok(())
    }

    /// # Errors
    /// Returns fatal supervision failures or failed signal registration.
    pub async fn run_until_signal(&mut self) -> Result<(), SupervisorError> {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|_| SupervisorError::Invalid("cannot register SIGTERM handler"))?;
        loop {
            tokio::select! {
                _ = terminate.recv() => break,
                _ = tokio::signal::ctrl_c() => break,
                result = self.poll_once() => result?,
            }
            tokio::select! {
                _ = terminate.recv() => break,
                _ = tokio::signal::ctrl_c() => break,
                () = sleep(Duration::from_secs(1)) => {}
            }
        }
        self.shutdown().await
    }

    /// # Errors
    /// Disables restart, drains every child in reverse dependency order, and logs closure.
    pub async fn shutdown(&mut self) -> Result<(), SupervisorError> {
        self.status.phase = MonitorPhase::Draining;
        self.status_store.publish(&mut self.status)?;
        self.processes
            .record_event(&MonitorEvent {
                kind: MonitorEventKind::Draining,
                service: None,
                pid: None,
                attempt: None,
            })
            .await?;
        self.processes.stop_all(&self.order, STOP_GRACE).await?;
        for state in self.status.services.values_mut() {
            state.pid = None;
            state.healthy = false;
        }
        self.status_store.publish(&mut self.status)?;
        self.processes
            .record_event(&MonitorEvent {
                kind: MonitorEventKind::Stopped,
                service: None,
                pid: None,
                attempt: None,
            })
            .await?;
        Ok(())
    }

    fn service(&self, id: &str) -> Result<&ServiceProfile, SupervisorError> {
        self.profile
            .services
            .iter()
            .find(|service| service.id == id)
            .ok_or(SupervisorError::Invalid("unknown service"))
    }

    async fn wait_for_probe(&mut self, service: &ServiceProfile) -> Result<(), SupervisorError> {
        let deadline = Instant::now() + STARTUP_DEADLINE;
        let mut last_heartbeat = Instant::now();
        loop {
            if !self.processes.alive(&service.id)? {
                return Err(SupervisorError::Invalid("service exited before readiness"));
            }
            if self.probes.probe_service(service).await.is_ok() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(SupervisorError::Invalid("service readiness deadline expired"));
            }
            if last_heartbeat.elapsed() >= Duration::from_secs(1) {
                self.status_store.publish(&mut self.status)?;
                last_heartbeat = Instant::now();
            }
            sleep(PROBE_RETRY_DELAY).await;
        }
    }

    fn affected_services(&self, root: &str) -> Vec<String> {
        let mut affected = BTreeSet::from([root.to_owned()]);
        loop {
            let before = affected.len();
            for service in &self.profile.services {
                if service
                    .dependencies
                    .iter()
                    .any(|dependency| affected.contains(dependency))
                {
                    affected.insert(service.id.clone());
                }
            }
            if affected.len() == before {
                break;
            }
        }
        self.order
            .iter()
            .filter(|id| affected.contains(*id) && self.status.services.contains_key(*id))
            .cloned()
            .collect()
    }

    async fn recover(&mut self, root: &str) -> Result<(), SupervisorError> {
        let affected = self.affected_services(root);
        let service = self.service(root)?.clone();
        let first_attempt = self
            .status
            .services
            .get(root)
            .map_or(1, |state| state.restart_attempts.saturating_add(1));
        for attempt in first_attempt..=service.restart.max_attempts {
            self.stop_affected(&affected).await?;
            if let Some(state) = self.status.services.get_mut(root) {
                state.restart_attempts = attempt;
            }
            self.status_store.publish(&mut self.status)?;
            self.processes
                .record_event(&MonitorEvent {
                    kind: MonitorEventKind::Restarting,
                    service: Some(root),
                    pid: None,
                    attempt: Some(attempt),
                })
                .await?;
            let shift = attempt.saturating_sub(1).min(31);
            let backoff = service
                .restart
                .backoff_base_ms
                .saturating_mul(1_u64 << shift)
                .min(service.restart.backoff_max_ms);
            sleep(Duration::from_millis(backoff)).await;
            if self.start_affected(&affected).await? {
                self.probe_failures.insert(root.to_owned(), 0);
                if self.bootstrapped {
                    self.status.phase = MonitorPhase::Ready;
                    self.status_store.publish(&mut self.status)?;
                    self.processes
                        .record_event(&MonitorEvent {
                            kind: MonitorEventKind::Ready,
                            service: None,
                            pid: None,
                            attempt: None,
                        })
                        .await?;
                }
                return Ok(());
            }
        }
        self.stop_affected(&affected).await?;
        self.fail_exhausted(root, first_attempt.max(service.restart.max_attempts))
            .await?;
        Err(SupervisorError::Invalid("service restart budget exhausted"))
    }

    async fn stop_affected(&mut self, affected: &[String]) -> Result<(), SupervisorError> {
        for id in affected.iter().rev() {
            if self.processes.owns(id) {
                self.processes.stop(id, STOP_GRACE).await?;
            }
            if let Some(state) = self.status.services.get_mut(id) {
                state.pid = None;
                state.healthy = false;
            }
        }
        self.status_store.publish(&mut self.status)?;
        Ok(())
    }

    async fn start_affected(&mut self, affected: &[String]) -> Result<bool, SupervisorError> {
        for id in affected {
            let environment = self
                .environment
                .get(id)
                .cloned()
                .ok_or(SupervisorError::Invalid("service environment is missing"))?;
            let restart_service = self.service(id)?.clone();
            let Ok(pid) = self.processes.start(&restart_service, &environment).await else {
                self.processes
                    .record_event(&MonitorEvent {
                        kind: MonitorEventKind::ChildStartFailed,
                        service: Some(id),
                        pid: None,
                        attempt: None,
                    })
                    .await?;
                return Ok(false);
            };
            let readiness = self.wait_for_probe(&restart_service).await;
            if let Err(SupervisorError::Status(error)) = readiness {
                self.processes.stop(id, STOP_GRACE).await?;
                return Err(SupervisorError::Status(error));
            }
            if readiness.is_err() {
                self.processes.stop(id, STOP_GRACE).await?;
                self.processes
                    .record_event(&MonitorEvent {
                        kind: MonitorEventKind::ProbeFailed,
                        service: Some(id),
                        pid: Some(pid),
                        attempt: None,
                    })
                    .await?;
                return Ok(false);
            }
            if let Some(state) = self.status.services.get_mut(id) {
                state.pid = Some(pid);
                state.generation = state.generation.saturating_add(1);
                state.healthy = true;
            }
            self.healthy_since.insert(id.clone(), Instant::now());
            self.status_store.publish(&mut self.status)?;
        }
        Ok(true)
    }

    async fn fail_exhausted(&mut self, root: &str, attempt: u32) -> Result<(), SupervisorError> {
        self.status.phase = MonitorPhase::Failed;
        self.status_store.publish(&mut self.status)?;
        self.processes
            .record_event(&MonitorEvent {
                kind: MonitorEventKind::RestartExhausted,
                service: Some(root),
                pid: None,
                attempt: Some(attempt),
            })
            .await?;
        self.processes.stop_all(&self.order, STOP_GRACE).await?;
        for state in self.status.services.values_mut() {
            state.pid = None;
            state.healthy = false;
        }
        self.status_store.publish(&mut self.status)?;
        Ok(())
    }
}
