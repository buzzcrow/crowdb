// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use std::sync::Arc;

use super::KVEngine;

/// Terminal progress from one accepted handoff and its maintenance phase.
#[derive(Debug)]
pub struct EngineFlushReport {
    pub handoff_status: Result<(), String>,
    pub maintenance_status: Result<(), String>,
    pub handoff_completed: bool,
    pub handoff_advanced: bool,
    pub floor: u64,
    pub frontier: u64,
    pub published_coverage: u64,
    pub deferred_pages: u64,
}
impl EngineFlushReport {
    /// A maintenance error retains any successful handoff progress.
    /// # Errors
    /// Returns the first unsuccessful phase status.
    pub fn status(&self) -> Result<(), &str> {
        self.handoff_status.as_ref().map_err(String::as_str)?;
        self.maintenance_status.as_ref().map_err(String::as_str).copied()
    }
}

/// Accepted checkpoint progress remains meaningful after post-commit errors.
#[derive(Debug)]
pub struct EngineCheckpointReport {
    pub status: Result<(), String>,
    pub durable_committed: bool,
    pub durable_coverage: u64,
    pub work: Option<EngineCheckpointWork>,
}

/// Local materialization details, absent for a compatibility-only engine.
#[derive(Debug)]
pub struct EngineCheckpointWork {
    pub writeback_completed: bool,
    pub captured_coverage: u64,
    pub written_frames: u64,
    pub reused_frames: u64,
    pub rebase_deferred: u64,
}

/// The compatibility path has no per-stage progress to report.
#[derive(Debug)]
pub enum EngineFlushOutcome {
    LegacyCompleted,
    Reported(EngineFlushReport),
}
impl EngineFlushOutcome {
    #[must_use]
    pub fn advances_flush_count(&self) -> bool {
        match self {
            Self::LegacyCompleted => true,
            Self::Reported(report) => report.handoff_advanced,
        }
    }
    /// # Errors
    /// Returns the first unsuccessful phase status.
    pub fn status(&self) -> Result<(), &str> {
        match self {
            Self::LegacyCompleted => Ok(()),
            Self::Reported(report) => report.status(),
        }
    }
}

/// Await native maintenance where available; preserve the existing blocking
/// adapter for engines that only expose the compatibility API.
/// # Errors
/// Returns an admission error or a failed compatibility worker.
pub async fn flush_engine(engine: Arc<dyn KVEngine>) -> Result<EngineFlushOutcome, String> {
    if let Some(future) = engine.flush_async() {
        return future.await.map(EngineFlushOutcome::Reported);
    }
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || span.in_scope(|| engine.flush()))
        .await
        .map_err(|error| format!("engine flush task failed: {error}"))?;
    Ok(EngineFlushOutcome::LegacyCompleted)
}

/// Await a checkpoint while retaining any committed durable progress.
///
/// # Errors
/// Returns an admission error or a failed compatibility worker.
pub async fn checkpoint_engine(engine: Arc<dyn KVEngine>) -> Result<EngineCheckpointReport, String> {
    if let Some(future) = engine.persist_snapshot_async() {
        return future.await;
    }
    let span = tracing::Span::current();
    let coverage = tokio::task::spawn_blocking(move || span.in_scope(|| engine.persist_snapshot()))
        .await
        .map_err(|error| format!("engine checkpoint task failed: {error}"))?;
    Ok(EngineCheckpointReport {
        status: Ok(()),
        durable_committed: coverage > 0,
        durable_coverage: coverage,
        work: None,
    })
}
