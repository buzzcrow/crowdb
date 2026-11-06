// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::report::OperationReport;
use crate::kv::{checkpoint_engine, flush_engine};

use super::local_replica::PxLocalReplica;

impl PxLocalReplica {
    pub(super) fn shutdown_apply_target(&self) -> u64 {
        self.last_chosen_slot()
            .max(self.known_commit_slot.load(std::sync::atomic::Ordering::Acquire))
    }

    pub(super) async fn drain_runtime_apply(&self, timeout: std::time::Duration) -> Result<(), String> {
        if !self.learner.engine().requires_contiguous_apply() {
            return Ok(());
        }
        self.wake_apply_loop();
        let drain = async {
            loop {
                let target = self.shutdown_apply_target();
                self.learner.await_applied(target).await;
                self.learner.drain_apply_tasks().await;
                if self.contiguous_applied() >= self.shutdown_apply_target() {
                    return;
                }
            }
        };
        if tokio::time::timeout(timeout, drain).await.is_err() {
            return Err(format!("critical: replica {} accepted Tree work did not drain within {} ms; apply/repair owners retained (applied {}, chosen {})",
                self.id, timeout.as_millis(), self.contiguous_applied(), self.shutdown_apply_target()));
        }
        Ok(())
    }

    pub(super) async fn finish_engine_shutdown(&self, report: &mut OperationReport) {
        let engine = self.learner.engine_arc();
        let flush = flush_engine(engine.clone()).await;
        let flush_error = match &flush {
            Ok(outcome) => outcome.status().err(),
            Err(error) => Some(error.as_str()),
        };
        if let Some(error) = flush_error {
            self.record_engine_shutdown_error(report, "flush", error);
        }
        match checkpoint_engine(engine).await {
            Ok(checkpoint) => {
                if checkpoint.durable_committed {
                    tracing::info!(
                        replica = self.id,
                        snapshot_slot = checkpoint.durable_coverage,
                        "PxLocalReplica shutdown: engine snapshot persisted"
                    );
                }
                if let Err(error) = checkpoint.status {
                    self.record_engine_shutdown_error(report, "checkpoint", &error);
                }
            }
            Err(error) => self.record_engine_shutdown_error(report, "checkpoint", &error),
        }
    }

    fn record_engine_shutdown_error(&self, report: &mut OperationReport, phase: &str, error: &str) {
        tracing::warn!(
            replica = self.id,
            phase,
            error,
            "engine shutdown stage failed; retaining WAL"
        );
        report.push_error(format!(
            "critical: replica {} engine {phase} failed: {error}",
            self.id
        ));
    }
}
