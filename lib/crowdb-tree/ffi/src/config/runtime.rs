// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicit host and per-Tree limits. Omitted values use native defaults.

use crate::{TreeBindingOptions, TreeResourceLimits, TreeRuntimeOptions};

#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TreeRuntimeConfig {
    pub host: TreeHostConfig,
    pub tree: TreeBindingConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TreeHostConfig {
    pub backend_workers: Option<usize>,
    pub backend_queue_limit: Option<usize>,
    pub workers: Option<usize>,
    pub cpu_queue_limit: Option<usize>,
    pub max_waiters_per_class: Option<usize>,
    pub lifecycle_bytes: Option<usize>,
    pub limits: TreeResourceConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TreeBindingConfig {
    pub backend_queue_limit: Option<usize>,
    pub cpu_queue_limit: Option<usize>,
    pub lifecycle_bytes: Option<usize>,
    pub limits: TreeResourceConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TreeResourceConfig {
    pub handoff_bytes: Option<usize>,
    pub routing_bytes: Option<usize>,
    pub reclaim_bytes: Option<usize>,
    pub writer_bytes: Option<usize>,
    pub gap_repair_bytes: Option<usize>,
    pub io_operations: Option<usize>,
}

impl TreeResourceConfig {
    fn apply(&self, defaults: TreeResourceLimits) -> TreeResourceLimits {
        TreeResourceLimits {
            handoff_bytes: self.handoff_bytes.unwrap_or(defaults.handoff_bytes),
            routing_bytes: self.routing_bytes.unwrap_or(defaults.routing_bytes),
            reclaim_bytes: self.reclaim_bytes.unwrap_or(defaults.reclaim_bytes),
            writer_bytes: self.writer_bytes.unwrap_or(defaults.writer_bytes),
            gap_repair_bytes: self.gap_repair_bytes.unwrap_or(defaults.gap_repair_bytes),
            io_operations: self.io_operations.unwrap_or(defaults.io_operations),
        }
    }
}

impl TreeRuntimeConfig {
    #[must_use]
    pub fn runtime_options(&self) -> TreeRuntimeOptions {
        let defaults = TreeRuntimeOptions::default();
        TreeRuntimeOptions {
            backend_workers: self.host.backend_workers.unwrap_or(defaults.backend_workers),
            backend_queue_limit: self
                .host
                .backend_queue_limit
                .unwrap_or(defaults.backend_queue_limit),
            workers: self.host.workers.unwrap_or(defaults.workers),
            cpu_queue_limit: self.host.cpu_queue_limit.unwrap_or(defaults.cpu_queue_limit),
            max_waiters_per_class: self
                .host
                .max_waiters_per_class
                .unwrap_or(defaults.max_waiters_per_class),
            lifecycle_bytes: self.host.lifecycle_bytes.unwrap_or(defaults.lifecycle_bytes),
            limits: self.host.limits.apply(defaults.limits),
        }
    }

    #[must_use]
    pub fn binding_options(&self) -> TreeBindingOptions {
        let defaults = TreeBindingOptions::default();
        TreeBindingOptions {
            backend_queue_limit: self
                .tree
                .backend_queue_limit
                .unwrap_or(defaults.backend_queue_limit),
            cpu_queue_limit: self.tree.cpu_queue_limit.unwrap_or(defaults.cpu_queue_limit),
            lifecycle_bytes: self.tree.lifecycle_bytes.unwrap_or(defaults.lifecycle_bytes),
            limits: self.tree.limits.apply(defaults.limits),
        }
    }

    /// Validate host and per-Tree capacity before opening data.
    /// # Errors
    /// Returns the specific incompatible configuration field.
    pub fn validate(&self) -> Result<(), String> {
        let host = self.runtime_options();
        let tree = self.binding_options();
        if host.workers == 0 || host.cpu_queue_limit == 0 || host.max_waiters_per_class == 0 {
            return Err("tree_runtime host workers, queue and waiter capacities must be positive".to_owned());
        }
        if host.backend_workers == 0
            || host.backend_queue_limit == 0
            || tree.backend_queue_limit == 0
            || tree.backend_queue_limit > host.backend_queue_limit
        {
            return Err(
                "tree_runtime backend workers/queues must be positive and the Tree queue must fit the host"
                    .to_owned(),
            );
        }
        if tree.cpu_queue_limit == 0 || tree.cpu_queue_limit > host.cpu_queue_limit {
            return Err("tree_runtime.tree.cpu_queue_limit must be in 1..=host.cpu_queue_limit".to_owned());
        }
        for (name, local, global) in [
            (
                "handoff_bytes",
                tree.limits.handoff_bytes,
                host.limits.handoff_bytes,
            ),
            (
                "routing_bytes",
                tree.limits.routing_bytes,
                host.limits.routing_bytes,
            ),
            (
                "reclaim_bytes",
                tree.limits.reclaim_bytes,
                host.limits.reclaim_bytes,
            ),
            ("writer_bytes", tree.limits.writer_bytes, host.limits.writer_bytes),
            (
                "gap_repair_bytes",
                tree.limits.gap_repair_bytes,
                host.limits.gap_repair_bytes,
            ),
            (
                "io_operations",
                tree.limits.io_operations,
                host.limits.io_operations,
            ),
        ] {
            if local == 0 || local > global {
                return Err(format!(
                    "tree_runtime.tree.limits.{name} must be positive and at most its host limit"
                ));
            }
        }
        if host.lifecycle_bytes == 0
            || host.lifecycle_bytes > host.limits.routing_bytes
            || tree.lifecycle_bytes == 0
            || tree.lifecycle_bytes > tree.limits.routing_bytes
        {
            return Err(
                "tree_runtime lifecycle_bytes must be positive and fit its routing partition".to_owned(),
            );
        }
        Ok(())
    }
}
