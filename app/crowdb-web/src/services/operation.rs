// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use super::Failure;
use crate::{error::err_409, state::AppState};

pub(crate) struct Operation {
    state: AppState,
    keys: Vec<String>,
}

impl Operation {
    pub(crate) fn claim(state: &AppState, keys: Vec<String>) -> Result<Self, Failure> {
        Self::claim_inner(state, keys, false)
    }

    pub(super) fn claim_cleanup(state: &AppState, keys: Vec<String>) -> Result<Self, Failure> {
        Self::claim_inner(state, keys, true)
    }

    fn claim_inner(state: &AppState, keys: Vec<String>, cleanup: bool) -> Result<Self, Failure> {
        loop {
            let current = state.service_operations.load_full();
            if !cleanup && current.contains("cluster/reset") {
                return Err(err_409(
                    "Cluster reset is in progress; wait before changing deployments",
                ));
            }
            if keys.iter().any(|key| current.contains(key)) {
                return Err(err_409(
                    "A service operation is already running for this node or instance",
                ));
            }
            let mut next = (*current).clone();
            next.extend(keys.iter().cloned());
            let previous = state
                .service_operations
                .compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&current, &previous) {
                return Ok(Self {
                    state: state.clone(),
                    keys,
                });
            }
        }
    }

    pub(crate) async fn reset(state: &AppState) -> Result<Self, Failure> {
        let operation = Self::claim(state, vec!["cluster/reset".into()])?;
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while state.service_operations.load().len() != 1 {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| err_409("Deployment is still running; reset has not removed anything"))?;
        Ok(operation)
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        self.state.service_operations.rcu(|current| {
            let mut next = (**current).clone();
            for key in &self.keys {
                next.remove(key);
            }
            Arc::new(next)
        });
    }
}

#[cfg(test)]
#[path = "../../tests/internal/service_operation_test.rs"]
mod tests;
