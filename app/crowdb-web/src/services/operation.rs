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
        loop {
            let current = state.service_operations.load_full();
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
