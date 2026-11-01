// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{cache::ServiceObservations, mutation::Mutation};
use crate::AppState;
use crowdb_protocol::mgmt::node::NodeServiceIntent;

/// Holds the production observation fence while a fixture controls a mutation.
pub struct ServiceObservationMutationForTests {
    state: AppState,
    mutation: Mutation,
}

/// Invalidate one target using the same fence as remote lifecycle execution.
#[must_use]
pub fn invalidate_service_observation_for_tests(
    state: &AppState,
    node: u64,
    service: &str,
) -> ServiceObservationMutationForTests {
    ServiceObservationMutationForTests {
        state: state.clone(),
        mutation: ServiceObservations::mutation(state, node, service),
    }
}

impl ServiceObservationMutationForTests {
    /// Publish an actual monitor reply through the production completion path.
    pub fn complete(&self, intent: NodeServiceIntent, reply: &serde_json::Value) {
        self.mutation.complete(&self.state, intent, reply);
    }
}
