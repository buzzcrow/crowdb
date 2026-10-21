// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reduce current dependency evidence without performing repair or deployment.

use crowdb_protocol::mgmt::cluster::{
    ClusterObservation, ClusterPhase, ClusterStatus, DependencyKind, DependencyObservation, DependencyState,
    ReadinessReason,
};

mod collect;
pub use collect::collect;
mod member;
pub use member::member_evidence;

const REQUIRED: [DependencyKind; 6] = [
    DependencyKind::Authority,
    DependencyKind::Metadata,
    DependencyKind::Storage,
    DependencyKind::Member,
    DependencyKind::Service,
    DependencyKind::Protocol,
];

#[derive(Default)]
struct Evidence {
    states: std::collections::BTreeMap<DependencyKind, Vec<DependencyState>>,
    reasons: Vec<ReadinessReason>,
}

/// Determine whole-cluster readiness from a bounded observational snapshot.
#[must_use]
pub fn reduce(mut observation: ClusterObservation, now_unix_ms: u64) -> ClusterStatus {
    observation.dependencies.sort();
    let mut evidence = collect_evidence(&observation, now_unix_ms);
    for kind in REQUIRED {
        if !evidence.states.contains_key(&kind) {
            evidence.reasons.push(reason(
                &format!("{kind:?}"),
                "observation-missing",
                "Required application evidence is missing",
            ));
        }
    }
    if invalid(&observation) {
        evidence
            .states
            .entry(DependencyKind::Identity)
            .or_default()
            .push(DependencyState::Failed);
        evidence.reasons.push(reason(
            "cluster",
            "invalid-observation",
            "Unsupported cluster observation version, identity or initial voter policy",
        ));
    }
    let phase = classify(&observation, &evidence.states);
    evidence.reasons.sort();
    evidence.reasons.dedup();
    ClusterStatus {
        phase,
        reasons: evidence.reasons,
        observation,
    }
}

fn collect_evidence(observation: &ClusterObservation, now_unix_ms: u64) -> Evidence {
    let mut evidence = Evidence::default();
    let mut identities = std::collections::BTreeSet::new();
    for fact in &observation.dependencies {
        if fact.operation_id != fact.current_operation_id {
            evidence.reasons.push(reason(
                &fact.id,
                "operation-superseded",
                "Historical operation does not describe current readiness",
            ));
            let replacement = observation.dependencies.iter().any(|current| {
                current.id == fact.id
                    && current.kind == fact.kind
                    && current.operation_id == fact.current_operation_id
                    && current.operation_id == current.current_operation_id
            });
            if !replacement && fact.required && fact.kind != DependencyKind::Runtime {
                evidence
                    .states
                    .entry(fact.kind)
                    .or_default()
                    .push(DependencyState::Unknown);
            }
            continue;
        }
        if !identities.insert(&fact.id) {
            evidence
                .states
                .entry(DependencyKind::Identity)
                .or_default()
                .push(DependencyState::Failed);
            evidence.reasons.push(reason(
                &fact.id,
                "conflicting-observations",
                "Duplicate dependency identity",
            ));
        }
        let state = current_state(fact, now_unix_ms);
        if state != DependencyState::Ready {
            evidence.reasons.push(observation_reason(fact, state));
        }
        // Runtime facts never gate application readiness, including remote unknowns.
        if fact.kind != DependencyKind::Runtime
            && (fact.required || (fact.kind == DependencyKind::Identity && state == DependencyState::Failed))
        {
            evidence.states.entry(fact.kind).or_default().push(state);
        }
    }
    evidence
}

fn classify(
    observation: &ClusterObservation,
    states: &std::collections::BTreeMap<DependencyKind, Vec<DependencyState>>,
) -> ClusterPhase {
    let contains = |kind, state| states.get(&kind).is_some_and(|states| states.contains(&state));
    if states
        .values()
        .flatten()
        .any(|state| *state == DependencyState::Failed)
    {
        ClusterPhase::Failed
    } else if observation.bound && !contains(DependencyKind::Authority, DependencyState::Ready) {
        ClusterPhase::Unavailable
    } else if observation.bound
        && REQUIRED.iter().all(|kind| states.contains_key(kind))
        && states
            .values()
            .flatten()
            .all(|state| *state == DependencyState::Ready)
    {
        ClusterPhase::Ready
    } else if observation.setup_completed
        || states
            .values()
            .flatten()
            .any(|state| *state == DependencyState::Stopped)
        || (observation.bound
            && states
                .get(&DependencyKind::Member)
                .is_some_and(|states| states.iter().any(|state| *state != DependencyState::Ready)))
    {
        ClusterPhase::Degraded
    } else if observation.work_accepted {
        ClusterPhase::Initializing
    } else {
        ClusterPhase::Waiting
    }
}

fn invalid(observation: &ClusterObservation) -> bool {
    observation.version != 1
        || !matches!(observation.initial_voters, 1 | 3)
        || (observation.bound && observation.cluster_id.is_none())
        || observation.cluster_id.as_ref().is_some_and(|identity| {
            uuid::Uuid::parse_str(identity).map_or(true, |identity| identity.is_nil())
        })
        || observation.policy.as_ref().is_some_and(|policy| {
            policy.validate().is_err()
                || policy.initial_voters != observation.initial_voters
                || policy.cluster != observation.cluster
        })
}

fn observation_reason(fact: &DependencyObservation, state: DependencyState) -> ReadinessReason {
    if state == fact.state {
        fact.reason
            .clone()
            .unwrap_or_else(|| reason(&fact.id, "not-ready", "Dependency is not ready"))
    } else {
        reason(
            &fact.id,
            "observation-not-current",
            "Dependency observation is stale, superseded or has a future timestamp",
        )
    }
}

fn current_state(fact: &DependencyObservation, now_unix_ms: u64) -> DependencyState {
    let fresh = now_unix_ms
        .checked_sub(fact.observed_at_unix_ms)
        .is_some_and(|age| age <= fact.max_age_ms);
    if fresh && fact.operation_id == fact.current_operation_id {
        fact.state
    } else {
        DependencyState::Unknown
    }
}

fn reason(dependency: &str, code: &str, detail: &str) -> ReadinessReason {
    ReadinessReason {
        dependency: dependency.into(),
        code: code.into(),
        detail: detail.into(),
    }
}
