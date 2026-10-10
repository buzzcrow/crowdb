// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;

use crowdb_protocol::mgmt::node::{CandidateNode, CandidateState, NodeAdvertisement, NODE_PROTOCOL_VERSION};
use uuid::Uuid;

const MAX_CANDIDATES: usize = 1024;
const MAX_ENDPOINTS: usize = 16;

/// Single-owner discovery cache; removal never modifies confirmed topology.
#[derive(Debug, Default)]
pub struct CandidateCache {
    instances: BTreeMap<String, NodeAdvertisement>,
}

impl CandidateCache {
    /// Returns false for malformed or excess observations.
    pub fn observe(&mut self, instance: String, mut advertisement: NodeAdvertisement) -> bool {
        if instance.len() > 255
            || instance.is_empty()
            || Uuid::parse_str(&advertisement.discovery_id).is_err()
            || advertisement
                .cluster_id
                .as_ref()
                .is_some_and(|id| Uuid::parse_str(id).is_err())
            || advertisement.monitor_endpoints.is_empty()
            || advertisement.monitor_endpoints.len() > MAX_ENDPOINTS
            || advertisement
                .monitor_endpoints
                .iter()
                .any(|endpoint| !valid_endpoint(endpoint))
            || (!self.instances.contains_key(&instance) && self.instances.len() >= MAX_CANDIDATES)
        {
            return false;
        }
        advertisement.monitor_endpoints.sort();
        advertisement.monitor_endpoints.dedup();
        self.instances.insert(instance, advertisement);
        true
    }

    pub fn remove(&mut self, instance: &str) {
        self.instances.remove(instance);
    }

    #[must_use]
    pub fn snapshot(&self, local: &NodeAdvertisement) -> Vec<CandidateNode> {
        let mut by_identity: BTreeMap<&str, Vec<&NodeAdvertisement>> = BTreeMap::new();
        for advertisement in self.instances.values() {
            by_identity
                .entry(&advertisement.discovery_id)
                .or_default()
                .push(advertisement);
        }
        let mut nodes = Vec::with_capacity(by_identity.len());
        for (identity, observations) in by_identity {
            let first = observations[0];
            let local_identity = identity == local.discovery_id;
            let conflict = local_identity || observations.iter().any(|observation| *observation != first);
            let state = if conflict {
                CandidateState::IdentityConflict
            } else if first.protocol_version != NODE_PROTOCOL_VERSION {
                CandidateState::Incompatible
            } else {
                match (&first.cluster_id, &local.cluster_id) {
                    (None, _) => CandidateState::Unbound,
                    (Some(peer), Some(ours)) if peer == ours => CandidateState::SameCluster,
                    (Some(_), _) => CandidateState::ForeignCluster,
                }
            };
            for observation in observations {
                let node = CandidateNode {
                    advertisement: observation.clone(),
                    state,
                };
                if !nodes.contains(&node) {
                    nodes.push(node);
                }
            }
        }
        nodes
    }
}

fn valid_endpoint(endpoint: &str) -> bool {
    if endpoint.len() > 256 {
        return false;
    }
    let Ok(url) = reqwest::Url::parse(endpoint) else {
        return false;
    };
    url.scheme() == "http"
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
        && url.path() == "/"
        && url.port().is_some()
}
