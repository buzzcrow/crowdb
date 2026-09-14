// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use crowdb_protocol::common::DiskId;
use crowdb_protocol::DiskGroupId;

#[derive(Debug)]
struct EndpointSnapshot {
    generation: u64,
    endpoints: HashMap<DiskGroupId, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DiskRoute {
    disk_group_id: DiskGroupId,
    endpoint_generation: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct EndpointRoute {
    disk_group_id: DiskGroupId,
    endpoint: String,
    generation: u64,
}

impl EndpointRoute {
    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

/// Shared RCU routing state retained by every `DiskdbClient` clone.
pub(crate) struct DiskdbRoutingState {
    endpoints: ArcSwap<EndpointSnapshot>,
    disk_routes: ArcSwap<HashMap<DiskId, DiskRoute>>,
    next_endpoint_generation: AtomicU64,
}

impl DiskdbRoutingState {
    pub(crate) fn new() -> Self {
        Self {
            endpoints: ArcSwap::from_pointee(EndpointSnapshot {
                generation: 0,
                endpoints: HashMap::new(),
            }),
            disk_routes: ArcSwap::from_pointee(HashMap::new()),
            next_endpoint_generation: AtomicU64::new(1),
        }
    }

    pub(crate) fn replace_endpoints(&self, endpoints: HashMap<DiskGroupId, String>) {
        let snapshot = Arc::new(EndpointSnapshot {
            generation: self.next_generation(),
            endpoints,
        });
        self.endpoints.store(Arc::clone(&snapshot));
        self.reconcile_disk_routes(&snapshot);
    }

    pub(crate) fn disk_group_ids(&self) -> Vec<DiskGroupId> {
        let mut ids: Vec<_> = self.endpoints.load().endpoints.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub(crate) fn endpoint_for(&self, disk_group_id: DiskGroupId) -> Option<EndpointRoute> {
        let snapshot = self.endpoints.load();
        snapshot
            .endpoints
            .get(&disk_group_id)
            .map(|endpoint| EndpointRoute {
                disk_group_id,
                endpoint: endpoint.clone(),
                generation: snapshot.generation,
            })
    }

    pub(crate) fn endpoint_entries(&self) -> Vec<(DiskGroupId, String)> {
        let snapshot = self.endpoints.load();
        let mut entries: Vec<_> = snapshot
            .endpoints
            .iter()
            .map(|(disk_group_id, endpoint)| (*disk_group_id, endpoint.clone()))
            .collect();
        entries.sort_unstable_by_key(|(disk_group_id, _)| *disk_group_id);
        entries
    }

    pub(crate) fn first_disk_group(&self) -> Option<DiskGroupId> {
        self.endpoints.load().endpoints.keys().copied().min()
    }

    pub(crate) fn evict_endpoint(&self, disk_group_id: DiskGroupId, route: &EndpointRoute) -> bool {
        if route.disk_group_id != disk_group_id {
            return false;
        }
        loop {
            let current = self.endpoints.load_full();
            if current.generation != route.generation
                || current.endpoints.get(&disk_group_id) != Some(&route.endpoint)
            {
                return false;
            }
            let mut endpoints = current.endpoints.clone();
            endpoints.remove(&disk_group_id);
            let replacement = Arc::new(EndpointSnapshot {
                generation: self.next_generation(),
                endpoints,
            });
            let previous = self
                .endpoints
                .compare_and_swap(&current, Arc::clone(&replacement));
            if Arc::ptr_eq(&previous, &current) {
                self.reconcile_disk_routes(&replacement);
                return true;
            }
        }
    }

    pub(crate) fn learn_disk_route(&self, disk_id: DiskId, disk_group_id: DiskGroupId) {
        loop {
            let endpoints = self.endpoints.load_full();
            if !endpoints.endpoints.contains_key(&disk_group_id) {
                return;
            }
            let route = DiskRoute {
                disk_group_id,
                endpoint_generation: endpoints.generation,
            };
            self.update_disk_route(disk_id, route);
            if self.endpoints.load().generation == endpoints.generation {
                return;
            }
        }
    }

    pub(crate) fn disk_group_for(&self, disk_id: DiskId) -> Option<DiskGroupId> {
        let endpoints = self.endpoints.load();
        let routes = self.disk_routes.load();
        let route = routes.get(&disk_id)?;
        (route.endpoint_generation == endpoints.generation
            && endpoints.endpoints.contains_key(&route.disk_group_id))
        .then_some(route.disk_group_id)
    }

    fn update_disk_route(&self, disk_id: DiskId, route: DiskRoute) {
        loop {
            let current = self.disk_routes.load_full();
            if current.get(&disk_id) == Some(&route) {
                return;
            }
            let mut routes = (*current).clone();
            routes.insert(disk_id, route);
            let previous = self.disk_routes.compare_and_swap(&current, Arc::new(routes));
            if Arc::ptr_eq(&previous, &current) {
                return;
            }
        }
    }

    fn reconcile_disk_routes(&self, endpoints: &EndpointSnapshot) {
        loop {
            let current = self.disk_routes.load_full();
            let routes: HashMap<_, _> = current
                .iter()
                .filter_map(|(disk_id, route)| {
                    endpoints.endpoints.contains_key(&route.disk_group_id).then_some((
                        *disk_id,
                        DiskRoute {
                            disk_group_id: route.disk_group_id,
                            endpoint_generation: endpoints.generation,
                        },
                    ))
                })
                .collect();
            let previous = self.disk_routes.compare_and_swap(&current, Arc::new(routes));
            if Arc::ptr_eq(&previous, &current) {
                return;
            }
        }
    }

    fn next_generation(&self) -> u64 {
        loop {
            let generation = self.next_endpoint_generation.fetch_add(1, Ordering::Relaxed);
            if generation != 0 {
                return generation;
            }
        }
    }
}
