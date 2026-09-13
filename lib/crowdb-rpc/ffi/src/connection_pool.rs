// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free-read connection pools shared by client transports.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::Connection;

/// Failure while acquiring a connection from an endpoint pool.
#[derive(Debug)]
pub enum ConnectionPoolError<E> {
    /// Establishing one of the candidate pool's connections failed.
    Connect(E),
    /// Installing another endpoint would exceed the configured bound.
    Capacity { max_endpoints: usize },
}

impl<E: std::fmt::Display> std::fmt::Display for ConnectionPoolError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(error) => write!(formatter, "connection failed: {error}"),
            Self::Capacity { max_endpoints } => {
                write!(formatter, "endpoint connection limit {max_endpoints} reached")
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for ConnectionPoolError<E> {}

/// A connection selected from one immutable pool generation.
#[derive(Clone, Debug)]
pub struct SelectedConnection {
    connection: Connection,
    generation: u64,
}

impl SelectedConnection {
    /// The selected transport connection.
    #[must_use]
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// The pool generation from which the connection was selected.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Consume the selection and return its connection.
    #[must_use]
    pub fn into_connection(self) -> Connection {
        self.connection
    }
}

#[derive(Debug)]
struct ConnectionPool {
    generation: u64,
    connections: Vec<Connection>,
    cursor: AtomicU64,
}

impl ConnectionPool {
    fn select(&self) -> SelectedConnection {
        let index = if self.connections.len() == 1 {
            0
        } else {
            usize::try_from(self.cursor.fetch_add(1, Ordering::Relaxed)).unwrap_or(0) % self.connections.len()
        };
        SelectedConnection {
            connection: self.connections[index].clone(),
            generation: self.generation,
        }
    }
}

#[derive(Clone, Debug)]
struct PoolSnapshot {
    clear_epoch: u64,
    pools: HashMap<String, Arc<ConnectionPool>>,
}

/// RCU-published endpoint connection pools.
///
/// Readers never acquire a shard lock. A cold caller builds a complete pool
/// before publication; concurrent builders race one compare-and-swap and only
/// the winner becomes current. Each selected connection carries the pool
/// generation needed for exact invalidation after a delayed transport error.
#[derive(Debug)]
pub struct ConnectionPoolIndex {
    snapshot: ArcSwap<PoolSnapshot>,
    pool_size: usize,
    max_endpoints: Option<usize>,
    next_generation: AtomicU64,
}

impl ConnectionPoolIndex {
    /// Create an index with at least one connection per endpoint.
    #[must_use]
    pub fn new(pool_size: usize, max_endpoints: Option<usize>) -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(PoolSnapshot {
                clear_epoch: 0,
                pools: HashMap::new(),
            }),
            pool_size: pool_size.max(1),
            max_endpoints: max_endpoints.map(|limit| limit.max(1)),
            next_generation: AtomicU64::new(1),
        }
    }

    /// Number of connections installed in each complete endpoint pool.
    #[must_use]
    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    /// Number of currently published endpoints.
    #[must_use]
    pub fn len(&self) -> usize {
        self.snapshot.load().pools.len()
    }

    /// Whether no endpoint pool is currently published.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.snapshot.load().pools.is_empty()
    }

    /// Select from an existing complete endpoint pool.
    #[must_use]
    pub fn get(&self, endpoint: &str) -> Option<SelectedConnection> {
        self.snapshot.load().pools.get(endpoint).map(|pool| pool.select())
    }

    /// Select an existing connection or build and atomically install a pool.
    ///
    /// `connect` runs without retaining the RCU snapshot. It may run more than
    /// once when a concurrent `clear` retires a candidate built before that
    /// clear; a concurrent installer for the same endpoint simply wins and the
    /// losing candidate is dropped.
    pub fn get_or_try_install<E>(
        &self,
        endpoint: &str,
        mut connect: impl FnMut() -> Result<Connection, E>,
    ) -> Result<SelectedConnection, ConnectionPoolError<E>> {
        loop {
            let observed = self.snapshot.load_full();
            if let Some(pool) = observed.pools.get(endpoint) {
                return Ok(pool.select());
            }
            self.check_capacity(&observed)?;

            let mut connections = Vec::with_capacity(self.pool_size);
            for _ in 0..self.pool_size {
                connections.push(connect().map_err(ConnectionPoolError::Connect)?);
            }
            let candidate = Arc::new(ConnectionPool {
                generation: self.next_generation(),
                connections,
                cursor: AtomicU64::new(0),
            });

            loop {
                let current = self.snapshot.load_full();
                if let Some(pool) = current.pools.get(endpoint) {
                    return Ok(pool.select());
                }
                if current.clear_epoch != observed.clear_epoch {
                    break;
                }
                self.check_capacity(&current)?;

                let mut pools = current.pools.clone();
                pools.insert(endpoint.to_string(), Arc::clone(&candidate));
                let replacement = Arc::new(PoolSnapshot {
                    clear_epoch: current.clear_epoch,
                    pools,
                });
                let previous = self.snapshot.compare_and_swap(&current, replacement);
                if Arc::ptr_eq(&previous, &current) {
                    return Ok(candidate.select());
                }
            }
        }
    }

    /// Remove an endpoint only when it is still the selected generation.
    pub fn invalidate(&self, endpoint: &str, generation: u64) -> bool {
        loop {
            let current = self.snapshot.load_full();
            let Some(pool) = current.pools.get(endpoint) else {
                return false;
            };
            if pool.generation != generation {
                return false;
            }

            let mut pools = current.pools.clone();
            pools.remove(endpoint);
            let replacement = Arc::new(PoolSnapshot {
                clear_epoch: current.clear_epoch,
                pools,
            });
            let previous = self.snapshot.compare_and_swap(&current, replacement);
            if Arc::ptr_eq(&previous, &current) {
                return true;
            }
        }
    }

    /// Retire every current pool and reject candidates built before this call.
    pub fn clear(&self) {
        loop {
            let current = self.snapshot.load_full();
            let replacement = Arc::new(PoolSnapshot {
                clear_epoch: current.clear_epoch.wrapping_add(1),
                pools: HashMap::new(),
            });
            let previous = self.snapshot.compare_and_swap(&current, replacement);
            if Arc::ptr_eq(&previous, &current) {
                return;
            }
        }
    }

    fn check_capacity<E>(&self, snapshot: &PoolSnapshot) -> Result<(), ConnectionPoolError<E>> {
        if let Some(max_endpoints) = self.max_endpoints {
            if snapshot.pools.len() >= max_endpoints {
                return Err(ConnectionPoolError::Capacity { max_endpoints });
            }
        }
        Ok(())
    }

    fn next_generation(&self) -> u64 {
        loop {
            let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
            if generation != 0 {
                return generation;
            }
        }
    }
}
