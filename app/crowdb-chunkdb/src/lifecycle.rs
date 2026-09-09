// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk lifecycle state, serialization, and orchestration.

mod handler;
pub mod state;

pub use handler::{
    AppendChunkOutcome, CacheHint, ChunkGuard, ChunkLockMap, LifecycleError, LifecycleHandler, LockPolicy,
    ReservationFence, ReservationMutation, ReservationUpdate, ReserveGroupSpec,
};
pub use state::{ChunkState, StateTransitionError};
