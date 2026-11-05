// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::KVFuture;

/// Rejection before a request can become chosen.
#[derive(Debug)]
pub enum ApplyAdmissionError {
    /// Finite resource capacity is currently occupied by accepted work.
    Busy,
    /// The request violates the configured complete-batch bound or format.
    Rejected(String),
}

/// A prepaid apply operation for one exact proposal payload.
/// Dropping it before choosing returns unused resource credit.
pub trait AdmittedApply: Send {
    /// Transfer the reserved bytes into the chosen slot's owned allocations.
    ///
    /// # Errors
    /// Reports a local terminal storage failure, never a consensus rejection.
    fn apply(self: Box<Self>, slot: u64) -> KVFuture<Result<(), String>>;
}

/// `None` selects an engine's compatibility apply path.
pub type ApplyAdmissionResult = Result<Option<Box<dyn AdmittedApply>>, ApplyAdmissionError>;
