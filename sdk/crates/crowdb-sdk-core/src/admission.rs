// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use crate::{Error, HttpConfig};

pub(crate) struct Admission {
    requests: AtomicUsize,
    bytes: AtomicUsize,
    config: HttpConfig,
}

impl Admission {
    pub(crate) fn new(config: HttpConfig) -> Self {
        Self {
            requests: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            config,
        }
    }

    pub(crate) fn acquire_bytes(self: &Arc<Self>, bytes: usize) -> Result<Permit, Error> {
        take(&self.bytes, bytes, self.config.max_in_flight_bytes)?;
        Ok(Permit {
            admission: self.clone(),
            bytes,
            request_released: AtomicBool::new(true),
        })
    }
    pub(crate) fn acquire(self: &Arc<Self>, bytes: usize) -> Result<Permit, Error> {
        take(&self.requests, 1, self.config.max_requests)?;
        if let Err(error) = take(&self.bytes, bytes, self.config.max_in_flight_bytes) {
            self.requests.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        Ok(Permit {
            admission: Arc::clone(self),
            bytes,
            request_released: AtomicBool::new(false),
        })
    }
}

fn take(counter: &AtomicUsize, amount: usize, limit: usize) -> Result<(), Error> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(amount).filter(|next| *next <= limit)
        })
        .map(|_| ())
        .map_err(|_| Error::Capacity)
}

pub(crate) struct Permit {
    admission: Arc<Admission>,
    bytes: usize,
    request_released: AtomicBool,
}

impl Permit {
    pub(crate) fn release_request(&self) {
        if !self.request_released.swap(true, Ordering::AcqRel) {
            self.admission.requests.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.admission.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.release_request();
    }
}

/// Reserves SDK-owned input bytes before allocation; it does not occupy a request slot.
pub struct BufferPermit {
    pub(crate) _permit: Permit,
}
