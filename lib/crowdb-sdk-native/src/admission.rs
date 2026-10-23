// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free credits; shared views retain one allocation charge.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::{CapacityKind, Error, Result};

pub(crate) struct Budget {
    limit: usize,
    used: AtomicUsize,
    high_water: AtomicUsize,
    kind: CapacityKind,
}

impl Budget {
    pub fn new(limit: usize, kind: CapacityKind) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
            high_water: AtomicUsize::new(0),
            kind,
        })
    }

    pub fn charge(self: &Arc<Self>, amount: usize) -> Result<Credit> {
        let before = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(amount).filter(|after| *after <= self.limit)
            })
            .map_err(|_| Error::Capacity(self.kind))?;
        self.high_water.fetch_max(before + amount, Ordering::Relaxed);
        Ok(Credit {
            budget: Arc::clone(self),
            amount,
        })
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    pub fn high_water(&self) -> usize {
        self.high_water.load(Ordering::Relaxed)
    }
}

pub(crate) struct Credit {
    budget: Arc<Budget>,
    amount: usize,
}

impl Drop for Credit {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.amount, Ordering::AcqRel);
    }
}
