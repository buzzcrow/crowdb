// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::VecDeque;

#[derive(Debug)]
struct Hint<T> {
    work: T,
    failed: bool,
}

/// Discardable local positions; durable resource facts remain the discovery source.
#[derive(Debug)]
pub(crate) struct RetirementQueue<T, const LIMIT: usize, const DISCOVERY: usize> {
    pending: VecDeque<Hint<T>>,
    active: bool,
    discarded: bool,
}
impl<T, const LIMIT: usize, const DISCOVERY: usize> Default for RetirementQueue<T, LIMIT, DISCOVERY> {
    fn default() -> Self {
        Self {
            pending: VecDeque::new(),
            active: false,
            discarded: false,
        }
    }
}
impl<T, const LIMIT: usize, const DISCOVERY: usize> RetirementQueue<T, LIMIT, DISCOVERY> {
    pub(crate) fn len(&self) -> usize {
        self.pending.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
    pub(crate) fn can_discover(&self) -> bool {
        self.pending.len() <= LIMIT - DISCOVERY
    }
    pub(crate) fn needs_rescan(&self) -> bool {
        self.discarded && !self.pending.is_empty() && self.pending.iter().all(|hint| hint.failed)
    }
    pub(crate) fn restarting_scan(&mut self) {
        self.discarded = false;
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.pending.iter().map(|hint| &hint.work)
    }
    pub(crate) fn push(&mut self, work: T) {
        self.pending.push_back(Hint { work, failed: false });
    }
    pub(crate) fn begin(&mut self) -> Option<&mut T> {
        self.active = !self.pending.is_empty();
        self.pending.front_mut().map(|hint| &mut hint.work)
    }
    pub(crate) fn finish(&mut self, complete: bool, failed: bool) {
        if !self.active {
            return;
        }
        self.active = false;
        if complete {
            self.pending.pop_front();
        } else {
            if let Some(hint) = self.pending.front_mut() {
                hint.failed = failed;
            }
            self.pending.rotate_left(1);
        }
        while self.pending.iter().filter(|hint| hint.failed).count() > LIMIT - DISCOVERY {
            if let Some(oldest) = self.pending.iter().position(|hint| hint.failed) {
                self.pending.remove(oldest);
                self.discarded = true;
            }
        }
    }
    pub(crate) fn timeout(&mut self) {
        self.finish(false, true);
    }
}
