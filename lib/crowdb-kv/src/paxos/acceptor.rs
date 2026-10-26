// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Acceptor state machine for one consensus group.
//!
//! Implements P1 M1 consensus logic. Holds per-slot promised and accepted
//! state in a lock-free ordered map with retained entries during reclamation.
//!
//! Invariants enforced:
//! - **C2 — Ballot monotonic per slot.** `prepare`/`accept` reject any ballot strictly
//!   lower than the slot's current promise; equal-ballot accepts are idempotent.

#![allow(unsafe_code)]

use crate::paxos::roles::{Acceptor, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply, SlotIndex};
use crate::paxos::slot_node::PxSlotNode;
use crossbeam_skiplist::{map::Entry, SkipMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, PartialEq, Eq)]
enum PxAcceptResult {
    Accepted { slot: SlotIndex, ballot: PxBallot },
    Rejected(PxBallot),
}

#[derive(Default)]
pub struct PxAcceptor {
    slots: SkipMap<SlotIndex, PxSlotNode>,
    trim_slot: AtomicU64,
    /// Highest slot index ever opened on this acceptor via `prepare` or
    /// `accept`. Used as the bulk-Phase-1 ceiling input so a new leader
    /// can re-Prepare every slot a previous leader may have touched here,
    /// even ones whose values were never chosen.
    highest_seen_slot: AtomicU64,
}

impl PxAcceptor {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Highest slot ever opened (monotonic).
    #[must_use]
    pub fn highest_seen_slot(&self) -> SlotIndex {
        self.highest_seen_slot.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn accepted_log_tip(&self) -> Option<(SlotIndex, u64)> {
        let before = self.trim_slot();
        self.slots
            .iter()
            .rev()
            .take_while(|node| *node.key() >= before)
            .find_map(|node| {
                node.value()
                    .accepted_cloned()
                    .map(|entry| (entry.slot, entry.term))
            })
    }

    /// Slot-ordered iteration over accepted entries in
    /// `[start_slot, end_slot_exclusive)`. Yields `(slot, PxLogEntry)`
    /// clones for slots that have an accepted value; gaps (slots with no
    /// accepted entry, or slots below the trim point) are skipped. Used
    /// by `JournalScan` (diskdb R73 strategy 2 replay) — the acceptor
    /// holds every chosen entry on every replica (leader via `accept`,
    /// follower via `FetchGap` → `accept`), so this is a complete
    /// slot-ordered op log bounded by `contiguous_applied`.
    ///
    /// Lock-free and safe under concurrent writers: each yielded entry
    /// is a clone taken while retaining the map entry.
    pub fn accepted_iter_range(
        &self,
        start_slot: SlotIndex,
        end_slot_exclusive: SlotIndex,
    ) -> Vec<(SlotIndex, PxLogEntry)> {
        let start = start_slot.max(self.trim_slot());
        if start >= end_slot_exclusive {
            return Vec::new();
        }
        // crossbeam-skiplist 0.1.3's range iterator retains previously visited
        // nodes. Walk RAII entries so scans cannot pin reclaimed payloads.
        let mut cursor = self.slots.lower_bound(Bound::Included(&start));
        let mut entries = Vec::new();
        while let Some(node) = cursor {
            if *node.key() >= end_slot_exclusive {
                break;
            }
            if let Some(entry) = node.value().accepted_cloned() {
                entries.push((*node.key(), entry));
            }
            cursor = node.next();
        }
        entries
    }

    /// Current trim slot — slots below this have been GC'd and are no
    /// longer readable. `JournalScan` returns `KV_ERROR_JOURNAL_SCAN_GC_GAP`
    /// when `min_slot < trim_slot`.
    #[must_use]
    pub fn trim_slot(&self) -> SlotIndex {
        self.trim_slot.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn retained_slots(&self) -> usize {
        self.slots.len()
    }

    fn open_slot(&self, slot: SlotIndex) -> Option<Entry<'_, SlotIndex, PxSlotNode>> {
        if slot < self.trim_slot() {
            return None;
        }
        let node = self.slots.get_or_insert_with(slot, PxSlotNode::default);
        // A concurrent trim may have passed the initial admission check.
        // Retaining this entry keeps all node fields alive through rejection.
        if slot < self.trim_slot() {
            node.remove();
            return None;
        }
        Some(node)
    }

    fn bump_highest_seen(&self, slot: SlotIndex) {
        let mut prev = self.highest_seen_slot.load(Ordering::Relaxed);
        while slot > prev {
            match self.highest_seen_slot.compare_exchange_weak(
                prev,
                slot,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }
    }

    // ---------- internals ----------

    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    async fn prepare(&self, slot: SlotIndex, ballot: PxBallot) -> PxPrepareReply {
        let Some(guard) = self.open_slot(slot) else {
            return PxPrepareReply::Rejected {
                slot,
                current_promised: PxBallot::new(0, 0),
            };
        };
        let node = guard.value();
        self.bump_highest_seen(slot);
        loop {
            let current_ptr = node.promised.load(Ordering::Acquire);
            if !current_ptr.is_null() {
                let current = unsafe { &*current_ptr };
                if ballot <= *current {
                    return PxPrepareReply::Rejected {
                        slot,
                        current_promised: *current,
                    };
                }
            }
            let accepted = node.accepted_cloned();
            if node.cas_promised(current_ptr, ballot).is_ok() {
                return PxPrepareReply::Promised { slot, accepted };
            } // another writer raced, retry
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn inner_accept(&self, entry: &PxLogEntry) -> Option<PxAcceptResult> {
        let slot = entry.slot;
        let ballot = entry.ballot;
        let guard = self.open_slot(slot)?;
        let node = guard.value();
        self.bump_highest_seen(slot);
        loop {
            let promised_ptr = node.promised.load(Ordering::Acquire);
            if !promised_ptr.is_null() {
                let promised = unsafe { &*promised_ptr };
                if ballot < *promised {
                    return Some(PxAcceptResult::Rejected(*promised));
                }
            }
            // Ensure promised is at least the accept ballot (Paxos formulation).
            // A null promised (no prior prepare) is treated as (0, 0), not as
            // the accept ballot — otherwise the promised is never updated and
            // a subsequent prepare with a lower ballot could overwrite the
            // accepted value.
            if ballot > node.promised_cloned().unwrap_or(PxBallot::new(0, 0)) {
                match node.cas_promised(promised_ptr, ballot) {
                    Ok(_) | Err(_) => {} // either way, continue to accepted CAS
                }
            }
            let accepted_ptr = node.accepted.load(Ordering::Acquire);
            if node.cas_accepted(accepted_ptr, entry.clone()).is_ok() {
                return Some(PxAcceptResult::Accepted { slot, ballot });
            } // another writer raced, retry
        }
    }
}

impl Acceptor for PxAcceptor {
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    async fn accept(&self, entry: &PxLogEntry) -> PxAcceptReply {
        let slot = entry.slot;
        match self.inner_accept(entry) {
            Some(PxAcceptResult::Accepted { slot: s, ballot: b }) => {
                PxAcceptReply::Accepted { slot: s, ballot: b }
            }
            Some(PxAcceptResult::Rejected(current)) => PxAcceptReply::Rejected {
                slot,
                current_promised: current,
            },
            None => PxAcceptReply::Rejected {
                slot,
                current_promised: PxBallot::new(0, 0),
            },
        }
    }
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    async fn prepare(&self, slot: SlotIndex, ballot: PxBallot) -> PxPrepareReply {
        self.prepare(slot, ballot).await
    }

    fn accepted_at(&self, slot: SlotIndex) -> Option<PxLogEntry> {
        if slot < self.trim_slot() {
            return None;
        }
        self.slots.get(&slot)?.value().accepted_cloned()
    }
    fn promised_at(&self, slot: SlotIndex) -> Option<PxBallot> {
        if slot < self.trim_slot() {
            return None;
        }
        self.slots.get(&slot)?.value().promised_cloned()
    }
    fn reclaim(&self) -> usize {
        let before = self.trim_slot();
        // The full iterator releases its cursor references as it advances;
        // the dependency's range iterator does not.
        self.slots
            .iter()
            .take_while(|node| *node.key() < before)
            .filter(Entry::remove)
            .count()
    }
    fn trim(&self, before_slot: SlotIndex) {
        self.trim_slot.fetch_max(before_slot, Ordering::AcqRel);
    }
    fn trim_slot(&self) -> SlotIndex {
        self.trim_slot()
    }
}
