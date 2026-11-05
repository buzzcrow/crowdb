// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;

use crate::paxos::{PxNodeId, PxTerm};
use crate::wal::record::{RecordType, WALRecord};

#[derive(Default)]
pub(super) struct ReplayRecords {
    slots: BTreeMap<(u64, u8), WALRecord>,
    votes: Vec<WALRecord>,
    pub(super) current_term: PxTerm,
    pub(super) voted_for: Option<PxNodeId>,
    pub(super) scanned: u64,
}

impl ReplayRecords {
    pub(super) fn push(&mut self, record: WALRecord) {
        self.scanned += 1;
        // Election safety depends on every verified record, including accepts
        // superseded by a higher ballot and records physically ordered by disk.
        if record.term > self.current_term {
            self.current_term = record.term;
            self.voted_for = None;
        }
        if record.record_type == RecordType::VoteGranted {
            if record.term == self.current_term {
                self.voted_for = record.voted_for_id();
            }
            self.votes.push(record);
            return;
        }
        // A later promise must never erase an older accepted value. Retain the
        // highest accepted ballot and the highest promised ballot independently.
        let key = (record.slot, record.record_type as u8);
        match self.slots.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(record);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if record.ballot >= entry.get().ballot {
                    entry.insert(record);
                }
            }
        }
    }

    pub(super) fn into_records(mut self) -> Vec<WALRecord> {
        self.votes.extend(self.slots.into_values());
        self.votes
    }
}
