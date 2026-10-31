// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]

use crate::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crate::kv::{CrowdbTreeBackend, CrowdbTreeConfig, CrowdbTreeEngine, KVEngine};
use crate::paxos::learner::PxLearner;
use crate::paxos::roles::{Acceptor, Learner};
use crate::wal::replay::ReplayResult;
use std::io;
use std::sync::atomic::Ordering;

impl PxLocalReplica {
    /// Rebuild a fresh local replica from WAL replay output, using the
    /// default [`KVEngine`] (crowdb-tree with mem-block backend, in-memory).
    /// See [`PxLocalReplica::restore_from_replay_with_engine`] for the full argument
    /// and for injecting a durable backend (e.g. [`crate::kv::CrowdbTreeEngine`]
    /// with file/block storage).
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if any replayed promised/accepted record cannot be
    /// re-applied through the normal replica handlers.
    pub async fn restore_from_replay(
        id: u64,
        role: PxLocalReplicaRole,
        replay: &ReplayResult,
    ) -> io::Result<Self> {
        let opt = CrowdbTreeConfig {
            backend: CrowdbTreeBackend::MemBlock,
            ..Default::default()
        };
        let engine = CrowdbTreeEngine::open(&opt)
            .map_err(|e| io::Error::other(format!("crowdb-tree mem-block open failed: {e:?}")))?;
        Self::restore_from_replay_with_engine(id, role, replay, Box::new(engine)).await
    }

    /// Rebuild a fresh local replica from WAL replay output, backed by a
    /// caller-supplied [`KVEngine`] instead of the default in-memory one.
    ///
    /// Replays the recovered records through the normal acceptor / learner APIs
    /// so restored state follows the same invariants as live traffic. A
    /// durable engine that reports a non-zero [`KVEngine::resume_from_slot`]
    /// (e.g. [`crate::kv::CrowdbTreeEngine`] recovered from an on-disk snapshot)
    /// skips re-`learn`ing that already-durable prefix — see Pass 2 below
    /// for how the learner's frontier is seeded to match what a full replay
    /// would have produced.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if any replayed promised/accepted record cannot be
    /// re-applied through the normal replica handlers.
    pub async fn restore_from_replay_with_engine(
        id: u64,
        role: PxLocalReplicaRole,
        replay: &ReplayResult,
        engine: Box<dyn KVEngine>,
    ) -> io::Result<Self> {
        // Read before the engine is wrapped/used: `resume_from_slot`'s
        // contract only promises an accurate floor for a freshly-recovered
        // engine that hasn't taken any `apply` calls yet in this process.
        let resume_from = engine.resume_from_slot();
        let replica = Self::new_with_learner(id, role, PxLearner::with_engine(engine));

        // Accepted history and later promises are independent durable facts.
        // FetchGap may learn an old chosen value after a higher promise; replay
        // must retain that value and the higher fence regardless of record order.
        for record in replay
            .records
            .iter()
            .filter(|record| record.record_type == crate::wal::record::RecordType::Accepted)
        {
            let entry = record.to_log_entry().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "restore replay accepted missing log entry",
                )
            })?;
            let _ = replica.acceptor.accept(&entry).await;
        }
        for record in &replay.records {
            if record.record_type == crate::wal::record::RecordType::Promised {
                let _ = replica.acceptor.prepare(record.slot, record.ballot).await;
            }
        }

        replica.with_election_state(|state| {
            state.current_term = replay.current_term;
            state.voted_for = replay.voted_for;
            state.role = role;
            state.leader_id = if role == PxLocalReplicaRole::Leader {
                Some(id)
            } else {
                None
            };
        });
        replica.role_atomic.store(role.as_u8(), Ordering::Release);

        // Pass 2: rebuild the learner's committed KV state from the acceptor.
        //
        // The acceptor was fully rebuilt in Pass 1 (highest-ballot-per-slot
        // wins).  Now we walk every slot that has an accepted entry and
        // `learn` it into the state machine.  This is safe because:
        //
        // - `KVEngine::apply` is idempotent: an op is skipped when
        //   `slot <= resolved_slot(key)`, so re-applying the same slot is a
        //   no-op.
        // - `apply` uses highest-slot-wins per key, so out-of-order replay
        //   still produces the correct final KV state.
        // - `update_frontier` handles out-of-order slots via the
        //   `out_of_order` BTreeMap, so watermarks stay correct even with
        //   gaps.
        // - NoOp entries (empty payload) are skipped by `apply_entry` and
        //   do not corrupt the KV state.
        //
        // If the engine reported a resume floor (`resume_from > 0`), skip
        // re-`learn`ing that prefix and start the walk at `resume_from +
        // 1` -- always, even if the term at `resume_from` can't be
        // recovered below. This is not just an optimization: an engine with
        // its own internal durable-floor gate (e.g. crowdb-tree's
        // `MemTable::durable_floor`, set from `resume_from_slot`'s exact
        // value at `flush` time) rejects *any* write at `slot <= floor`
        // regardless of key -- stronger than the per-key highest-slot-wins
        // `KVEngine::apply` documents -- so re-attempting a write below the
        // floor isn't just redundant, it can silently no-op a key that slot
        // legitimately touches. There is no safe way to "fall back" to
        // replaying it once the engine is past that floor.
        //
        // A recovered engine proves the complete durable prefix even when
        // WAL reclamation removed its boundary record. Preserve that prefix;
        // requesting it again can never succeed once peers reclaimed it too.
        // Term zero explicitly records unknown historical term. Election
        // freshness uses the durable acceptor tip, not this chosen watermark.
        let highest = replica.acceptor.highest_seen_slot();
        let start_slot = if resume_from > 0 {
            let term = replica
                .acceptor
                .accepted_at(resume_from)
                .map_or(0, |entry| entry.term);
            replica.learner.seed_resume_frontier(resume_from, term);
            resume_from.saturating_add(1)
        } else {
            1
        };
        for slot in start_slot..=highest {
            if let Some(entry) = replica.acceptor.accepted_at(slot) {
                replica.learner.learn(entry, &[]).await;
            }
        }

        Ok(replica)
    }
}
