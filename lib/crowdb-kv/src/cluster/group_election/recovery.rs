// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded concurrent recovery of a new leader's open prefix.

use crate::{
    cluster::group::{AcceptAttempt, PrepareAttempt, PxGroup},
    common::config::PxElectionConfig,
};
use futures::{stream, StreamExt};
use std::sync::atomic::Ordering;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

impl PxGroup {
    #[tracing::instrument(level = "info", skip_all, fields(s = self.log_store_id().unwrap_or(0), g = self.group_id, replica = self.local_replica().id, term))]
    pub(super) async fn recover_open_prefix(
        &self,
        term: u64,
        peer_floor: u64,
        peer_ceiling: u64,
        cfg: PxElectionConfig,
        cancel: CancellationToken,
    ) {
        let replica = self.local_replica();
        // Peers' committed frontiers carry no payload. Start at our own
        // frontier so quorum Phase 1 recovers every missing chosen value.
        let floor = replica.contiguous_chosen();
        let ceiling = replica
            .highest_seen_slot()
            .max(self.next_slot.load(Ordering::Acquire).saturating_sub(1))
            .max(peer_ceiling);
        debug!(term, floor, ceiling, peer_floor, "bulk phase 1 start");
        if ceiling > floor {
            let window = usize::try_from(cfg.bulk_prepare_window.max(1)).unwrap_or(usize::MAX);
            let mut slots = stream::iter((floor + 1)..=ceiling)
                .map(|slot| self.recover_prefix_slot(term, slot, &cancel))
                .buffer_unordered(window);
            let mut resolved = true;
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    result = slots.next() => match result {
                        Some(success) => resolved &= success,
                        None => break,
                    },
                }
            }
            self.next_slot
                .fetch_max(ceiling.saturating_add(1), Ordering::AcqRel);
            if !resolved {
                warn!(
                    term,
                    ceiling, "bulk phase 1 incomplete; leader recovery remains closed"
                );
                return;
            }
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = replica.await_apply_fence(ceiling.max(floor)) => {}
        }
        if replica.current_term_snapshot() != term || !replica.is_leader() {
            return;
        }
        self.leader_read_ready.store(true, Ordering::Release);
        replica.advance_known_commit_slot(replica.contiguous_chosen());
        replica.wake_apply_loop();
        debug!(term, ceiling, "bulk phase 1 done");
    }

    async fn recover_prefix_slot(&self, term: u64, slot: u64, cancel: &CancellationToken) -> bool {
        let replica = self.local_replica();
        if cancel.is_cancelled() || replica.current_term_snapshot() != term || !replica.is_leader() {
            return false;
        }
        let mut entry = match self
            .run_prepare_phase(replica, slot, bytes::Bytes::new(), self.quorum(), 0)
            .await
        {
            PrepareAttempt::Proceed { entry, .. } => entry,
            PrepareAttempt::Retry { error, .. } | PrepareAttempt::Fail { error } => {
                warn!(
                    slot,
                    error = error.keyword(),
                    "bulk phase 1 prepare failed; leader recovery remains closed"
                );
                return false;
            }
        };
        if cancel.is_cancelled() || replica.current_term_snapshot() != term || !replica.is_leader() {
            return false;
        }
        entry.term = term;
        match self.run_accept_phase(replica, &entry, self.quorum()).await {
            AcceptAttempt::Chosen => {
                replica.learn_chosen(&entry, &[]).await;
                self.fan_out_chosen_notice(&entry, self.group_id);
                true
            }
            AcceptAttempt::Retry { error, .. } | AcceptAttempt::Fail { error } => {
                warn!(
                    slot,
                    error = error.keyword(),
                    "bulk phase 1 accept failed; leader recovery remains closed"
                );
                false
            }
        }
    }
}
