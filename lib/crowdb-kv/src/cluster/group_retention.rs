// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Snapshot-backed reclamation certificates, separate from commit progress.

use super::{
    group::{PxGroup, RemoteReplicaKind},
    local_replica::PxLocalReplicaRole,
    replica::{HeartbeatReply, HeartbeatRequestPayload, PxReplicaError},
};
use crate::paxos::PxNodeId;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::Instrument;

pub(super) type HeartbeatTasks = JoinSet<(PxNodeId, bool, Result<HeartbeatReply, PxReplicaError>)>;

pub(super) struct ReclamationCertificate {
    epoch: u64,
    term: u64,
    leader: PxNodeId,
    before_slot: u64,
}

impl PxGroup {
    /// Every learner has applied this prefix, and snapshots retain it on the
    /// leader plus a voting peer (or locally for a singleton voting group).
    pub(crate) fn reclamation_certificate(&self) -> u64 {
        let local = self.local_replica();
        if self.tenure_cancel.is_cancelled() || local.role() != PxLocalReplicaRole::Leader {
            return 0;
        }
        let local_durable = local.wal().map_or(0, |wal| wal.snapshot_slot());
        let mut safe = local.contiguous_applied().min(local_durable);
        let applied = self.peer_applied.lock();
        for remote in &self.remote_replicas {
            if let RemoteReplicaKind::Real(remote) = remote {
                safe = safe.min(applied.get(&remote.node_id).copied().unwrap_or(0));
            }
        }
        drop(applied);
        if self.quorum() > 1 {
            let durable = self.peer_durable.lock();
            let witness = self
                .remote_replicas
                .iter()
                .filter_map(|remote| match remote {
                    RemoteReplicaKind::Real(remote) if remote.voting => {
                        Some(durable.get(&remote.node_id).copied().unwrap_or(0))
                    }
                    _ => None,
                })
                .max()
                .unwrap_or(0);
            safe = safe.min(witness);
        }
        safe
    }

    pub(crate) fn local_reclamation_slot(&self) -> u64 {
        let local = self.local_replica();
        let certificate = if local.role() == PxLocalReplicaRole::Leader {
            self.reclamation_certificate()
        } else {
            self.leader_gc_certificate
                .load()
                .as_ref()
                .map_or(0, |certificate| {
                    if certificate.epoch == self.membership_epoch()
                        && certificate.term == local.current_term_snapshot()
                        && Some(certificate.leader) == local.believed_leader_id()
                        && !self.tenure_cancel.is_cancelled()
                    {
                        certificate.before_slot
                    } else {
                        0
                    }
                })
        };
        certificate
            .min(local.contiguous_applied())
            .min(local.wal().map_or(0, |wal| wal.snapshot_slot()))
    }

    pub(crate) fn record_reclamation_heartbeat(&self, request: &HeartbeatRequestPayload) {
        let local = self.local_replica();
        if !self.tenure_cancel.is_cancelled()
            && request.gc_membership_epoch == self.membership_epoch()
            && local.role() == PxLocalReplicaRole::Follower
            && local.current_term_snapshot() == request.term
            && local.believed_leader_id() == Some(request.leader_id)
            && self
                .get_remote_replica(request.leader_id)
                .is_some_and(|peer| peer.voting)
        {
            // Keep the epoch and watermark in one retained object: an epoch
            // change racing this publication must not relabel an old value.
            self.leader_gc_certificate.rcu(|previous| {
                let before_slot = previous
                    .as_ref()
                    .filter(|previous| {
                        previous.epoch == request.gc_membership_epoch
                            && previous.term == request.term
                            && previous.leader == request.leader_id
                    })
                    .map_or(request.gc_before_slot, |previous| {
                        previous.before_slot.max(request.gc_before_slot)
                    });
                Some(Arc::new(ReclamationCertificate {
                    epoch: request.gc_membership_epoch,
                    term: request.term,
                    leader: request.leader_id,
                    before_slot,
                }))
            });
        }
    }

    pub(crate) fn record_heartbeat_progress(&self, peer: PxNodeId, term: u64, reply: HeartbeatReply) {
        let local = self.local_replica();
        if reply.success
            && reply.term == term
            && local.current_term_snapshot() == term
            && local.role() == PxLocalReplicaRole::Leader
            && !self.tenure_cancel.is_cancelled()
        {
            self.note_peer_applied(peer, reply.contiguous_applied);
            self.note_peer_durable(peer, reply.durable_snapshot_slot);
        }
    }

    /// Quorum latency is independent of the slowest peer, but its progress is
    /// still required before reclaiming a prefix shared by every learner.
    pub(super) fn drain_heartbeat_progress(self: &Arc<Self>, mut tasks: HeartbeatTasks, term: u64) {
        let weak = Arc::downgrade(self);
        tokio::spawn(
            async move {
                while let Some(result) = tasks.join_next().await {
                    let Some(group) = weak.upgrade() else { return };
                    if let Ok((peer, _, Ok(reply))) = result {
                        group.record_heartbeat_progress(peer, term, reply);
                    }
                }
            }
            .instrument(tracing::Span::current()),
        );
    }
}
