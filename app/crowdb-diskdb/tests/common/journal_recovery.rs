// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_diskdb::model::zone::DdbZone;
use crowdb_kv::cluster::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::kv::{Batch, BatchOp, CrowdbTreeConfig, CrowdbTreeEngine, KVEngine, Op};
use crowdb_kv::paxos::roles::{PxBallot, PxLogEntry};
use crowdb_kv::wal::{index::SegmentIndex, record::WALRecord, replay::ReplayResult};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::BusyBlockValue;
use crowdb_protocol::key::{BinaryKey, BusyBlockKey, ZoneKey};
use crowdb_protocol::ZoneValueExt;

pub struct TestReclaimedJournal;

impl TestReclaimedJournal {
    pub async fn replica(disk_id: DiskId) -> PxLocalReplica {
        let baseline = DdbZone::new(disk_id, 0, 1, 128);
        baseline
            .snapshot_slot
            .store(1, std::sync::atomic::Ordering::Release);
        let zone_key = ZoneKey {
            disk_id,
            zone_index: 0,
        }
        .to_bytes();
        let busy_key = BusyBlockKey {
            disk_id,
            zone_index: 0,
            unit_offset: 7,
        }
        .to_bytes();
        let busy = BusyBlockValue {
            unit_count: 1,
            owner_chunk: Some(ChunkId { high: 30, low: 40 }),
            unit_size: 1024 * 1024,
            allocation_ts: 100,
            ..Default::default()
        };
        let engine = CrowdbTreeEngine::open(&CrowdbTreeConfig::default()).unwrap();
        for (slot, key, value) in [
            (1, zone_key, baseline.to_zone_value().to_bytes()),
            (2, busy_key, bincode::serialize(&busy).unwrap()),
        ] {
            engine
                .apply(
                    slot,
                    &Batch {
                        ops: vec![BatchOp {
                            key: key.into(),
                            op: Op::Put(value.into()),
                        }],
                    },
                )
                .await
                .unwrap();
        }
        engine.noop(3);
        engine.handle().flush().unwrap();
        assert_eq!(engine.resume_from_slot(), 3);
        let replay = ReplayResult {
            records: vec![WALRecord::from_accepted(
                1,
                &PxLogEntry {
                    slot: 3,
                    ballot: PxBallot::new(0, 1),
                    term: 1,
                    payload: Bytes::new(),
                },
            )],
            index: SegmentIndex::new(),
            max_segment_id: 1,
            current_term: 1,
            voted_for: None,
        };
        PxLocalReplica::restore_from_replay_with_engine(
            1,
            PxLocalReplicaRole::Leader,
            &replay,
            Box::new(engine),
        )
        .await
        .unwrap()
    }
}
