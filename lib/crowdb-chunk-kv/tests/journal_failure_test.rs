use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_kv::memory::MemoryPartitionTree;
use crowdb_chunk_kv::{
    ChunkKvError, JournalPosition, MutationOperation, Partition, PartitionConfig, PartitionId,
    PartitionJournal, PartitionLifecycle, PartitionRange, RequestId,
};
use crowdb_chunk_stream::StreamName;

struct FailedJournal {
    appends: AtomicUsize,
    wrong_position_count: bool,
}

#[async_trait]
impl PartitionJournal for FailedJournal {
    async fn append_frames(&self, _: &[Bytes]) -> crowdb_chunk_kv::Result<Vec<JournalPosition>> {
        self.appends.fetch_add(1, Ordering::Relaxed);
        if self.wrong_position_count {
            Ok(Vec::new())
        } else {
            Err(ChunkKvError::Internal("journal outcome unavailable".into()))
        }
    }

    async fn read_window(&self, _: u64, _: usize) -> crowdb_chunk_kv::Result<Bytes> {
        Ok(Bytes::new())
    }

    async fn trim_prefix(&self, _: u64) -> crowdb_chunk_kv::Result<u64> {
        Ok(0)
    }

    async fn reclaim_metadata_before(&self, _: u64, _: usize) -> crowdb_chunk_kv::Result<u64> {
        Ok(0)
    }

    async fn close(&self) -> crowdb_chunk_kv::Result<()> {
        Ok(())
    }

    fn stream_name(&self) -> StreamName {
        StreamName { high: 1, low: 1 }
    }

    fn manifest_generation(&self) -> u64 {
        1
    }

    fn tail(&self) -> u64 {
        0
    }
}

#[tokio::test]
async fn failed_journal_requires_recovery_and_cannot_be_checkpointed_as_transfer_source() {
    assert_failed_journal_recovers(false).await;
}

#[tokio::test]
async fn malformed_journal_result_requires_recovery_without_reusing_sequence() {
    assert_failed_journal_recovers(true).await;
}

async fn assert_failed_journal_recovers(wrong_position_count: bool) {
    let journal = Arc::new(FailedJournal {
        appends: AtomicUsize::new(0),
        wrong_position_count,
    });
    let partition = Partition::open(
        PartitionId { high: 1, low: 1 },
        PartitionRange::default(),
        1,
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::default()),
        journal.clone(),
    )
    .unwrap();
    let first = partition
        .mutate(
            1,
            RequestId {
                client_high: 1,
                client_low: 1,
                client_sequence: 1,
            },
            MutationOperation::Put {
                key: b"first".to_vec(),
                value: b"value".to_vec(),
            },
        )
        .await;
    assert!(matches!(first, Err(ChunkKvError::Internal(_))));
    assert_eq!(partition.lifecycle(), PartitionLifecycle::Recovering);
    assert_eq!(journal.appends.load(Ordering::Relaxed), 1);
    assert_eq!(partition.metrics().snapshot().journal_failures, 1);
    assert_eq!(
        partition.get(1, b"first", None).await,
        Err(ChunkKvError::Recovering)
    );
    assert_eq!(
        partition
            .mutate(
                1,
                RequestId {
                    client_high: 1,
                    client_low: 1,
                    client_sequence: 2,
                },
                MutationOperation::Delete {
                    key: b"first".to_vec(),
                },
            )
            .await,
        Err(ChunkKvError::Recovering)
    );
    assert_eq!(journal.appends.load(Ordering::Relaxed), 1);
    assert_eq!(
        partition.suspend_for_transfer(1).await,
        Err(ChunkKvError::Recovering)
    );
    assert!(matches!(
        partition.checkpoint_quiesced(1).await,
        Err(ChunkKvError::InvalidRequest(_))
    ));
}
