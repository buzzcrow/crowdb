use crowdb_protocol::chunkdb::rpc::{DeleteChunkRangeRequest, Location};

use crate::{ChunkAllocator, IoError, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReclaimOutcome {
    /// ChunkDB accepted durable cleanup ownership, or the chunk is already gone.
    /// This does not report physical capacity reclaimed by DiskDB.
    Accepted,
    /// ChunkDB has not accepted this notification yet.
    Deferred,
}

pub async fn reclaim_location(allocator: &dyn ChunkAllocator, location: &Location) -> Result<ReclaimOutcome> {
    let chunk_id = location
        .chunk_id
        .filter(|identity| *identity != crowdb_protocol::common::ChunkId::default())
        .ok_or_else(|| IoError::MetadataConflict("missing reclamation chunk identity".into()))?;
    let end = location
        .offset
        .checked_add(location.length)
        .filter(|_| location.length > 0)
        .ok_or_else(|| IoError::MetadataConflict("invalid reclamation range".into()))?;
    u32::try_from(end).map_err(|_| IoError::MetadataConflict("range end exceeds protocol".into()))?;
    let request = DeleteChunkRangeRequest {
        chunk_id: Some(chunk_id),
        chunk_offset: u32::try_from(location.offset)
            .map_err(|_| IoError::MetadataConflict("range offset exceeds protocol".into()))?,
        chunk_size: u32::try_from(location.length)
            .map_err(|_| IoError::MetadataConflict("range length exceeds protocol".into()))?,
        // ChunkDB qualifies the current immutable chunk identity and logical
        // extent, then conditions retirement on its own atomic layout read.
        expected_modify_ts: 0,
    };
    match allocator.delete_chunk_range(request).await {
        Ok(_) | Err(IoError::ChunkNotFound(_)) => Ok(ReclaimOutcome::Accepted),
        Err(IoError::Unsupported(_) | IoError::MetadataConflict(_)) => Ok(ReclaimOutcome::Deferred),
        Err(error) => Err(error),
    }
}
