use crowdb_protocol::chunkdb::rpc::{
    ChunkState, DeleteChunkRangeRequest, DeleteChunkRequest, Location, QueryChunkRequest,
};

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
    let chunk = match allocator
        .query_chunk(QueryChunkRequest {
            chunk_id: Some(chunk_id),
        })
        .await
    {
        Ok(response) => response
            .chunk
            .ok_or_else(|| IoError::MetadataConflict("missing chunk response".into()))?,
        Err(IoError::ChunkNotFound(_)) => return Ok(ReclaimOutcome::Accepted),
        Err(error) => return Err(error),
    };
    if chunk.id != Some(chunk_id) {
        return Err(IoError::MetadataConflict(
            "reclamation chunk identity mismatch".into(),
        ));
    }
    if chunk.writer_epoch != 0 {
        u32::try_from(end).map_err(|_| IoError::MetadataConflict("range end exceeds protocol".into()))?;
        if chunk.modify_ts == 0
            || chunk.acknowledged_cursor < end
            || !matches!(
                ChunkState::try_from(chunk.state),
                Ok(ChunkState::Sealed | ChunkState::Deleted)
            )
        {
            return Ok(ReclaimOutcome::Deferred);
        }
        let request = DeleteChunkRangeRequest {
            chunk_id: Some(chunk_id),
            chunk_offset: u32::try_from(location.offset)
                .map_err(|_| IoError::MetadataConflict("range offset exceeds protocol".into()))?,
            chunk_size: u32::try_from(location.length)
                .map_err(|_| IoError::MetadataConflict("range length exceeds protocol".into()))?,
            expected_modify_ts: chunk.modify_ts,
        };
        return match allocator.delete_chunk_range(request).await {
            Ok(_) | Err(IoError::ChunkNotFound(_)) => Ok(ReclaimOutcome::Accepted),
            Err(IoError::Unsupported(_)) => Ok(ReclaimOutcome::Deferred),
            Err(error) => Err(error),
        };
    }
    if location.offset != 0
        || end.div_ceil(1024) != u64::from(chunk.sealed_length)
        || !matches!(
            ChunkState::try_from(chunk.state),
            Ok(ChunkState::Sealed | ChunkState::Deleted)
        )
    {
        return Err(IoError::MetadataConflict(
            "range does not own a sealed dedicated chunk".into(),
        ));
    }
    let deleted = match allocator
        .delete_chunk(DeleteChunkRequest {
            chunk_id: Some(chunk_id),
        })
        .await
    {
        Ok(response) => response
            .chunk
            .ok_or_else(|| IoError::MetadataConflict("missing deletion response".into()))?,
        Err(IoError::ChunkNotFound(_)) => return Ok(ReclaimOutcome::Accepted),
        Err(error) => return Err(error),
    };
    if deleted.id != Some(chunk_id) || deleted.state != ChunkState::Deleted as i32 {
        return Ok(ReclaimOutcome::Deferred);
    }
    Ok(ReclaimOutcome::Accepted)
}
