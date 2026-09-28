use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use crowdb_chunk_client::{IoError, SmallWriteIntent};
use crowdb_protocol::chunkdb::rpc::Location;
use sha2::{Digest, Sha256};

use super::super::{ChunkRoot, FileIdentity, FileIoError, FileWriteIntent};
use crate::{catalog::CatalogStore, error::ValidationError, key::OperationId};

pub(super) struct BlockIntent {
    store: Arc<dyn CatalogStore>,
    owner: FileIdentity,
    identity: OperationId,
    height: u8,
    pub(super) digest: [u8; 32],
    length: u64,
    created_ms: u64,
}

impl BlockIntent {
    pub(super) fn new(
        store: Arc<dyn CatalogStore>,
        owner: FileIdentity,
        height: u8,
        bytes: &[u8],
    ) -> Result<Self, FileIoError> {
        let created_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| ValidationError::Deadline)?
                .as_millis(),
        )
        .map_err(|_| ValidationError::Deadline)?;
        Ok(Self {
            store,
            owner,
            identity: OperationId::random(),
            height,
            digest: Sha256::digest(bytes).into(),
            length: bytes.len() as u64,
            created_ms,
        })
    }
}

#[async_trait]
impl SmallWriteIntent for BlockIntent {
    async fn before_write(&self, location: &Location) -> crowdb_chunk_client::Result<()> {
        if location.logical_offset != 0 || location.logical_length != self.length {
            return Err(IoError::MetadataConflict("block intent location mismatch".into()));
        }
        let intent = FileWriteIntent {
            identity: self.identity,
            owner: self.owner,
            created_ms: self.created_ms,
            not_before_ms: 0,
            deleting: false,
            root: ChunkRoot {
                chunk: location
                    .chunk_id
                    .ok_or_else(|| IoError::MetadataConflict("block intent missing chunk".into()))?,
                offset: location.offset,
                physical_length: location.length,
                logical_offset: location.logical_offset,
                logical_length: location.logical_length,
                height: self.height,
                digest: self.digest,
            },
        };
        intent.register(self.store.clone()).await.map_err(|error| {
            tracing::error!(catalog = %self.owner.table.catalog, file = %self.owner.file, %error,
                "file block intent failed; aborting physical batch and retaining uncertain ownership");
            IoError::WriteFailed(error.to_string())
        })
    }
}
