// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{
    decode_stream_publication_guard, StreamPublicationGuard, StreamPublicationId,
};
use crowdb_protocol::key::{BinaryKey, StreamPublicationGuardKey};

use super::{CollectionStage, DirectoryPageCollection};
use crate::directory::metadata::{kv_error, GetOutcome};
use crate::{DirectoryMetadataStore, Result, StreamError};

impl DirectoryMetadataStore {
    pub(super) async fn check_page_protections(
        &self,
        cursor: &mut DirectoryPageCollection,
        mut budget: usize,
    ) -> Result<bool> {
        let name = cursor.scope.stream_name;
        while budget > 0 {
            match cursor.stage {
                CollectionStage::Current => {
                    if self.page_reachable_from_root(cursor, cursor.current_root).await? {
                        return Ok(true);
                    }
                    budget -= 1;
                    cursor.stage = CollectionStage::Roots(None);
                }
                CollectionStage::Roots(after) => {
                    let page = self.scan_directory_roots(name, after, budget).await?;
                    for identity in &page.entries {
                        let head = self
                            .load_directory_root(name, *identity)
                            .await?
                            .ok_or(StreamError::Backpressure)?;
                        if self.page_reachable_from_head(cursor, &head).await? {
                            return Ok(true);
                        }
                    }
                    budget -= page.entries.len();
                    cursor.stage = page.next.map_or(CollectionStage::Pins(None), |after| {
                        CollectionStage::Roots(Some(after))
                    });
                }
                CollectionStage::Pins(after) => {
                    let page = self.scan_directory_pins(name, after, budget).await?;
                    for id in &page.entries {
                        let pin = self
                            .load_directory_pin(name, *id)
                            .await?
                            .ok_or(StreamError::Backpressure)?;
                        // Expired metadata remains conservative protection until
                        // lease, skew and outstanding read-IO grace are resolved.
                        if self.page_reachable_from_head(cursor, &pin.pin().head).await? {
                            return Ok(true);
                        }
                    }
                    budget -= page.entries.len();
                    cursor.stage = page.next.map_or(CollectionStage::Publications(None), |after| {
                        CollectionStage::Pins(Some(after))
                    });
                }
                CollectionStage::Publications(after) => {
                    let page = self.scan_directory_publications(name, after, budget).await?;
                    for id in &page.entries {
                        let guard = self.load_collection_guard(cursor, *id).await?;
                        if guard.pages.contains(&cursor.target.id) {
                            return Ok(true);
                        }
                        if let Some(base) = guard.base {
                            if self.page_reachable_from_head(cursor, &base).await? {
                                return Ok(true);
                            }
                        }
                    }
                    budget -= page.entries.len();
                    cursor.stage = page.next.map_or(CollectionStage::Delete, |after| {
                        CollectionStage::Publications(Some(after))
                    });
                }
                CollectionStage::Delete => break,
            }
        }
        Ok(false)
    }

    async fn load_collection_guard(
        &self,
        cursor: &DirectoryPageCollection,
        id: StreamPublicationId,
    ) -> Result<StreamPublicationGuard> {
        let key = StreamPublicationGuardKey {
            stream_name: cursor.scope.stream_name,
            publication_id: id,
        }
        .to_bytes();
        let GetOutcome::Found { value, .. } = self.records.get(&key).await.map_err(kv_error)? else {
            return Err(StreamError::Backpressure);
        };
        let guard = decode_stream_publication_guard(&value, id, cursor.scope.stream_name)
            .map_err(|error| StreamError::Corruption(error.to_string()))?;
        if guard.head.metadata_group_id != cursor.group_id || guard.head.purpose != cursor.scope.purpose {
            return Err(StreamError::Corruption(
                "publication protection scope is invalid".into(),
            ));
        }
        Ok(guard)
    }
}
