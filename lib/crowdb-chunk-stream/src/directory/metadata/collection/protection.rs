// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{StreamPublicationGuard, StreamPublicationId};

use super::{CollectionStage, DirectoryCollection};
use crate::{DirectoryMetadataStore, Result, StreamError};

impl DirectoryMetadataStore {
    pub(super) async fn check_collection_protections(
        &self,
        cursor: &mut DirectoryCollection,
        mut budget: usize,
    ) -> Result<bool> {
        let name = cursor.scope.stream_name;
        while budget > 0 {
            match cursor.stage {
                CollectionStage::Current => {
                    if self.collection_protected_by_current(cursor).await? {
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
                        if self.collection_protected_by_head(cursor, &head).await? {
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
                        if self.collection_protected_by_pin(cursor, pin.pin()).await? {
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
                        if self.collection_protected_by_guard(cursor, &guard).await? {
                            return Ok(true);
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
        cursor: &DirectoryCollection,
        id: StreamPublicationId,
    ) -> Result<StreamPublicationGuard> {
        let guard = self
            .load_directory_publication(cursor.scope.stream_name, id)
            .await?
            .ok_or(StreamError::Backpressure)?;
        if guard.head.metadata_group_id != cursor.group_id || guard.head.purpose != cursor.scope.purpose {
            return Err(StreamError::Corruption(
                "publication protection scope is invalid".into(),
            ));
        }
        Ok(guard)
    }
}
