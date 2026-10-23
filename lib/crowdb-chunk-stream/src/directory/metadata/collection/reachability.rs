// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{
    StreamDirectoryHead, StreamDirectoryPage, StreamDirectoryRef, StreamPublicationGuard, StreamRootPin,
};

use super::{CollectionTarget, DirectoryCollection};
use crate::{DirectoryIndex, DirectoryMetadataStore, Result, StreamError};

impl DirectoryMetadataStore {
    pub(super) async fn collection_protected_by_current(&self, cursor: &DirectoryCollection) -> Result<bool> {
        match cursor.target {
            CollectionTarget::Page(_) => self.page_reachable_from_root(cursor, cursor.current_root).await,
            CollectionTarget::Chunk(span) => Ok(span.logical_end > cursor.current_trim
                || cursor.current_active == Some(span.chunk_id)
                || span_overlaps(span, cursor.current_trim, cursor.current_tail)),
        }
    }

    pub(super) async fn collection_protected_by_pin(
        &self,
        cursor: &DirectoryCollection,
        pin: &StreamRootPin,
    ) -> Result<bool> {
        if pin.head.purpose != cursor.scope.purpose {
            return Err(StreamError::Corruption(
                "pin collection purpose is invalid".into(),
            ));
        }
        match cursor.target {
            CollectionTarget::Page(_) => self.collection_protected_by_head(cursor, &pin.head).await,
            CollectionTarget::Chunk(span) => Ok(span_overlaps(span, pin.needed_offset, pin.captured_tail)),
        }
    }

    pub(super) async fn collection_protected_by_guard(
        &self,
        cursor: &DirectoryCollection,
        guard: &StreamPublicationGuard,
    ) -> Result<bool> {
        let retained = match cursor.target {
            CollectionTarget::Page(target) => guard.pages.contains(&target.id),
            CollectionTarget::Chunk(_) => self.collection_protected_by_head(cursor, &guard.head).await?,
        };
        if retained {
            return Ok(true);
        }
        match &guard.base {
            Some(base) => self.collection_protected_by_head(cursor, base).await,
            None => Ok(false),
        }
    }

    pub(super) async fn collection_protected_by_head(
        &self,
        cursor: &DirectoryCollection,
        head: &StreamDirectoryHead,
    ) -> Result<bool> {
        if head.stream_name != cursor.scope.stream_name
            || head.purpose != cursor.scope.purpose
            || head.metadata_group_id != cursor.group_id
        {
            return Err(StreamError::Corruption(
                "page protection head scope is invalid".into(),
            ));
        }
        match cursor.target {
            CollectionTarget::Page(_) => self.page_reachable_from_root(cursor, head.root).await,
            CollectionTarget::Chunk(span) => {
                Ok(head.active.is_some_and(|active| active.chunk_id == span.chunk_id)
                    || span_overlaps(span, head.trim_offset, head.published_tail))
            }
        }
    }

    pub(super) async fn page_reachable_from_root(
        &self,
        cursor: &DirectoryCollection,
        root: Option<StreamDirectoryRef>,
    ) -> Result<bool> {
        let index = DirectoryIndex::new(cursor.scope.stream_name, cursor.scope.purpose, root)?;
        let CollectionTarget::Page(target) = cursor.target else {
            unreachable!()
        };
        let mut next = root;
        while let Some(reference) = next {
            if reference.id == target.id {
                return if reference == target {
                    Ok(true)
                } else {
                    Err(StreamError::Corruption(
                        "page protection fence differs from content".into(),
                    ))
                };
            }
            if reference.level <= target.level
                || reference.first_logical > target.first_logical
                || reference.end_logical < target.end_logical
            {
                return Ok(false);
            }
            let StreamDirectoryPage::Branch(branch) = index.load(self, reference).await? else {
                return Err(StreamError::Corruption(
                    "page protection path has an invalid level".into(),
                ));
            };
            next = branch.children.into_iter().find(|child| {
                child.id == target.id
                    || (child.first_logical <= target.first_logical
                        && child.end_logical >= target.end_logical)
            });
        }
        Ok(false)
    }
}

pub(super) const fn span_overlaps(
    span: crowdb_protocol::chunk_stream::StreamSealedChunkSpan,
    start: u64,
    end: u64,
) -> bool {
    start < end
        && span.logical_start < span.logical_end
        && start < span.logical_end
        && end > span.logical_start
}
