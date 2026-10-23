// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{StreamDirectoryHead, StreamDirectoryPage, StreamDirectoryRef};

use super::DirectoryPageCollection;
use crate::{DirectoryIndex, DirectoryMetadataStore, Result, StreamError};

impl DirectoryMetadataStore {
    pub(super) async fn page_reachable_from_head(
        &self,
        cursor: &DirectoryPageCollection,
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
        self.page_reachable_from_root(cursor, head.root).await
    }

    pub(super) async fn page_reachable_from_root(
        &self,
        cursor: &DirectoryPageCollection,
        root: Option<StreamDirectoryRef>,
    ) -> Result<bool> {
        let index = DirectoryIndex::new(cursor.scope.stream_name, cursor.scope.purpose, root)?;
        let target = cursor.target;
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
