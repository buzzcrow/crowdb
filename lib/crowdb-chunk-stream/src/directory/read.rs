// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::ops::Range;

use super::{
    DirectoryIndex, Result, StreamDirectoryPage, StreamDirectoryStore, StreamError, StreamExtentLocation,
    MAX_DIRECTORY_MUTATION_EXTENTS,
};

pub struct DirectoryScan {
    pub locations: Vec<StreamExtentLocation>,
    /// Logical offset for the next scan against the same immutable root.
    pub next_logical: Option<u64>,
    pub pages_read: usize,
}

impl DirectoryIndex {
    /// # Errors
    /// Returns page IO/validation failures; a lookup loads at most height + 1 pages.
    pub async fn lookup(
        &self,
        store: &dyn StreamDirectoryStore,
        offset: u64,
    ) -> Result<Option<StreamExtentLocation>> {
        let mut next = self.root;
        while let Some(reference) = next {
            if offset < reference.first_logical || offset >= reference.end_logical {
                return Ok(None);
            }
            match self.load(store, reference).await? {
                StreamDirectoryPage::Leaf(leaf) => {
                    let index = leaf
                        .locations
                        .partition_point(|location| location.logical_start <= offset);
                    return Ok(index.checked_sub(1).map(|index| leaf.locations[index]));
                }
                StreamDirectoryPage::Branch(branch) => {
                    let index = branch
                        .children
                        .partition_point(|child| child.end_logical <= offset);
                    next = branch.children.get(index).copied();
                }
            }
        }
        Ok(None)
    }

    /// Read a bounded range without materializing the retained directory. Both
    /// result count and page IO have explicit budgets; continuation keeps this root.
    ///
    /// # Errors
    /// Rejects empty budgets/inverted ranges and propagates missing/corrupt page IO.
    pub async fn scan(
        &self,
        store: &dyn StreamDirectoryStore,
        range: Range<u64>,
        max_locations: usize,
        max_pages: usize,
    ) -> Result<DirectoryScan> {
        if range.start > range.end
            || max_locations == 0
            || max_pages == 0
            || max_locations > MAX_DIRECTORY_MUTATION_EXTENTS
            || self
                .root
                .is_some_and(|root| max_pages < usize::from(root.level) + 1)
        {
            return Err(StreamError::InvalidRequest(
                "invalid directory scan bounds".into(),
            ));
        }
        let mut result = DirectoryScan {
            locations: Vec::new(),
            next_logical: None,
            pages_read: 0,
        };
        if range.is_empty() {
            return Ok(result);
        }
        let mut pending = self.root.into_iter().collect::<Vec<_>>();
        while let Some(reference) = pending.pop() {
            if reference.first_logical >= range.end || reference.end_logical <= range.start {
                continue;
            }
            if result.pages_read == max_pages || result.locations.len() == max_locations {
                result.next_logical = Some(reference.first_logical.max(range.start));
                break;
            }
            let page = self.load(store, reference).await?;
            result.pages_read += 1;
            match page {
                StreamDirectoryPage::Branch(branch) => pending.extend(branch.children.into_iter().rev()),
                StreamDirectoryPage::Leaf(leaf) => {
                    for location in leaf.locations {
                        if location.logical_start >= range.end {
                            break;
                        }
                        if location.logical_start + location.location.logical_length <= range.start {
                            continue;
                        }
                        if result.locations.len() == max_locations {
                            result.next_logical = Some(location.logical_start);
                            return Ok(result);
                        }
                        result.locations.push(location);
                    }
                }
            }
        }
        Ok(result)
    }
}
