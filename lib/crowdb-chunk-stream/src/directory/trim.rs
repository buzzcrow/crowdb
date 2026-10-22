// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{DirectoryIndex, DirectoryMutation, Result, StreamDirectoryPage, StreamDirectoryStore};

impl DirectoryIndex {
    /// Drop completed locations before a watermark by copying only the left path.
    /// A location crossing the watermark remains whole. Data release is governed
    /// separately by retained roots/pins and cannot follow from this mutation alone.
    ///
    /// # Errors
    /// Returns page IO/validation failures while traversing the retained left edge.
    pub async fn prepare_trim(
        &self,
        store: &dyn StreamDirectoryStore,
        watermark: u64,
    ) -> Result<DirectoryMutation> {
        let mut mutation = self.mutation();
        let Some(root) = self.root else {
            return Ok(mutation);
        };
        if watermark <= root.first_logical {
            return Ok(mutation);
        }
        if watermark >= root.end_logical {
            mutation.index.root = None;
            return Ok(mutation);
        }
        let mut path = Vec::new();
        let mut next = root;
        let mut replacement;
        loop {
            if next.first_logical >= watermark {
                replacement = next;
                break;
            }
            match self.load(store, next).await? {
                StreamDirectoryPage::Branch(mut branch) => {
                    let first = branch
                        .children
                        .partition_point(|child| child.end_logical <= watermark);
                    branch.children.drain(..first);
                    next = branch.children[0];
                    path.push(branch);
                }
                StreamDirectoryPage::Leaf(mut leaf) => {
                    let first = leaf.locations.partition_point(|location| {
                        location.logical_start + location.location.logical_length <= watermark
                    });
                    if first == 0 {
                        replacement = next;
                    } else {
                        leaf.locations.drain(..first);
                        replacement = mutation.leaf(leaf.locations)?;
                    }
                    break;
                }
            }
        }
        for mut branch in path.into_iter().rev() {
            branch.children[0] = replacement;
            replacement = mutation.branch(branch.level, branch.children)?;
        }
        mutation.index.root = Some(replacement);
        Ok(mutation)
    }
}
