// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    checked_join, invalid_directory, DirectoryIndex, DirectoryMutation, Result, StreamDirectoryBranch,
    StreamDirectoryPage, StreamDirectoryRef, StreamDirectoryStore, StreamError, StreamExtentLocation,
    MAX_DIRECTORY_MUTATION_EXTENTS, MAX_STREAM_DIRECTORY_CHILDREN, MAX_STREAM_DIRECTORY_HEIGHT,
    MAX_STREAM_DIRECTORY_LOCATIONS,
};

impl DirectoryIndex {
    /// Copy only the unfinished leaf and its ancestor path; completed pages retain
    /// their identities. Input is a bounded checkpoint of complete append groups.
    ///
    /// # Errors
    /// Rejects gaps, invalid geometry, missing/corrupt pages, or an oversized checkpoint.
    pub async fn prepare_append(
        &self,
        store: &dyn StreamDirectoryStore,
        locations: &[StreamExtentLocation],
    ) -> Result<DirectoryMutation> {
        if locations.len() > MAX_DIRECTORY_MUTATION_EXTENTS {
            return Err(StreamError::Backpressure);
        }
        let mut mutation = self.mutation();
        if locations.is_empty() {
            return Ok(mutation);
        }
        self.check_locations(locations)?;
        let (path, previous) = self.right_path(store).await?;
        let mut replacements = append_leaves(&mut mutation, previous, locations)?;
        for (reference, branch) in path.into_iter().rev() {
            replacements = append_branches(&mut mutation, reference, branch, replacements)?;
        }
        while replacements.len() > 1 {
            let level = replacements[0].level + 1;
            if level > MAX_STREAM_DIRECTORY_HEIGHT {
                return Err(invalid_directory("directory height limit exceeded"));
            }
            replacements = pack_branches(&mut mutation, level, &replacements)?;
        }
        mutation.index.root = replacements.first().copied();
        Ok(mutation)
    }

    fn check_locations(&self, locations: &[StreamExtentLocation]) -> Result<()> {
        if self
            .root
            .is_some_and(|root| root.end_logical != locations[0].logical_start)
        {
            return Err(invalid_directory("checkpoint does not extend the root"));
        }
        for location in locations {
            if !self.purpose.matches(location.location.chunk_id) {
                return Err(invalid_directory("checkpoint has the wrong chunk purpose"));
            }
            location.end_logical().map_err(invalid_directory)?;
            location.end_physical().map_err(invalid_directory)?;
        }
        for pair in locations.windows(2) {
            checked_join(pair[0], pair[1])?;
        }
        Ok(())
    }

    async fn right_path(
        &self,
        store: &dyn StreamDirectoryStore,
    ) -> Result<(
        Vec<(StreamDirectoryRef, StreamDirectoryBranch)>,
        Option<(StreamDirectoryRef, Vec<StreamExtentLocation>)>,
    )> {
        let mut path = Vec::new();
        let mut next = self.root;
        while let Some(reference) = next {
            match self.load(store, reference).await? {
                StreamDirectoryPage::Leaf(leaf) => return Ok((path, Some((reference, leaf.locations)))),
                StreamDirectoryPage::Branch(branch) => {
                    next = branch.children.last().copied();
                    path.push((reference, branch));
                }
            }
        }
        Ok((path, None))
    }
}

fn append_leaves(
    mutation: &mut DirectoryMutation,
    previous: Option<(StreamDirectoryRef, Vec<StreamExtentLocation>)>,
    locations: &[StreamExtentLocation],
) -> Result<Vec<StreamDirectoryRef>> {
    let mut replacements = Vec::new();
    let mut tail = Vec::with_capacity(MAX_STREAM_DIRECTORY_LOCATIONS);
    if let Some((reference, old)) = previous {
        checked_join(
            *old.last().ok_or_else(|| invalid_directory("empty leaf"))?,
            locations[0],
        )?;
        if old.len() == MAX_STREAM_DIRECTORY_LOCATIONS {
            replacements.push(reference);
        } else {
            tail = old;
        }
    }
    for location in locations {
        tail.push(*location);
        if tail.len() == MAX_STREAM_DIRECTORY_LOCATIONS {
            replacements.push(mutation.leaf(std::mem::take(&mut tail))?);
        }
    }
    if !tail.is_empty() {
        replacements.push(mutation.leaf(tail)?);
    }
    Ok(replacements)
}

fn append_branches(
    mutation: &mut DirectoryMutation,
    reference: StreamDirectoryRef,
    mut branch: StreamDirectoryBranch,
    replacements: Vec<StreamDirectoryRef>,
) -> Result<Vec<StreamDirectoryRef>> {
    if branch.children.len() == MAX_STREAM_DIRECTORY_CHILDREN
        && branch.children.last() == replacements.first()
    {
        let mut result = vec![reference];
        result.extend(pack_branches(mutation, branch.level, &replacements[1..])?);
        return Ok(result);
    }
    branch.children.pop();
    branch.children.extend(replacements);
    pack_branches(mutation, branch.level, &branch.children)
}

fn pack_branches(
    mutation: &mut DirectoryMutation,
    level: u8,
    children: &[StreamDirectoryRef],
) -> Result<Vec<StreamDirectoryRef>> {
    children
        .chunks(MAX_STREAM_DIRECTORY_CHILDREN)
        .map(|children| mutation.branch(level, children.to_vec()))
        .collect()
}
