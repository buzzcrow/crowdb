// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Preserve the primary fixture chunk while permitting independent successors.

use std::{
    ops::{Deref, DerefMut},
    sync::MutexGuard,
};

use super::{Allocator, Chunk, ChunkId, HashMap};

pub(super) struct TestChunkGuard<'a> {
    primary: MutexGuard<'a, Option<Chunk>>,
    others: MutexGuard<'a, HashMap<ChunkId, Chunk>>,
    id: ChunkId,
}

impl Allocator {
    pub(super) fn chunk_for(&self, id: Option<ChunkId>) -> TestChunkGuard<'_> {
        TestChunkGuard {
            primary: self.chunk.lock().unwrap(),
            others: self.successors.lock().unwrap(),
            id: id.unwrap(),
        }
    }
}

impl Deref for TestChunkGuard<'_> {
    type Target = Chunk;
    fn deref(&self) -> &Self::Target {
        if self.primary.as_ref().and_then(|chunk| chunk.id) == Some(self.id) {
            self.primary.as_ref().unwrap()
        } else {
            self.others.get(&self.id).unwrap()
        }
    }
}

impl DerefMut for TestChunkGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        if self.primary.as_ref().and_then(|chunk| chunk.id) == Some(self.id) {
            self.primary.as_mut().unwrap()
        } else {
            self.others.get_mut(&self.id).unwrap()
        }
    }
}
