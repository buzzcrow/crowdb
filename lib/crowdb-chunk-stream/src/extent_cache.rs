// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free extent-page snapshots shared by readers of one stream handle.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use crowdb_protocol::chunk_stream::{StreamExtentPage, StreamManifest};

#[derive(Clone)]
struct CacheSnapshot {
    writer_epoch: u64,
    generation: u64,
    pages: HashMap<u64, Arc<StreamExtentPage>>,
}

pub(crate) struct ExtentPageCache {
    snapshot: ArcSwap<CacheSnapshot>,
}

impl ExtentPageCache {
    pub(crate) fn new(manifest: &StreamManifest, pages: &[StreamExtentPage]) -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(CacheSnapshot {
                writer_epoch: manifest.writer_epoch,
                generation: manifest.generation,
                pages: pages
                    .iter()
                    .filter(|page| {
                        page.writer_epoch == manifest.writer_epoch && page.generation == manifest.generation
                    })
                    .map(|page| (page.page_index, Arc::new(page.clone())))
                    .collect(),
            }),
        }
    }

    pub(crate) fn get(
        &self,
        writer_epoch: u64,
        generation: u64,
        page_index: u64,
    ) -> Option<Arc<StreamExtentPage>> {
        let snapshot = self.snapshot.load();
        (snapshot.writer_epoch == writer_epoch && snapshot.generation == generation)
            .then(|| snapshot.pages.get(&page_index).cloned())
            .flatten()
    }

    pub(crate) fn insert(&self, page: StreamExtentPage) -> Arc<StreamExtentPage> {
        let page = Arc::new(page);
        let inserted = Arc::clone(&page);
        self.snapshot.rcu(move |current| {
            let current_identity = (current.writer_epoch, current.generation);
            let page_identity = (page.writer_epoch, page.generation);
            if page_identity < current_identity {
                return Arc::clone(current);
            }
            let mut pages = if page_identity == current_identity {
                current.pages.clone()
            } else {
                HashMap::new()
            };
            pages.insert(page.page_index, Arc::clone(&page));
            Arc::new(CacheSnapshot {
                writer_epoch: page.writer_epoch,
                generation: page.generation,
                pages,
            })
        });
        inserted
    }
}
