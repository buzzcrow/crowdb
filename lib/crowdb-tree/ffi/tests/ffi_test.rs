// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// PT8.5: C ABI / Rust integration tests through the safe adapter.
use crowdb_tree_ffi::{
    AsyncCrowdbtree, BatchOp, ChunkPageStoreOptions, ChunkRootCatalog, Config, Crowdbtree, CtError, ExtOp,
    KeyRange, PageStore, PageStoreBackend, PinnedGetOutcome, RootCatalogObject, RootCatalogStore,
    ScanDirection,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

struct FileRootCatalogStore {
    dir: PathBuf,
    generation: AtomicU64,
    next_reference: AtomicU64,
    hide_current: AtomicBool,
}

impl FileRootCatalogStore {
    fn path(&self, tree_id: u64, object: RootCatalogObject) -> PathBuf {
        let suffix = match object {
            RootCatalogObject::CurrentManifest => "current".to_owned(),
            RootCatalogObject::Manifest(generation) => format!("manifest-{generation}"),
            RootCatalogObject::ReferenceSegment(id) => format!("reference-{id}"),
        };
        self.dir.join(format!("{tree_id}-{suffix}"))
    }

    fn pin_path(&self, tree_id: u64, transition_high: u64, transition_low: u64) -> PathBuf {
        self.dir.join(format!(
            "{tree_id}-pin-{transition_high:016x}{transition_low:016x}"
        ))
    }
}

impl RootCatalogStore for FileRootCatalogStore {
    fn load(&self, tree_id: u64, object: RootCatalogObject) -> Result<Option<Vec<u8>>, CtError> {
        if object == RootCatalogObject::CurrentManifest && self.hide_current.load(Ordering::Acquire) {
            return Ok(None);
        }
        match std::fs::read(self.path(tree_id, object)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(CtError::IoError),
        }
    }

    fn store(&self, tree_id: u64, object: RootCatalogObject, data: &[u8]) -> Result<(), CtError> {
        std::fs::write(self.path(tree_id, object), data).map_err(|_| CtError::IoError)
    }

    fn publish(
        &self,
        tree_id: u64,
        expected_generation: u64,
        _owner_epoch: u64,
        generation: u64,
        manifest: &[u8],
    ) -> Result<(), CtError> {
        self.generation
            .compare_exchange(
                expected_generation,
                generation,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| CtError::Unavailable)?;
        self.store(tree_id, RootCatalogObject::Manifest(generation), manifest)?;
        self.store(tree_id, RootCatalogObject::CurrentManifest, manifest)
    }

    fn allocate_reference_segment_id(&self, _tree_id: u64) -> Result<u64, CtError> {
        Ok(self.next_reference.fetch_add(1, Ordering::Relaxed))
    }

    fn pin_generation(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
        generation: u64,
    ) -> Result<(), CtError> {
        if self
            .load(tree_id, RootCatalogObject::Manifest(generation))?
            .is_none()
        {
            return Err(CtError::NotFound);
        }
        let path = self.pin_path(tree_id, transition_high, transition_low);
        match std::fs::read(&path) {
            Ok(value) if value == generation.to_be_bytes() => Ok(()),
            Ok(_) => Err(CtError::InvalidArgument),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::write(path, generation.to_be_bytes()).map_err(|_| CtError::IoError)
            }
            Err(_) => Err(CtError::IoError),
        }
    }

    fn unpin_generation(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
    ) -> Result<(), CtError> {
        match std::fs::remove_file(self.pin_path(tree_id, transition_high, transition_low)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(CtError::IoError),
        }
    }
}

#[cfg(feature = "chunk-rpc")]
use crowdb_rpc_ffi::{OwnedClientRoute, RpcClient, RpcServer};
#[cfg(feature = "chunk-rpc")]
use crowdb_tree_ffi::{OwnedChunkRpcDiskRoute, OwnedChunkRpcTransportOptions};

fn key(i: usize) -> Vec<u8> {
    format!("key{i:05}").into_bytes()
}

#[cfg(feature = "chunk-rpc")]
#[test]
fn owned_chunk_rpc_transport_retains_route_handles() {
    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).unwrap();
    server.start();
    let connection = server.connect("127.0.0.1", server.port()).unwrap();
    let client = Arc::new(RpcClient::new());
    client.attach(&connection);
    let route = OwnedClientRoute::new(client, server, connection);

    let transport = crowdb_tree_ffi::ChunkTransport::open_owned_rpc(OwnedChunkRpcTransportOptions {
        chunkdb: route.clone(),
        disk_routes: vec![OwnedChunkRpcDiskRoute {
            disk_id_high: 1,
            disk_id_low: 2,
            route,
        }],
        writer_lease_ms: 30_000,
        rpc_timeout_ms: 1_000,
        completion_capacity: 32,
    })
    .unwrap();
    drop(transport);
}

#[test]
fn mem_apply_get_scan() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    for i in 0..40usize {
        let v = format!("v{i}").into_bytes();
        t.apply_put((i + 1) as u64, &key(i), &v).unwrap();
    }
    t.flush().unwrap();
    // Point read.
    let got = t.get(&key(5)).unwrap();
    assert_eq!(got, Some((6u64, b"v5".to_vec())));

    // Delete.
    t.apply_delete(1000, &key(7)).unwrap();
    t.force_advance_slot(1000);
    t.flush().unwrap();
    assert_eq!(t.get(&key(7)).unwrap(), None);

    // Scan all live entries.
    let (entries, truncated) = t.scan(b"", b"", b"", 0, 0, false, 0, false).unwrap();
    assert!(!truncated);
    assert_eq!(entries.len(), 39); // 40 puts - 1 delete
    assert!(entries.windows(2).all(|w| w[0].key < w[1].key)); // key-sorted
}

#[test]
fn scan_from_honors_inclusive_and_exclusive_lower_bounds() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    for (slot, key) in [b"".as_slice(), b"b"].into_iter().enumerate() {
        tree.apply_put(slot as u64 + 1, key, b"value").unwrap();
    }
    tree.flush().unwrap();
    tree.apply_put(3, b"d", b"value").unwrap();

    let (inclusive, _) = tree
        .scan_from(b"", b"b", true, b"", 1, 1024, false, 0, false)
        .unwrap();
    assert_eq!(inclusive[0].key.as_ref(), b"b");

    let (exclusive, _) = tree
        .scan_from(b"", b"b", false, b"", 1, 1024, false, 0, false)
        .unwrap();
    assert_eq!(exclusive[0].key.as_ref(), b"d");

    let (after_empty, _) = tree
        .scan_from(b"", b"", false, b"", 1, 1024, false, 0, false)
        .unwrap();
    assert_eq!(after_empty[0].key.as_ref(), b"b");
}

#[test]
fn scan_start_after_pages_with_the_unified_scan_ffi() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    for index in 0..300_u64 {
        tree.apply_put(index + 1, format!("key-{index:04}").as_bytes(), b"value")
            .unwrap();
    }
    tree.flush().unwrap();
    let (first, truncated) = tree
        .scan(b"", b"", b"", 256, 1024 * 1024, false, 0, false)
        .unwrap();
    assert!(truncated);
    assert_eq!(first.len(), 256);
    let cursor = first.last().unwrap().key.clone();
    let (second, truncated) = tree
        .scan(b"", &cursor, b"", 256, 1024 * 1024, false, 0, false)
        .unwrap();
    assert!(!truncated);
    assert_eq!(second.len(), 44);
    assert!(second.iter().all(|entry| entry.key > cursor));
}

#[test]
fn reverse_seek_merges_l0_l1_and_tombstones() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    for (slot, key) in [b"".as_slice(), b"b", b"d"].into_iter().enumerate() {
        tree.apply_put(slot as u64 + 1, key, b"value").unwrap();
    }
    tree.flush().unwrap();
    tree.apply_put(4, b"e", b"latest").unwrap();

    assert_eq!(
        tree.seek_reverse(b"d", true, b"").unwrap().unwrap().key.as_ref(),
        b"d"
    );
    assert_eq!(
        tree.seek_reverse(b"d", false, b"").unwrap().unwrap().key.as_ref(),
        b"b"
    );
    assert_eq!(
        tree.seek_reverse(b"e", true, b"").unwrap().unwrap().key.as_ref(),
        b"e"
    );
    assert_eq!(
        tree.seek_reverse(b"", true, b"").unwrap().unwrap().key.as_ref(),
        b""
    );
    assert!(tree.seek_reverse(b"", false, b"").unwrap().is_none());

    tree.apply_delete(5, b"d").unwrap();
    assert_eq!(
        tree.seek_reverse(b"d", true, b"").unwrap().unwrap().key.as_ref(),
        b"b"
    );
    assert!(tree.seek_reverse(b"d", true, b"c").unwrap().is_none());
}

#[test]
fn reverse_scan_is_descending_bounded_and_tombstone_aware() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    for (slot, key) in [b"a", b"b", b"c", b"d"].into_iter().enumerate() {
        tree.apply_put(slot as u64 + 1, key, key).unwrap();
    }
    tree.flush().unwrap();
    tree.apply_put(5, b"e", b"e").unwrap();
    tree.apply_delete(6, b"d").unwrap();

    let (page, truncated) = tree.scan_reverse(Some(b"e"), true, b"b", 2, 1024).unwrap();
    assert_eq!(
        page.iter().map(|entry| entry.key.as_ref()).collect::<Vec<_>>(),
        vec![b"e", b"c"]
    );
    assert!(truncated);

    let (page, truncated) = tree.scan_reverse(Some(b"e"), false, b"b", 10, 1024).unwrap();
    assert_eq!(
        page.iter().map(|entry| entry.key.as_ref()).collect::<Vec<_>>(),
        vec![b"c", b"b"]
    );
    assert!(!truncated);

    let (page, truncated) = tree.scan_reverse(None, false, b"b", 10, 1024).unwrap();
    assert_eq!(
        page.iter().map(|entry| entry.key.as_ref()).collect::<Vec<_>>(),
        vec![b"e", b"c", b"b"]
    );
    assert!(!truncated);
}

#[test]
fn reverse_scan_crosses_leaf_boundaries() {
    let tree = Crowdbtree::open(&Config {
        frame_bytes: 4096,
        ..Config::default()
    })
    .unwrap();
    for index in 0..400_u64 {
        let key = format!("key-{index:04}");
        tree.apply_put(index + 1, key.as_bytes(), b"0123456789abcdef")
            .unwrap();
    }
    tree.flush().unwrap();

    let (entries, truncated) = tree.scan_reverse(None, false, b"", 500, 1 << 20).unwrap();
    assert!(!truncated);
    assert_eq!(entries.len(), 400);
    assert!(entries.windows(2).all(|pair| pair[0].key > pair[1].key));
    assert_eq!(entries.first().unwrap().key.as_ref(), b"key-0399");
    assert_eq!(entries.last().unwrap().key.as_ref(), b"key-0000");
}

#[test]
fn injected_chunk_store_round_trip_and_stats() {
    let catalog = Arc::new(ChunkRootCatalog::open_memory(7).unwrap());
    let store = Arc::new(
        PageStore::open_chunk(
            ChunkPageStoreOptions {
                tree_id: 41,
                owner_epoch: 7,
                open_generation: 0,
                pack_bytes: 4096,
                iu_size: 1,
                max_concurrent_packs: 2,
                materialization_bytes_per_pass: 4096,
            },
            Arc::clone(&catalog),
            None,
        )
        .unwrap(),
    );
    let tree = Crowdbtree::open(&Config {
        page_store: Some(Arc::clone(&store)),
        ..Config::default()
    })
    .unwrap();
    tree.apply_put(1, b"chunk-key", b"chunk-value").unwrap();
    let published = tree.snapshot_info().unwrap();
    assert_eq!(published.0, 1);
    assert_eq!(tree.snapshot_state().unwrap(), published);
    assert_eq!(tree.snapshot_state().unwrap(), published);
    assert_eq!(
        tree.get(b"chunk-key").unwrap(),
        Some((1, b"chunk-value".to_vec()))
    );
    let stats = store.chunk_stats().unwrap();
    assert_eq!(stats.generations_published, 1);
    assert!(stats.packs_written > 0);
    assert!(stats.pack_bytes_written > 0);
    assert!(stats.diskio_operations > 0);
    assert!(stats.rpc_operations > 0);
    assert_eq!(tree.materialize_ownership().unwrap(), (0, true));
}

#[test]
fn callback_root_catalog_reopens_published_manifest() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-callback-root");
    let backend = Arc::new(FileRootCatalogStore {
        dir: dir.path().to_path_buf(),
        generation: AtomicU64::new(0),
        next_reference: AtomicU64::new(1),
        hide_current: AtomicBool::new(false),
    });
    let catalog = Arc::new(ChunkRootCatalog::open_callback(backend).unwrap());
    let options = ChunkPageStoreOptions {
        tree_id: 42,
        owner_epoch: 9,
        open_generation: 0,
        pack_bytes: 4096,
        iu_size: 1,
        max_concurrent_packs: 2,
        materialization_bytes_per_pass: 4096,
    };
    let store = Arc::new(PageStore::open_chunk(options, Arc::clone(&catalog), None).unwrap());
    {
        store.set_wal_replay_offset(4_096).unwrap();
        assert_eq!(store.set_wal_replay_offset(4_095), Err(CtError::InvalidArgument));
        let tree = Crowdbtree::open(&Config {
            page_store: Some(Arc::clone(&store)),
            ..Config::default()
        })
        .unwrap();
        tree.apply_put(1, b"durable-root", b"chunk-value").unwrap();
        tree.flush().unwrap();
        assert_eq!(tree.snapshot_info().unwrap(), (1, 1));
    }

    let reopened_store = Arc::new(PageStore::open_chunk(options, catalog, None).unwrap());
    assert_eq!(reopened_store.wal_replay_offset().unwrap(), 4_096);
    assert_eq!(
        reopened_store.set_wal_replay_offset(4_095),
        Err(CtError::InvalidArgument)
    );
    let reopened = Crowdbtree::open(&Config {
        page_store: Some(reopened_store),
        ..Config::default()
    })
    .unwrap();
    assert_eq!(
        reopened.get(b"durable-root").unwrap(),
        Some((1, b"chunk-value".to_vec()))
    );
}

#[test]
fn callback_root_catalog_persists_transition_generation_pin() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-callback-root-pin");
    let backend = Arc::new(FileRootCatalogStore {
        dir: dir.path().to_path_buf(),
        generation: AtomicU64::new(0),
        next_reference: AtomicU64::new(1),
        hide_current: AtomicBool::new(false),
    });
    let catalog = Arc::new(ChunkRootCatalog::open_callback(backend.clone()).unwrap());
    let options = ChunkPageStoreOptions {
        tree_id: 43,
        owner_epoch: 9,
        open_generation: 0,
        pack_bytes: 4096,
        iu_size: 1,
        max_concurrent_packs: 2,
        materialization_bytes_per_pass: 4096,
    };
    let store = Arc::new(PageStore::open_chunk(options, Arc::clone(&catalog), None).unwrap());
    let tree = Crowdbtree::open(&Config {
        page_store: Some(Arc::clone(&store)),
        ..Config::default()
    })
    .unwrap();
    tree.apply_put(1, b"key", b"value").unwrap();
    tree.flush().unwrap();
    tree.snapshot_info().unwrap();
    let generation = store.chunk_manifest_generation().unwrap();
    store.pin_chunk_generation(43, 7, 8, generation).unwrap();
    drop(tree);
    drop(store);
    drop(catalog);

    let reopened = ChunkRootCatalog::open_callback(backend).unwrap();
    reopened.pin_generation(43, 7, 8, generation).unwrap();
    assert_eq!(
        reopened.pin_generation(43, 7, 8, generation + 1),
        Err(CtError::NotFound)
    );
    reopened.unpin_generation(43, 7, 8).unwrap();
    reopened.unpin_generation(43, 7, 8).unwrap();
}

#[test]
fn memory_root_catalog_pin_blocks_generation_reclaim_until_unpin() {
    let catalog = Arc::new(ChunkRootCatalog::open_memory(9).unwrap());
    let options = ChunkPageStoreOptions {
        tree_id: 45,
        owner_epoch: 9,
        open_generation: 0,
        pack_bytes: 4096,
        iu_size: 1,
        max_concurrent_packs: 2,
        materialization_bytes_per_pass: 4096,
    };
    let store = Arc::new(PageStore::open_chunk(options, Arc::clone(&catalog), None).unwrap());
    let tree = Crowdbtree::open(&Config {
        page_store: Some(Arc::clone(&store)),
        ..Config::default()
    })
    .unwrap();
    for sequence in 1..=3 {
        tree.apply_put(sequence, b"key", &[sequence as u8]).unwrap();
        tree.flush().unwrap();
        tree.snapshot_info().unwrap();
        if sequence == 1 {
            store.pin_chunk_generation(45, 9, 10, 1).unwrap();
        }
    }
    assert_eq!(catalog.reclaim_before(45, 4), 0);
    PageStore::open_chunk(
        ChunkPageStoreOptions {
            open_generation: 1,
            ..options
        },
        Arc::clone(&catalog),
        None,
    )
    .unwrap();

    store.unpin_chunk_generation(45, 9, 10).unwrap();
    assert!(catalog.reclaim_before(45, 4) > 0);
    assert_eq!(
        PageStore::open_chunk(
            ChunkPageStoreOptions {
                open_generation: 1,
                ..options
            },
            catalog,
            None,
        )
        .unwrap_err(),
        CtError::NotFound
    );
}

#[test]
fn callback_root_catalog_opens_exact_manifest_without_latest_fallback() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-callback-exact-root");
    let backend = Arc::new(FileRootCatalogStore {
        dir: dir.path().to_path_buf(),
        generation: AtomicU64::new(0),
        next_reference: AtomicU64::new(1),
        hide_current: AtomicBool::new(false),
    });
    let catalog_backend: Arc<dyn RootCatalogStore> = backend.clone();
    let catalog = Arc::new(ChunkRootCatalog::open_callback(catalog_backend).unwrap());
    let latest_options = ChunkPageStoreOptions {
        tree_id: 44,
        owner_epoch: 11,
        open_generation: 0,
        pack_bytes: 4096,
        iu_size: 1,
        max_concurrent_packs: 2,
        materialization_bytes_per_pass: 4096,
    };
    let latest_store = Arc::new(PageStore::open_chunk(latest_options, Arc::clone(&catalog), None).unwrap());
    let latest = Crowdbtree::open(&Config {
        page_store: Some(Arc::clone(&latest_store)),
        ..Config::default()
    })
    .unwrap();
    latest.apply_put(1, b"key", b"generation-one").unwrap();
    latest.flush().unwrap();
    assert_eq!(latest.snapshot_info().unwrap(), (1, 1));
    latest.apply_put(2, b"key", b"generation-two").unwrap();
    latest.flush().unwrap();
    assert_eq!(latest.snapshot_info().unwrap(), (2, 2));
    assert_eq!(latest_store.chunk_manifest_generation().unwrap(), 2);
    drop(latest);
    drop(latest_store);

    let exact_options = ChunkPageStoreOptions {
        open_generation: 1,
        ..latest_options
    };
    let exact_store = Arc::new(PageStore::open_chunk(exact_options, Arc::clone(&catalog), None).unwrap());
    let exact = Crowdbtree::open(&Config {
        page_store: Some(Arc::clone(&exact_store)),
        ..Config::default()
    })
    .unwrap();
    assert_eq!(exact_store.chunk_manifest_generation().unwrap(), 1);
    assert_eq!(exact.snapshot_state().unwrap(), (1, 1));
    assert_eq!(exact.get(b"key").unwrap(), Some((1, b"generation-one".to_vec())));
    exact.apply_put(2, b"key", b"stale-branch").unwrap();
    exact.flush().unwrap();
    assert_eq!(exact.snapshot_info(), Err(CtError::Unavailable));

    let missing_options = ChunkPageStoreOptions {
        open_generation: 3,
        ..latest_options
    };
    assert_eq!(
        PageStore::open_chunk(missing_options, catalog, None).unwrap_err(),
        CtError::NotFound
    );
}

#[test]
fn published_manifest_is_visible_through_the_same_page_store() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-callback-read-own-write");
    let backend = Arc::new(FileRootCatalogStore {
        dir: dir.path().to_path_buf(),
        generation: AtomicU64::new(0),
        next_reference: AtomicU64::new(1),
        hide_current: AtomicBool::new(false),
    });
    let catalog_backend: Arc<dyn RootCatalogStore> = backend.clone();
    let catalog = Arc::new(ChunkRootCatalog::open_callback(catalog_backend).unwrap());
    let store = Arc::new(
        PageStore::open_chunk(
            ChunkPageStoreOptions {
                tree_id: 43,
                owner_epoch: 10,
                open_generation: 0,
                pack_bytes: 4096,
                iu_size: 65_536,
                max_concurrent_packs: 2,
                materialization_bytes_per_pass: 4096,
            },
            catalog,
            None,
        )
        .unwrap(),
    );
    let config = Config {
        page_store: Some(Arc::clone(&store)),
        ..Config::default()
    };
    let tree = Crowdbtree::open(&config).unwrap();
    tree.apply_put(1, b"durable-root", b"chunk-value").unwrap();
    tree.flush().unwrap();
    let published = tree.snapshot_info().unwrap();
    backend.hide_current.store(true, Ordering::Release);

    let reopened = Crowdbtree::open(&config).unwrap();
    assert_eq!(reopened.snapshot_state().unwrap(), published);
    assert_eq!(
        reopened.get(b"durable-root").unwrap(),
        Some((1, b"chunk-value".to_vec()))
    );
}

#[test]
fn range_rebuild_returns_independent_bounded_tree() {
    let source = Crowdbtree::open(&Config::default()).unwrap();
    for (slot, key) in [b"a", b"b", b"m", b"z"].into_iter().enumerate() {
        source.apply_put(slot as u64 + 1, key, b"v").unwrap();
    }
    source.flush().unwrap();

    let store = Arc::new(PageStore::open_mem(1).unwrap());
    let (rebuilt, stats) = source
        .rebuild_range(&Config {
            page_store: Some(store),
            key_range: KeyRange::Bounded {
                start: Some(b"b".to_vec()),
                end: Some(b"m".to_vec()),
            },
            ..Default::default()
        })
        .unwrap();
    assert_eq!(stats.entries_examined, 4);
    assert_eq!(stats.entries_emitted, 1);
    assert_eq!(stats.entries_filtered, 3);
    assert_eq!(rebuilt.get(b"b").unwrap(), Some((2, b"v".to_vec())));
    assert_eq!(rebuilt.get(b"a"), Err(CtError::InvalidArgument));
}

#[test]
fn injected_mem_store_survives_caller_handle_drop() {
    let store = std::sync::Arc::new(PageStore::open_mem(1).unwrap());
    let t = Crowdbtree::open(&Config {
        page_store: Some(store.clone()),
        frame_bytes: 4096,
        ..Default::default()
    })
    .unwrap();
    drop(store);

    t.apply_put(1, b"key", b"value").unwrap();
    t.flush().unwrap();
    assert_eq!(t.get(b"key").unwrap(), Some((1, b"value".to_vec())));
}

#[test]
fn bounded_tree_rejects_foreign_keys_and_filters_scans() {
    let t = Crowdbtree::open(&Config {
        key_range: KeyRange::Bounded {
            start: Some(b"b".to_vec()),
            end: Some(b"d".to_vec()),
        },
        ..Default::default()
    })
    .unwrap();
    assert_eq!(t.apply_put(1, b"a", b"outside"), Err(CtError::InvalidArgument));
    t.apply_put(1, b"b", b"left").unwrap();
    t.apply_put(2, b"c", b"right").unwrap();
    assert_eq!(t.get(b"d"), Err(CtError::InvalidArgument));
    let (entries, truncated) = t.scan(b"", b"", b"", 0, 0, false, 0, false).unwrap();
    assert!(!truncated);
    assert_eq!(
        entries.iter().map(|entry| entry.key.as_ref()).collect::<Vec<_>>(),
        vec![b"b".as_slice(), b"c".as_slice()]
    );
}

#[test]
fn mem_gc_watermark_and_snapshot_folding() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    t.apply_put(1, b"a", b"A").unwrap();
    t.apply_delete(2, b"a").unwrap();
    t.flush().unwrap();
    assert_eq!(t.get(b"a").unwrap(), None);

    // Below the (default zero) watermark: a snapshot does not fold the
    // tombstone — it is not yet eligible.
    t.snapshot().unwrap();
    assert_eq!(t.get(b"a").unwrap(), None);

    // gc_slot = min(snapshot_slot, safe_slot): a low snapshot_slot still holds
    // the floor down even though safe_slot alone would allow the drop.
    t.set_gc_watermark(0, 2);
    t.snapshot().unwrap();
    assert_eq!(t.get(b"a").unwrap(), None);

    // With the watermark past the tombstone's slot, the next snapshot folds
    // it into a fresh clean leaf. The logical read path is unaffected.
    t.set_gc_watermark(2, 2);
    t.snapshot().unwrap();
    assert_eq!(t.get(b"a").unwrap(), None);

    // A second snapshot has nothing left to fold (idempotent).
    t.snapshot().unwrap();
    assert_eq!(t.get(b"a").unwrap(), None);
}

#[test]
fn mem_compact_sparse_blocks_noop_on_mem_store() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    t.apply_put(1, b"a", b"A").unwrap();
    t.flush().unwrap();
    t.snapshot().unwrap();

    // A non-block store returns an empty stats result with no snapshot write.
    let stats = t.compact_sparse_blocks().unwrap();
    assert_eq!(stats, crowdb_tree_ffi::MergeGcStats::default());
}

#[test]
fn mem_apply_batch_multi_key_and_dup_last_wins() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    t.apply_batch(
        1,
        &[
            BatchOp::Put {
                key: b"a",
                value: b"va",
            },
            BatchOp::Put {
                key: b"b",
                value: b"vb",
            },
            BatchOp::Delete { key: b"c" },
        ],
    )
    .unwrap();
    t.flush().unwrap();
    assert_eq!(t.get(b"a").unwrap(), Some((1, b"va".to_vec())));
    assert_eq!(t.get(b"b").unwrap(), Some((1, b"vb".to_vec())));
    assert_eq!(t.get(b"c").unwrap(), None);

    // Intra-batch duplicate key: last occurrence wins.
    t.apply_batch(
        2,
        &[
            BatchOp::Put {
                key: b"d",
                value: b"first",
            },
            BatchOp::Put {
                key: b"d",
                value: b"second",
            },
        ],
    )
    .unwrap();
    t.flush().unwrap();
    assert_eq!(t.get(b"d").unwrap(), Some((2, b"second".to_vec())));

    // Empty batch is a no-op (mirrors a NoOp repair-fill payload).
    t.apply_batch(3, &[]).unwrap();
}

#[test]
fn file_snapshot_reopen_smoke() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };

    {
        let t = Crowdbtree::open(&opt).unwrap();
        for i in 0..50usize {
            let v = format!("value{i}").into_bytes();
            t.apply_put((i + 1) as u64, &key(i), &v).unwrap();
            t.flush().unwrap();
        }
        let durable = t.snapshot().unwrap();
        assert_eq!(durable, 50);
    }
    // Reopen the same file and verify recovery.
    let t = Crowdbtree::open(&opt).unwrap();
    assert_eq!(t.last_applied_slot(), 50);
    for i in 0..50usize {
        assert_eq!(
            t.get(&key(i)).unwrap(),
            Some(((i + 1) as u64, format!("value{i}").into_bytes()))
        );
    }
}

// : Config::backend = PageStoreBackend::Block selects
// BlockPageStore instead of the default file-based page store -- same
// round-trip as file_snapshot_reopen_smoke above, just through the
// block-device backend.
#[test]
fn block_device_snapshot_reopen_smoke() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 4096,
        frame_bytes: 4096,
        backend: PageStoreBackend::Block,
        ..Default::default()
    };

    {
        let t = Crowdbtree::open(&opt).unwrap();
        for i in 0..50usize {
            let v = format!("value{i}").into_bytes();
            t.apply_put((i + 1) as u64, &key(i), &v).unwrap();
            t.flush().unwrap();
        }
        let durable = t.snapshot().unwrap();
        assert_eq!(durable, 50);
    }
    // Reopen the same file and verify recovery.
    let t = Crowdbtree::open(&opt).unwrap();
    assert_eq!(t.last_applied_slot(), 50);
    for i in 0..50usize {
        assert_eq!(
            t.get(&key(i)).unwrap(),
            Some(((i + 1) as u64, format!("value{i}").into_bytes()))
        );
    }
}

#[test]
fn snapshot_sessions_stream_with_metadata_and_abort_without_installing() {
    let source = Arc::new(Crowdbtree::open(&Config::default()).unwrap());
    for i in 0..30usize {
        source
            .apply_put((i + 1) as u64, &key(i), format!("v{i}").as_bytes())
            .unwrap();
        source.flush().unwrap();
    }

    let mut export = source.snapshot_export_begin(19).unwrap();
    let metadata = export.metadata();
    assert_eq!(metadata.at_slot, 30);
    assert_eq!(metadata.chunk_bytes, 19);
    assert!(metadata.total_bytes > 19);
    assert_ne!(metadata.final_crc32c, 0);
    assert_eq!(export.read(1), Err(CtError::InvalidArgument));

    let target = Arc::new(Crowdbtree::open(&Config::default()).unwrap());
    let mut import = target.snapshot_import_begin().unwrap();
    let mut offset = 0;
    loop {
        let chunk = export.read(offset).unwrap();
        assert_eq!(chunk.offset, offset);
        assert!(chunk.bytes.len() <= metadata.chunk_bytes);
        import.feed(&chunk.bytes).unwrap();
        offset += chunk.bytes.len() as u64;
        if chunk.done {
            break;
        }
    }
    assert_eq!(offset, metadata.total_bytes);
    assert_eq!(import.finish().unwrap(), metadata.at_slot);
    assert_eq!(target.snapshot_view().unwrap(), source.snapshot_view().unwrap());

    let empty_target = Arc::new(Crowdbtree::open(&Config::default()).unwrap());
    let mut abandoned = empty_target.snapshot_import_begin().unwrap();
    let mut second_export = source.snapshot_export_begin(19).unwrap();
    abandoned.feed(&second_export.read(0).unwrap().bytes).unwrap();
    abandoned.abort();
    assert_eq!(empty_target.get(&key(0)).unwrap(), None);
}

#[test]
fn io_failed_clean_on_healthy_engine() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    for i in 0..10usize {
        t.apply_put((i + 1) as u64, &key(i), b"v").unwrap();
        t.flush().unwrap();
    }
    for i in 0..10usize {
        let _ = t.get(&key(i)).unwrap();
    }
    assert!(!t.io_failed());
    t.clear_io_error();
    assert!(!t.io_failed());
}

#[test]
fn open_rejects_path_with_nul() {
    let opt = Config {
        path: Some("bad\0path".to_string()),
        ..Default::default()
    };
    assert_eq!(Crowdbtree::open(&opt).unwrap_err(), CtError::InvalidArgument);
}

#[tokio::test]
async fn async_bridge_apply_get_snapshot() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowdbtree::open(&opt).unwrap();
    for i in 0..20usize {
        t.handle()
            .apply_put((i + 1) as u64, &key(i), format!("a{i}").as_bytes())
            .unwrap();
        t.flush().await.unwrap();
    }
    let durable = t.snapshot().await.unwrap();
    assert_eq!(durable, 20);
    assert_eq!(t.get(key(3)).await.unwrap(), Some((4u64, b"a3".to_vec())));
    assert_eq!(t.get(key(999)).await.unwrap(), None);
}

// Phase 3 : AsyncCrowdbtree::get/flush/snapshot now drive the
// engine's io_uring reactor directly (CtGetFuture/CtFlushFuture/
// CtSnapshotFuture) -- no spawn_blocking. Regression guard for the whole
// point of this phase: manually poll the returned future exactly once with a
// no-op waker and assert it is already Ready -- a resident hit must resolve
// without ever registering on the reactor's eventfd (unlike a timing-based
// assertion, this is deterministic).
#[tokio::test]
async fn async_get_fast_path_completes_on_first_poll() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowdbtree::open(&opt).unwrap();
    t.handle().apply_put(1, &key(0), b"v0").unwrap();
    t.flush().await.unwrap();

    // Resident (never evicted): get_async's fast path (try_get_view_no_load,
    // design #5 B3) must resolve this on the very first poll.
    let mut fut = Box::pin(t.get(key(0)));
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert_eq!(
        fut.as_mut().poll(&mut cx),
        Poll::Ready(Ok(Some((1u64, b"v0".to_vec()))))
    );
}

#[tokio::test]
async fn async_get_slow_path_completes_after_eviction() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowdbtree::open(&opt).unwrap();
    t.handle().apply_put(1, &key(0), b"v0").unwrap();
    t.flush().await.unwrap();
    t.snapshot().await.unwrap();
    // Force the leaf unloaded so the next get takes the demand-load miss
    // path -- on this (liburing) build, that means a genuine reactor/eventfd
    // round trip, not spawn_blocking.
    t.handle().evict_clean_leaves(0);

    assert_eq!(t.get(key(0)).await.unwrap(), Some((1u64, b"v0".to_vec())));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_async_gets_all_resolve_correctly() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowdbtree::open(&opt).unwrap();
    const N: usize = 16;
    for i in 0..N {
        t.handle()
            .apply_put((i + 1) as u64, &key(i), &format!("v{i}").into_bytes())
            .unwrap();
    }
    t.flush().await.unwrap();
    t.snapshot().await.unwrap();
    // Force every leaf unloaded so all N concurrent gets below take the
    // reactor round trip -- proves the eventfd wakeup fans out to every
    // pending future, not just one.
    t.handle().evict_clean_leaves(0);

    let mut tasks = Vec::with_capacity(N);
    for i in 0..N {
        let t = t.clone();
        tasks.push(tokio::spawn(async move { t.get(key(i)).await }));
    }
    for (i, task) in tasks.into_iter().enumerate() {
        let got = task.await.unwrap().unwrap();
        assert_eq!(got, Some(((i + 1) as u64, format!("v{i}").into_bytes())));
    }
}

// follow-up: AsyncCrowdbtree::scan mirrors async_get_fast_
// path_completes_on_first_poll's regression shape for scan, which also
// has a resident-hit fast path (try_scan_no_load).
#[tokio::test]
async fn async_scan_fast_path_completes_on_first_poll() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowdbtree::open(&opt).unwrap();
    for i in 0..10usize {
        t.handle()
            .apply_put((i + 1) as u64, &key(i), &format!("v{i}").into_bytes())
            .unwrap();
    }
    t.flush().await.unwrap();

    let mut fut = Box::pin(t.scan(Vec::new(), Vec::new(), Vec::new(), 0, 0, false, 0));
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(Ok((entries, truncated))) => {
            assert_eq!(entries.len(), 10);
            assert!(!truncated);
        }
        other => panic!("expected an immediately-ready resolved scan, got {other:?}"),
    }
}

// Mirrors async_get_slow_path_completes_after_eviction for scan: forcing
// every leaf unloaded makes scan_async take the demand-load-miss retry loop
// (scan_async_attempt) instead of resolving on the first poll, and it still
// produces the full, correct result.
#[tokio::test]
async fn async_scan_slow_path_completes_after_eviction() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowdbtree::open(&opt).unwrap();
    for i in 0..20usize {
        t.handle()
            .apply_put((i + 1) as u64, &key(i), &format!("v{i}").into_bytes())
            .unwrap();
    }
    t.flush().await.unwrap();
    t.snapshot().await.unwrap();
    t.handle().evict_clean_leaves(0);

    let (entries, truncated) = t
        .scan(Vec::new(), Vec::new(), Vec::new(), 0, 0, false, 0)
        .await
        .unwrap();
    assert!(!truncated);
    assert_eq!(entries.len(), 20);
    let mut got: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = entries
        .into_iter()
        .map(|e| (e.key.to_vec(), e.value.to_vec()))
        .collect();
    for i in 0..20usize {
        assert_eq!(got.remove(&key(i)), Some(format!("v{i}").into_bytes()));
    }
    assert!(got.is_empty());
}

#[tokio::test]
async fn async_reverse_scan_crosses_evicted_leaves_and_respects_cursor() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowdbtree::open(&opt).unwrap();
    for i in 0..80usize {
        t.handle()
            .apply_put((i + 1) as u64, &key(i), &[b'v'; 128])
            .unwrap();
    }
    t.flush().await.unwrap();
    t.snapshot().await.unwrap();
    t.handle().evict_clean_leaves(0);

    let (entries, truncated) = t
        .scan_directional(
            b"key0".to_vec(),
            b"key00070".to_vec(),
            b"key00075".to_vec(),
            5,
            0,
            true,
            0,
            ScanDirection::Reverse,
        )
        .await
        .unwrap();
    assert!(truncated);
    let keys: Vec<_> = entries.iter().map(|entry| entry.key.to_vec()).collect();
    assert_eq!(keys, (65..70).rev().map(key).collect::<Vec<_>>(),);
    assert!(entries.iter().all(|entry| entry.value.is_empty()));
}

// A limit smaller than the matching key count truncates, matching scan's
// own synchronous semantics.
#[tokio::test]
async fn async_scan_respects_limit_and_truncated_flag() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowdbtree::open(&opt).unwrap();
    for i in 0..15usize {
        t.handle()
            .apply_put((i + 1) as u64, &key(i), &format!("v{i}").into_bytes())
            .unwrap();
    }
    t.flush().await.unwrap();

    let (entries, truncated) = t
        .scan(Vec::new(), Vec::new(), Vec::new(), 5, 0, false, 0)
        .await
        .unwrap();
    assert_eq!(entries.len(), 5);
    assert!(truncated);
}

// keys_only projection: the engine skips value materialization (no
// overflow-chain assembly) and stages empty values; the packed wire format
// is unchanged (vlen=0). Keys match a full scan; values are all empty.
#[tokio::test]
async fn async_scan_keys_only_skips_values() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowdbtree::open(&opt).unwrap();
    for i in 0..10usize {
        t.handle()
            .apply_put((i + 1) as u64, &key(i), &format!("v{i}").into_bytes())
            .unwrap();
    }
    t.flush().await.unwrap();

    let (entries, truncated) = t
        .scan(Vec::new(), Vec::new(), Vec::new(), 0, 0, true, 0)
        .await
        .unwrap();
    assert!(!truncated);
    assert_eq!(entries.len(), 10);
    assert!(
        entries.iter().all(|e| e.value.is_empty()),
        "keys_only values are empty"
    );
    // Keys match the full scan's keys, in order.
    let (full, _) = t
        .scan(Vec::new(), Vec::new(), Vec::new(), 0, 0, false, 0)
        .await
        .unwrap();
    let keys: Vec<Vec<u8>> = entries.iter().map(|e| e.key.to_vec()).collect();
    let full_keys: Vec<Vec<u8>> = full.iter().map(|e| e.key.to_vec()).collect();
    assert_eq!(keys, full_keys);
    assert!(
        full.iter().all(|e| !e.value.is_empty()),
        "full scan values are non-empty"
    );
}

#[tokio::test]
async fn try_get_pinned_fast_path_returns_borrowed_value() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowdbtree::open(&opt).unwrap();
    t.handle().apply_put(1, &key(0), b"v0").unwrap();
    t.flush().await.unwrap();

    match t.try_get_pinned(&key(0)) {
        PinnedGetOutcome::Ready(Ok(Some((slot, pinned)))) => {
            assert_eq!(slot, 1);
            assert_eq!(pinned.as_bytes(), b"v0");
        }
        _ => panic!("expected Ready(Ok(Some)) on resident hit"),
    }

    // Not-found case: should return Ready(Ok(None)).
    match t.try_get_pinned(&key(99)) {
        PinnedGetOutcome::Ready(Ok(None)) => {}
        _ => panic!("expected Ready(Ok(None)) for missing key"),
    }
}

#[tokio::test]
async fn try_get_pinned_slow_path_resolves_after_eviction() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowdbtree::open(&opt).unwrap();
    t.handle().apply_put(1, &key(0), b"v0").unwrap();
    t.flush().await.unwrap();
    t.snapshot().await.unwrap();
    t.handle().evict_clean_leaves(0);

    // On builds without io_uring (e.g. macOS), ct_get_async completes
    // synchronously even after eviction (sync fallback), so both Ready
    // and Pending are valid outcomes — either way the value must be correct.
    match t.try_get_pinned(&key(0)) {
        PinnedGetOutcome::Ready(Ok(Some((slot, pinned)))) => {
            assert_eq!(slot, 1);
            assert_eq!(pinned.as_bytes(), b"v0");
        }
        PinnedGetOutcome::Pending(fut) => {
            let result = fut.await.unwrap();
            assert_eq!(result, Some((1u64, b"v0".to_vec())));
        }
        _ => panic!("expected Ok(Some) or Pending for evicted key"),
    }
}

#[tokio::test]
async fn try_get_pinned_fast_path_with_large_value() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-ffi");
    let opt = Config {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowdbtree::open(&opt).unwrap();
    let large: Vec<u8> = (0..4096u32).map(|i| u8::try_from(i % 256).unwrap()).collect();
    t.handle().apply_put(1, &key(0), &large).unwrap();
    t.flush().await.unwrap();

    match t.try_get_pinned(&key(0)) {
        PinnedGetOutcome::Ready(Ok(Some((slot, pinned)))) => {
            assert_eq!(slot, 1);
            assert_eq!(pinned.as_bytes(), large.as_slice());
        }
        _ => panic!("expected Ready(Ok(Some)) on resident hit"),
    }
}

#[test]
fn zero_copy_alloc_apply_round_trip() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    let mut h = t.alloc_put(3, 5).unwrap();
    h.key_mut().copy_from_slice(b"abc");
    h.value_mut().copy_from_slice(b"hello");
    h.apply(1).unwrap();
    t.flush().unwrap();
    let got = t.get(b"abc").unwrap();
    assert_eq!(got, Some((1u64, b"hello".to_vec())));
}

#[test]
fn zero_copy_large_value_round_trip() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    let big: Vec<u8> = (0..8192u32).map(|i| u8::try_from(i % 256).unwrap()).collect();
    let mut h = t.alloc_put(4, big.len()).unwrap();
    h.key_mut().copy_from_slice(b"bigk");
    h.value_mut().copy_from_slice(&big);
    h.apply(1).unwrap();
    t.flush().unwrap();
    let got = t.get(b"bigk").unwrap();
    assert_eq!(got, Some((1u64, big)));
}

#[test]
fn zero_copy_empty_value_round_trip() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    let mut h = t.alloc_put(2, 0).unwrap();
    h.key_mut().copy_from_slice(b"ev");
    assert!(h.value_mut().is_empty());
    h.apply(1).unwrap();
    t.flush().unwrap();
    let got = t.get(b"ev").unwrap();
    assert_eq!(got, Some((1u64, Vec::new())));
}

#[test]
fn zero_copy_handle_drop_without_apply_no_leak() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    let mut h = t.alloc_put(10, 4096).unwrap();
    h.key_mut().fill(0xAB);
    h.value_mut().fill(0xCD);
    drop(h); // RAII frees the handle
}

#[test]
fn zero_copy_oversized_key_rejected() {
    let opt = Config {
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = Crowdbtree::open(&opt).unwrap();
    let result = t.alloc_put(3000, 4);
    assert_eq!(result.err(), Some(CtError::InvalidArgument));
}

// ── R30: zero-copy apply_batch_external ───────────────────────────

#[test]
fn apply_batch_external_round_trip_before_flush() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    let big = bytes::Bytes::from(vec![0xF0; 4096]);
    let ops = vec![
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"k1"),
            value: big.clone(),
        },
        ExtOp::Delete {
            key: bytes::Bytes::from_static(b"k2"),
        },
    ];
    t.apply_batch_external(1, ops).unwrap();
    // L0 read (before flush): materializes the split cell.
    assert_eq!(t.get(b"k1").unwrap(), Some((1, big.to_vec())));
    assert_eq!(t.get(b"k2").unwrap(), None);
}

#[test]
fn apply_batch_external_round_trip_after_flush() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    let big = bytes::Bytes::from(vec![0xE1; 8192]);
    let ops = vec![ExtOp::Put {
        key: bytes::Bytes::from_static(b"k2"),
        value: big.clone(),
    }];
    t.apply_batch_external(1, ops).unwrap();
    t.flush().unwrap(); // drain -> materialize -> Rust ref released
    assert_eq!(t.get(b"k2").unwrap(), Some((1, big.to_vec())));
}

#[test]
fn apply_batch_external_multi_key_atomicity() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    let ops = vec![
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"a"),
            value: bytes::Bytes::from_static(b"va"),
        },
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"b"),
            value: bytes::Bytes::from_static(b"vb"),
        },
        ExtOp::Delete {
            key: bytes::Bytes::from_static(b"c"),
        },
    ];
    t.apply_batch_external(1, ops).unwrap();
    t.flush().unwrap();
    assert_eq!(t.get(b"a").unwrap(), Some((1, b"va".to_vec())));
    assert_eq!(t.get(b"b").unwrap(), Some((1, b"vb".to_vec())));
    assert_eq!(t.get(b"c").unwrap(), None);
}

#[test]
fn apply_batch_external_intra_batch_last_key_wins() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    let ops = vec![
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"k"),
            value: bytes::Bytes::from_static(b"first"),
        },
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"k"),
            value: bytes::Bytes::from_static(b"second"),
        },
    ];
    t.apply_batch_external(1, ops).unwrap();
    assert_eq!(t.get(b"k").unwrap(), Some((1, b"second".to_vec())));
}

#[test]
fn apply_batch_external_large_value_round_trip() {
    let t = Crowdbtree::open(&Config::default()).unwrap();
    // 64 KiB values — the workload R30 targets (eliminate the apply-path copy).
    let v1 = bytes::Bytes::from(vec![0x11; 65536]);
    let v2 = bytes::Bytes::from(vec![0x22; 65536]);
    let v3 = bytes::Bytes::from(vec![0x33; 65536]);
    let ops = vec![
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"big1"),
            value: v1.clone(),
        },
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"big2"),
            value: v2.clone(),
        },
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"big3"),
            value: v3.clone(),
        },
    ];
    t.apply_batch_external(1, ops).unwrap();
    t.flush().unwrap();
    assert_eq!(t.get(b"big1").unwrap(), Some((1, v1.to_vec())));
    assert_eq!(t.get(b"big2").unwrap(), Some((1, v2.to_vec())));
    assert_eq!(t.get(b"big3").unwrap(), Some((1, v3.to_vec())));
}

#[test]
fn apply_batch_external_bytes_kept_alive_until_drain() {
    // The payload Bytes is kept alive by the external buffers' Arc clones;
    // it must not be freed until the MemTable drains (flush). This test
    // verifies the ref handle lifecycle: the Bytes stays valid through apply
    // and flush, and the drop callback fires at drain (not before).
    let t = Crowdbtree::open(&Config::default()).unwrap();
    let payload = bytes::Bytes::from(vec![0xAB; 1024]);
    let ops = vec![ExtOp::Put {
        key: bytes::Bytes::from_static(b"k"),
        value: payload.clone(),
    }];
    t.apply_batch_external(1, ops).unwrap();
    // payload is still alive here (we hold a clone); the engine's clone is
    // pinned by the memtable entry. After flush, the engine's clone is freed.
    assert_eq!(t.get(b"k").unwrap(), Some((1, payload.to_vec())));
    t.flush().unwrap();
    assert_eq!(t.get(b"k").unwrap(), Some((1, payload.to_vec())));
    // payload still valid in the test (our clone) — verifies no use-after-free.
    assert_eq!(payload.as_ref(), &[0xAB; 1024]);
}
