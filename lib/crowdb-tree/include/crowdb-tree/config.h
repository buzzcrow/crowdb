// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Config: tunables for consolidation, flush triggers, and page split/merge.
// Defaults follow the core engine design.
#pragma once

#include "crowdb-tree/btree/key_range.h"
#include "crowdb-tree/maptable/compressor.h"
#include "crowdb-tree/tree_execution_options.h"
#include "crowdb-tree/tree_runtime.h"

#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"
#endif

#include <cstddef>
#include <cstdint>
#include <string>

namespace crowdb::tree
{

class PageStore;
class AsyncPageStore;

struct Config
{
    // The host owns one shared runtime; new Tree core operations retain their
    // binding through all CPU and backend callbacks. Null selects the shared
    // process standalone runtime; it does not create a pool per Tree.
    std::shared_ptr<TreeRuntime> runtime;
    TreeBindingOptions           runtime_binding{};
    TreeExecutionOptions         execution{};

    // Deprecated compatibility fields. Immutable publication uses bounded
    // overlay runs and maintenance; it does not create pointer delta chains.
    uint32_t max_delta_len   = 64;
    size_t   max_delta_bytes = 512ULL * 1024; // 512 KiB

    // ── Leaf split / merge (core doc §8) ──
    // Leaf thresholds count the record region, excluding frame headers/trailer.
    // Runtime planning adds that overhead and caps the split target to the
    // configured frame and current public frame payload capacity. A default
    // split target derives the usable record capacity for smaller frames.
    // Merge triggers below leaf_merge_bytes; the merged payload must also fit
    // within three quarters of the split target to preserve hysteresis. Zero
    // disables merging. Values that overflow frame arithmetic are rejected.
    size_t leaf_split_bytes = 65'502;
    size_t leaf_merge_bytes = 16ULL * 1024;

    // Inner-page fanout bound (separator count) before an inner split.
    uint32_t inner_max_keys = 256;
    // Inner-page underflow threshold: when a non-root inner page drops below this
    // many separators (after a child merge), it is merged with its left sibling
    // so delete-heavy workloads don't leave a sparse upper tree. 0 = derive
    // max(1, inner_max_keys / 4) (well below the post-split size for hysteresis).
    uint32_t inner_merge_keys = 0;

    // ── Overflow pages (PT11) ──
    // A value larger than this spills out of its leaf into a chain of fixed
    // overflow frames; the leaf keeps a small fixed pointer cell, so every base
    // page stays <= frame_bytes (pool-resident + evictable). 0 = derive a default
    // of frame_bytes / 4 at engine construction.
    size_t max_inline_value = 0;

    // ── Key size limit (plan-tree #15) ──
    // A key larger than this is rejected at apply() with kInvalidArgument (an
    // oversized key is assumed to be a caller bug: keys are copied into every
    // delta and inner separator, so a huge key would blow up the tree). 0 =
    // derive a default of frame_bytes / 2 at engine construction, which keeps a
    // key comfortably within a single leaf/inner frame alongside its cell.
    size_t max_key_size = 0;

    // Deprecated compatibility fields. New publication does not append an
    // in-frame delta region; historical delta frames remain readable.
    bool     inframe_delta     = false;
    uint32_t max_inframe_delta = 8;

    // Soft per-source thresholds. A successful write crossing either bound
    // publishes a fresh writable source while the old source remains readable.
    // Values of zero request rotation after each nonempty write. Independent
    // hard memory/version limits still apply to admission.
    size_t   memtable_flush_bytes   = 16ULL * 1024 * 1024;
    uint32_t memtable_flush_entries = 100000;

    // Total soft source count, including one writable source (minimum two).
    // Runtime maintenance can trigger earlier to keep inherited tables, a
    // capture successor and a child writer within fixed read/flush windows.
    // At this limit another threshold crossing requests the existing finite
    // handoff, so sustained writes make bounded progress without growing the
    // source queue. A gap or unavailable whole-batch credit leaves accepted
    // data readable; explicit maintenance and the accepted/gap apply channels
    // retain their own progress admission. A failed soft maintenance attempt
    // never changes the already committed write result.
    uint32_t max_memtable_count = 10;

    // Deprecated compatibility field. Immutable MappingView radix geometry
    // and checkpoint metadata layout are fixed by their format.
    uint32_t mapping_segment_slots = 1024;

    // ── Persistence ──
    // Durable backend. Non-owning; nullptr = pure in-memory engine (no
    // snapshot/recovery). When set, snapshot() writes the materialized L1
    // state and open() recovers it.
    PageStore *page_store = nullptr;

    // ── Async I/O ──
    // Non-owning; the caller owns and outlives the Crowdbtree. When null,
    // storage work runs on the host runtime's bounded backend executor.
    // The store must address the same durable image as
    // `page_store`. This contract is independent of io_uring so RPC-backed
    // stores work on builds without liburing.
    AsyncPageStore *async_page_store = nullptr;
    // Optional shared owner of both raw backend fields and their reactor.
    // Detached runtime snapshots keep this owner after the Tree closes.
    std::shared_ptr<const void> backend_owner;
#ifdef CROWDB_HAVE_LIBURING
    // Optional local reactor used only by BlockAsyncPageStore.
    ::crowdb::common::DiskIOUring *async_uring = nullptr;
#endif

    // Fixed output frame geometry and the independent decoded-page cache
    // capacity. Overflow values use bounded chains of fixed-size frames.
    uint32_t frame_bytes       = 64ULL * 1024;        // fixed arena frame size
    size_t   buffer_pool_bytes = 64ULL * 1024 * 1024; // arena capacity (frames * frame_bytes)

    // ── Page compression (PT10) ──
    // On-disk only: each durable base page is wrapped in a self-describing blob
    // ([algo][raw_len][stored_len][crc][stored]); the buffer pool always holds
    // uncompressed frames. The algo is recorded per page, so a page is decoded
    // correctly regardless of the option in force when it is read back (mixed
    // pages across incremental snapshots decode fine). default_env kNone keeps
    // the stored bytes equal to the raw frame; kLz4 compresses when it shrinks
    // the page (falling back to stored-raw when LZ4 is unavailable or unhelpful).
    compress_algo compression = compress_algo::kNone;

    // ── Engine identity (logging context) ──
    // store_id / group_id from the caller (CrowdbKV passes its Paxos store/group
    // ids). Used solely to build `name` for CT_LOG context — not for file naming
    // (the PageStore handles that) or any logic.
    uint32_t store_id = 0;
    uint32_t group_id = 0;
    // Human-readable label prepended to every CT_LOG line, e.g. "s1.g1".
    // If empty, logs show "[unnamed]".
    std::string name;

    // Short backend label for metric names (e.g. "file", "block", "mem").
    // Set by the caller before open(); included in the init_metrics prefix
    // so C++ metrics are tagged with their storage backend.
    std::string backend_label;

    // Immutable public key policy. Chunk-KV partitions use a bounded range;
    // ordinary KV trees retain the unbounded default.
    KeyRange key_range = KeyRange::unbounded();

    // ── Block compaction (R129) ──
    // Cadence-driven merge GC reclaims sparse array-of-blocks source blocks
    // by relocating their live extents during a snapshot. Defaults: disabled
    // cadence (interval 0), 70% free-ratio threshold, one block of relocation
    // per pass. See design-crowdb-tree-storage.md §2.5.
    // merge_gc_interval_ms: wall-clock cadence for compact_sparse_blocks; 0
    // disables the cadence-driven pass (snapshot folding still runs).
    uint64_t merge_gc_interval_ms = 0;
    // merge_gc_block_free_threshold: shared by allocation gap filtering and
    // source-block selection. A block whose free ratio is strictly greater
    // than this value is a compaction candidate. (0.0, 1.0).
    double merge_gc_block_free_threshold = 0.70;
    // merge_gc_max_relocation_bytes: per-pass byte budget; 0 disables
    // relocation (selection returns empty). The first eligible block is
    // always allowed even when it exceeds the budget so progress is possible.
    uint64_t merge_gc_max_relocation_bytes = 0;

    // ── Logging ──
    // These fields are no longer used by Crowdbtree::open() — logging is now
    // process-global and must be initialized by the application via
    // init_logging() (or ct_init_logging in the C API) before any open().
    // They are retained for API compatibility.
    std::string log_dir;                         // unused (see init_logging)
    std::string log_level       = "info";        // unused (see init_logging)
    std::string log_file_prefix = "crowdb-tree"; // unused (see init_logging)
    size_t      log_max_file_mb = 30;            // unused (see init_logging)
    size_t      log_max_files   = 5;             // unused (see init_logging)
};

} // namespace crowdb::tree
