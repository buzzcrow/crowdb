// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Crowdbtree: one ordered, single-version-per-key store per consensus group.
// Two-level write path: apply() lands in the MemTable
// (L0); flush() merges the contiguous-applied prefix into the COW B+tree (L1).
#pragma once

#include "crowdb-common/metrics/metrics.h"
#include "crowdb-tree/btree/cell.h"
#include "crowdb-tree/btree/diagnostics.h"
#include "crowdb-tree/btree/read_result.h"
#include "crowdb-tree/btree/scan_packed.h"
#include "crowdb-tree/config.h"
#include "crowdb-tree/epoch.h"
#include "crowdb-tree/maptable/mapping_table.h"
#include "crowdb-tree/maptable/page.h"
#include "crowdb-tree/memtable/memtable.h"
#include "crowdb-tree/memtable/source.h"
#include "crowdb-tree/snapshot/native_frame.h"
#include "crowdb-tree/snapshot/prepared.h"
#include "crowdb-tree/snapshot/snapshot.h"
#include "crowdb-tree/status.h"

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <vector>

namespace crowdb::tree
{

struct RangeRebuildStats;

// The metrics core moved to crowdb-common::metrics (R12); bridge the moved types
// into `crowdb-tree` with per-type using-declarations so existing
// `Counter*`/`Gauge*`/`LatencySummary*`/`MetricsRegistry`/`Bandwidth`
// references compile unchanged. (Not a `namespace crowdb::tree =
// crowdb::common::metrics;` alias — only the specific types are bridged.)
using crowdb::common::metrics::Bandwidth;
using crowdb::common::metrics::CallbackGauge;
using crowdb::common::metrics::Counter;
using crowdb::common::metrics::Gauge;
using crowdb::common::metrics::LatencySummary;
using crowdb::common::metrics::MetricsRegistry;

class AsyncPageStore;

// One mutation in a batch. All ops in a batch share the batch's slot.
struct batch_op
{
    std::string key;
    OpKind      kind;
    std::string value; // empty for Delete
};

struct Batch
{
    std::vector<batch_op> ops;
};

class Crowdbtree
{
  public:
    explicit Crowdbtree(Config opt = Config());
    ~Crowdbtree();

    Crowdbtree(const Crowdbtree &)            = delete;
    Crowdbtree &operator=(const Crowdbtree &) = delete;

    // open a tree, recovering durable state from opt.page_store if a valid
    // snapshot exists; otherwise start empty. Requires opt.page_store != null.
    static Status open(const Config &opt, std::unique_ptr<Crowdbtree> *out);

    // Persist the materialized L1 state durably. Folds delta chains, writes
    // dirty base pages plus a fresh image for each dirty mapping-table
    // segment and the segment directory, then commits the inactive A/B
    // anchor slot. Returns the durable last_applied_slot via out (if
    // non-null). Requires opt.page_store != null.
    Status snapshot(uint64_t *out_last_applied = nullptr, uint64_t *out_snapshot_seq = nullptr);

    // Run one bounded immutable-backend ownership cleanup pass under the same
    // generation gate as snapshot publication. Local stores complete as a no-op.
    Status materialize_ownership(uint64_t *bytes_written, bool *complete);

    // Async twin of snapshot(). Always genuinely
    // async from *this* caller's perspective when Config::async_page_store is
    // wired (flush/snapshot are
    // *always* slow-path, unlike get/scan): snapshot_async() returns
    // immediately after kicking off the walk + first I/O submission, and
    // on_done fires later from the Reactor thread with the same result
    // snapshot() would have returned.
    //
    // Lock discipline (this is the one place in the engine where a
    // completion legitimately runs on a *different* thread than the one
    // that started the operation, so it gets its own note): write_mutex_
    // itself is only ever locked and unlocked on the *same* thread, for the
    // brief synchronous prepare_snapshot_locked() walk, exactly like every
    // other writer entry point -- std::mutex has no defined behavior for a
    // cross-thread unlock, so it is never held across the async write
    // phase. What *does* span the whole prepare-through-commit sequence is
    // snapshot_inflight_, a plain std::atomic<bool> spin-gate (a mutex's
    // "same thread unlocks it" restriction is a pthread_mutex_t property,
    // not a general lock property -- an atomic has no such restriction):
    // it serializes this generation's SpaceAllocator against a *second*
    // overlapping snapshot(_async) call (which would otherwise rebuild an
    // allocator from the same last-*committed* anchor and could hand
    // out the same "free" byte range to two different pages -- silent
    // corruption), without blocking apply()/flush()/evict_clean_leaves(),
    // which remain free to run concurrently against write_mutex_ as usual.
    // That safety hinges on prepare_snapshot_locked() never eagerly setting
    // a dirty page's PageBase::durable_addr -- see its doc comment --
    // because evict_clean_leaves_locked() treats durable_addr != kNoAddr as
    // "safe to evict, a durable copy already exists"; commit_prepared_snapshot()
    // is what actually sets it, one write_mutex_ critical section per page,
    // only once that page's specific byte write has landed.
    //
    // Falls back to running the existing synchronous snapshot() in the
    // caller's stack frame (still correct, just not async) when no async
    // backend is wired -- e.g. a MemPageStore-backed tree.
    void snapshot_async(std::function<void(Status, uint64_t last_applied)> on_done);

    // Ingest a batch at `slot`. The tree internally tracks received slots and
    // computes the contiguous prefix (how far the flusher may flush) itself, so
    // callers no longer pass Paxos/learner state. Lands in L0; may trigger a
    // size-based flush. For a slot with no data (a NoOp), call force_advance_slot.
    Status apply(uint64_t slot, const Batch &batch);

    // One already-encoded op for apply_encoded (plan-tree #5 B2d): `cell` is
    // a slot+kind+value cell already packed via encode_cell_buf. Lets a
    // caller that owns the raw bytes up front (the C API boundary) allocate
    // the key and cell buffers exactly once and move them straight down to
    // MemTable::upsert, instead of building a Batch (plain key/kind/value
    // strings) that apply_batch would otherwise re-encode into a cell here.
    struct encoded_op
    {
        std::string key;
        buffer      cell;
    };

    // Same semantics as apply() (oversized-key rejection, slot bookkeeping,
    // maybe_swap_active), but for pre-encoded ops -- no encode_cell_buf call
    // in here, no intermediate Batch/batch_op. Intra-batch: last occurrence
    // (by vector order) wins, same as apply_batch.
    Status apply_encoded(uint64_t slot, std::vector<encoded_op> ops);

    // One zero-copy op for apply_external (R30): `value` is a kExternal buffer
    // borrowing bytes from a Rust `bytes::Bytes` (Put) or an empty buffer
    // (Delete, `flags = kFlagTombstone`). The 9-byte cell header is NOT in the
    // buffer — it is stored as the `flags` field here plus the `slot` argument,
    // and materialized into a contiguous cell at MemTable drain/get. Lets the
    // consensus apply path skip the value memcpy that encode_cell_buf performs.
    struct external_op
    {
        std::string key;
        uint8_t     flags = 0; // kPut (0) or kFlagTombstone
        buffer      value;     // kExternal (borrowed) for Put; empty for Delete
    };

    // Same semantics as apply_encoded (oversized-key rejection, slot
    // bookkeeping, maybe_swap_active, intra-batch last-key-wins) but stores
    // split cells via MemTable::upsert_external — no encode_cell_buf, no value
    // memcpy on the apply critical path. The value copy is deferred to flush
    // (off the critical path).
    Status apply_external(uint64_t slot, std::vector<external_op> ops);

    // Advance the contiguous frontier up to `slot`, filling any intervening slots
    // as NoOps (e.g. after learner NoOp slots or during restore). Explicit and
    // free of learner jargon.
    void force_advance_slot(uint64_t slot);

    // Convenience methods: auto-assign the next slot (max_seen + 1) and apply.
    // Intended for single-writer use; do not mix with explicit-slot apply calls.
    Status put(Slice key, Slice value);
    Status del(Slice key);
    Status batch_put(const Batch &batch);

    [[nodiscard]] Status validate_key(Slice key) const;
    // Advisory separator from bounded resident index pages, without I/O or flush.
    // Callers validate live keys on both sides before publishing a split.
    [[nodiscard]] std::optional<std::string> approximate_split_key() const;

    // Logical retention GC watermark:
    // stores both slots and computes gc_floor_ = min(snapshot_slot, safe_slot).
    // Tombstones with slot <= gc_floor_ may be dropped during snapshot
    // preparation (folding) and block compaction. Using the min of the two
    // (rather than safe_slot alone) is what makes it safe to call this before
    // #20's learner wiring: a tombstone whose deletion isn't yet durable on a
    // quorum (snapshot_slot) is never dropped early just because every member
    // has locally applied it (safe_slot). Monotonic: gc_floor_ never regresses
    // even if a later call passes a smaller min.
    void set_gc_watermark(uint64_t snapshot_slot, uint64_t safe_slot);

    [[nodiscard]] uint64_t gc_watermark() const
    {
        return gc_floor_.load();
    }

    // Cadence-driven block compaction (R129). Selects the sparsest source
    // blocks above the configured free-ratio threshold, capped by the
    // per-pass byte budget, and runs one snapshot that relocates their live
    // extents (resident + unloaded pages and current mapping metadata) to
    // destinations outside every selected source block. Eligible tombstones
    // are folded during the same snapshot. After the second durability
    // barrier and prepared-state commit, the shared finalizer deletes only
    // blocks unreachable from any retained anchor. Non-block stores and
    // disabled configurations return an empty stats result with no snapshot
    // write. Uses the existing snapshot gate; no new mutex or blocking lock.
    Status compact_sparse_blocks(MergeGcStats *out_stats);

    // Close the selected active table, capture a finite contiguous frontier,
    // publish its successor, then wait for only the captured writers. Publish
    // eligible prefix versions to L1 and detach wholly covered tables. Future
    // versions remain in their original sources until a later flush covers
    // them. This is in-memory publication; snapshot() establishes durability.
    Status flush();

    // Moves the currently visible L0 tables into one split-owned shared view
    // and installs a fresh active table for post-prepare writes. Normal flush
    // and reclamation leave that shared view alone until its split session
    // explicitly releases it.
    Status begin_split_memtable_view(uint64_t *out_generation, uint64_t *out_journal_frontier);

    // Makes the source's current L0 generations immediately visible to this
    // range-bounded writer at `journal_frontier`. The source outlives the
    // destination until clear_split_memtable_overlay() completes.
    Status install_split_memtable_overlay(Crowdbtree &source, uint64_t journal_frontier);

    // Stops consulting the source L0 after its filtered entries have been
    // bulk-published into this writer.
    Status clear_split_memtable_overlay(Crowdbtree &source);

    // Releases the split-owned shared view after both derived range trees have
    // durably published it. The generation fences stale release attempts.
    Status release_split_memtable_view(uint64_t generation);

    // Bulk-publishes the split-owned shared view into one range-bounded
    // destination. Source entries remain owned by the session; callers release
    // them only after every destination has durably snapshotted its result.
    Status publish_split_memtable_view(uint64_t generation, uint64_t journal_frontier, Crowdbtree &destination,
                                       const KeyRange &range);

    // Run finite flush on a completion worker. Waiting for admitted writers
    // never blocks the submitting async caller. Destruction drains workers;
    // completion callbacks run after the worker releases tree ownership.
    void flush_async(std::function<void(Status)> on_done);

    // Point read (L0 overlay then L1). Returns true if a live value is found;
    // tombstones return false.
    [[nodiscard]] bool get(Slice key, uint64_t *out_slot, std::string *out_value) const;

    // Zero-copy point read (plan-tree #5 B3 remaining): same lookup as
    // get(), but the returned GetView borrows an L1 hit's value directly
    // from its resident frame instead of copying it out. See GetView's doc.
    [[nodiscard]] GetView get_view(Slice key) const;

    // Async twin of get(). Fast path (every page needed to resolve `key` is already
    // resident, or no async backend is wired -- see
    // Config::async_page_store) invokes on_done synchronously, before this call
    // returns, exactly like get(). A genuine miss on the L1 descent (some
    // base page along the root->leaf path is tagged unloaded) submits
    // exactly one page load via the reactor and resumes automatically; on
    // completion it either resolves (calls on_done) or, if that page was
    // itself only a step deeper into the tree, hits another miss and
    // repeats -- on_done fires exactly once regardless, but for a miss it
    // runs on the Reactor thread, not the caller's.
    //
    // Scope boundary (deliberate, matches the miss scenario): only
    // the L1 base-page descent is async. A value spilled into an overflow
    // chain (large values, PT11) still resolves its chain synchronously via
    // the existing assemble_overflow_value()/resident() path -- overflow
    // chains are the less common case and the miss
    // walkthrough only describes "the leaf page is unloaded", not an
    // overflow page.
    //
    // Zero-copy fast path: `on_done`
    // receives the resolved `GetView` itself, not a copied-out std::string.
    // For the *first* attempt's synchronous resolution (this call's own
    // thread, no I/O), the GetView's epoch guard is still live -- it's safe
    // to keep borrowing an L1 hit's frame bytes all the way out to the C
    // ABI, since ct_future_free (which finally drops the guard) is
    // guaranteed to run on this same thread too. Any resolution that
    // crosses to the Reactor thread instead (a genuine miss) materializes
    // an owned copy and releases its guard before calling on_done -- see
    // get_async_attempt's `same_thread` parameter and `materialize_owned`.
    void get_async(Slice key, std::function<void(Status, GetView)> on_done) const;

    // Batched point read.
    [[nodiscard]] std::vector<get_result> multi_get(const std::vector<Slice> &keys) const;

    // Ordered range scan over keys with `prefix` (empty = whole keyspace), latest
    // state (L0 overlaid on L1). When `include_tombstones` is false (default),
    // tombstones are skipped. Returns up to `limit` entries in key order; sets
    // *truncated if more matched beyond the limit. `start_after` (empty = start
    // from the beginning) is an exclusive lower bound: only keys strictly
    // greater than `start_after` are returned. When non-empty, the descent
    // targets the leaf that would contain `start_after` instead of `prefix`,
    // so a deep-pagination scan starts at the cursor rather than walking every
    // earlier leaf in the prefix range. `end_key` (empty = unbounded) is an
    // exclusive upper bound: only keys strictly less than `end_key` are
    // returned, and the merge loop early-stops once the winner key reaches it.
    // `byte_budget` (0 = unlimited) caps the total key+value bytes emitted;
    // the scan stops with *truncated = true when exceeded, always returning at
    // least one entry (so a single oversized entry still makes progress). A
    // warning is logged for any single entry whose key+value size alone
    // exceeds the budget. `keys_only` skips value materialization (no
    // overflow-chain assembly, no value copy): entries are staged with empty
    // values and the byte budget accounts for key bytes only, so a page fits
    // more entries. Default false.
    Status scan(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget, bool keys_only,
                uint64_t deadline_ms, std::vector<scan_entry> *out, bool *truncated, bool include_tombstones = false,
                ScanPackedBuf *out_packed = nullptr, size_t *out_count = nullptr, bool has_start_bound = false,
                bool start_inclusive = false) const;

    // Resolve the greatest live key <= `start_key`, or < it when `inclusive`
    // is false, while merging L0 and L1 under one epoch guard. `begin_key` is
    // an inclusive lower bound; empty means unbounded.
    Status seek_reverse(Slice start_key, bool inclusive, Slice begin_key, scan_entry *out, bool *found) const;

    // Descending counterpart to scan(), with an explicit upper cursor and an
    // inclusive lower bound. Work and materialization are bounded by the
    // requested count and byte budget.
    Status scan_reverse(Slice start_key, bool has_start_bound, bool start_inclusive, Slice begin_key, size_t limit,
                        size_t byte_budget, std::vector<scan_entry> *out, bool *truncated) const;

    // Async twin of scan(). Unlike get_async,
    // which has exactly one possible miss point (the root->leaf descent for
    // a single key), scan() walks a whole range of leaves via
    // right_sibling and any of them -- or an inner page on the initial
    // descent to the first leaf -- can be cold. Rather than a resumable
    // cursor, a miss simply retries the *whole* scan from scratch once the
    // blocking page resolves (matches get_async_attempt's own "retry, still
    // correct, not maximally efficient" trade-off) -- each retry is pure
    // in-memory work except for exactly one more page becoming permanently
    // resident, so this always terminates and does no redundant I/O.
    // on_done fires exactly once, synchronously if the whole scan was
    // already resident (matching scan()'s cost exactly), or from the
    // Reactor thread after however many page loads were needed. `start_after`
    // is the same exclusive lower bound as scan()'s. `end_key` is the same
    // exclusive upper bound as scan()'s. `byte_budget` is the same total
    // key+value byte cap as scan()'s. `keys_only` is the same value-skip flag
    // as scan()'s.
    void scan_async(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget, bool keys_only,
                    uint64_t deadline_ms, std::function<void(Status, ScanPackedBuf, bool truncated)> on_done) const;

    // Directional form used by the ordinary KV API. `reverse` keeps
    // `start_after` as an exclusive continuation, but walks toward smaller
    // keys. With no continuation it begins below `end_key`, or below the
    // prefix successor when `end_key` is empty.
    void scan_directional_async(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget,
                                bool keys_only, uint64_t deadline_ms, bool reverse,
                                std::function<void(Status, ScanPackedBuf, bool truncated)> on_done) const;

    // pin a consistent point-in-time view at `last_applied_slot` (the published L1
    // state). Used for scan-at / compare / iter_all / snapshot export.
    // R6: returns a PinnedSnapshot (zero-copy, page refcount pins keep frames
    // alive across threads). The return type is shared_ptr<Snapshot> for ABI
    // compatibility with existing callers; PinnedSnapshot inherits from Snapshot.
    [[nodiscard]] std::shared_ptr<Snapshot> snapshot_view();

    // Replace the entire engine state with `sorted_entries` (key-sorted, including
    // tombstones) at `at_slot`, used by snapshot import. Clears L0/L1 and rebuilds
    // a fresh tree, then sets last_applied_slot = at_slot. Serialized against other
    // writers by write_mutex_. Concurrent lock-free readers are **safe** (#13): the
    // old tree is epoch-retired, not freed, so a reader mid-walk keeps its pages
    // under its guard (it may observe a transient empty/partly-replaced tree — a
    // consistent snapshot swap via a pinned RootVersion is a later refinement).
    Status install_snapshot(std::vector<leaf_entry> sorted_entries, uint64_t at_slot);

    // plan-tree #16: native snapshot format. `collect_native_frames` walks
    // the reachable tree (root -> inner children -> leaf overflow chains,
    // folding any delta chain into a fresh consolidated base first -- same
    // side effect `snapshot()` already has, unlike the read-only
    // `snapshot_view()`) and returns every base/overflow page's *raw frame
    // bytes* verbatim, tagged with its PID -- no cell decode, no tuple
    // encoding. `install_snapshot_native` is the inverse: installs each
    // frame directly as a resident page under its original PID (via
    // `from_frame_copy`, the same reconstruction demand-load already uses),
    // no entry-by-entry tree rebuild. Both intended for crowdb-tree-to-crowdb-tree
    // transfer (Raft InstallSnapshot); `install_snapshot`/`snapshot_view`'s
    // portable tuple format remains available for cross-engine scenarios
    // and testing (comparable against a non-crowdb-tree oracle). The optional
    // next-PID high-water output lets range rebuilds allocate rewritten pages
    // beyond every PID ever issued by the selected source lineage.
    Status collect_native_frames(std::vector<NativeFrame> *out, uint64_t *out_root_page_id, uint64_t *out_at_slot,
                                 uint64_t *out_next_page_id = nullptr, const KeyRange *filter = nullptr,
                                 uint64_t *out_subtrees_skipped = nullptr);
    Status open_native_frame_iterator(const KeyRange *filter, std::unique_ptr<NativeFrameIterator> *out);
    Status install_snapshot_native(std::vector<NativeFrame> frames, uint64_t root_page_id, uint64_t at_slot,
                                   uint64_t next_page_id = 0);

    // Wipe every key/value and reset watermarks back to a fresh, empty tree
    // (the same wipe `install_snapshot` performs on the live tree before
    // loading imported entries, factored out for a caller that wants an
    // empty tree with nothing to load afterward -- e.g. resetting a
    // diverged/corrupted replica in place before a snapshot import).
    // Serialized against other writers by write_mutex_; concurrent
    // lock-free readers are safe (#13), same as `install_snapshot`. Not
    // durable by itself -- an explicit `snapshot()`/`flush()` afterward is
    // required to persist the wipe to a file-backed store.
    Status clear();

    // Reassemble a large value spilled into an overflow chain headed at `head_page_id`
    // (PT11). Walks the chain via resident under the caller's read epoch guard.
    [[nodiscard]] std::string assemble_overflow_value(uint64_t head_page_id, uint64_t total_len) const;

    [[nodiscard]] uint64_t last_applied_slot() const
    {
        return last_applied_slot_.load();
    }

    [[nodiscard]] uint64_t contiguous_slot() const
    {
        return contiguous_slot_.load();
    }

    [[nodiscard]] uint64_t version() const
    {
        return version_.load();
    }

    // Read one immutable base frame through a bounded root/child path. Does not
    // flush memtables, fold deltas, or follow overflow values. UINT64_MAX starts
    // a new observation; subsequent requests must retain the returned version.
    Status inspect_page(const std::vector<uint32_t> &path, uint64_t expected_version, NativeFrame *out,
                        uint64_t *out_version, uint64_t *out_root, uint32_t *out_deltas) const;

    [[nodiscard]] uint64_t durable_snapshot_seq() const
    {
        return durable_snapshot_seq_.load(std::memory_order_acquire);
    }

    [[nodiscard]] uint64_t durable_snapshot_last_applied_slot() const
    {
        return durable_snapshot_last_applied_slot_.load(std::memory_order_acquire);
    }

    [[nodiscard]] uint64_t root_page_id() const
    {
        return root_page_id_.load();
    }

    // Latched media-fault flag (design follow-up). A demand-load that fails to
    // read or validate a durable page (`resident`) cannot return an error through
    // the lock-free read path, so it latches this flag (and the page reads as a
    // miss). A caller can poll this after reads to detect on-disk corruption /
    // I/O faults and fail the node out of the group. `clear_io_error` resets it.
    [[nodiscard]] bool io_failed() const
    {
        return io_failed_.load();
    }

    void clear_io_error()
    {
        io_failed_.store(false);
    }

    // Diagnostics: total entries across every live MemTable (active_ + any
    // not-yet-drained frozen_ buffers), not just active_.
    [[nodiscard]] size_t memtable_count() const;

    // Gap 5 step 2: number of frozen memtables waiting to be drained.
    // The maintenance loop polls this to decide whether to flush
    // immediately or sleep until the next tick.
    [[nodiscard]] size_t frozen_table_count() const;

    MappingTable &mapping()
    {
        return mapping_;
    }

    // Diagnostics/tests: the tree-owned epoch manager (plan-tree #7).
    EpochManager &epoch()
    {
        return epoch_;
    }

    [[nodiscard]] const BufferPool *buffer_pool() const
    {
        return pool_.get();
    }

    [[nodiscard]] int    height() const;     // 1 = single-leaf root
    [[nodiscard]] size_t leaf_count() const; // live leaves reachable from the root

    // O(1) atomic snapshot of the leaf/inner page counters (maintained at
    // SMO sites, restored from the commit anchor on open()). For tests and
    // metrics — prefer these over leaf_count() (which walks the tree).
    [[nodiscard]] uint64_t leaf_count_atomic() const
    {
        return leaf_count_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] uint64_t inner_count_atomic() const
    {
        return inner_count_.load(std::memory_order_relaxed);
    }

    // # of base pages physically written by the most recent snapshot (the rest
    // were clean and retained their durable addr). For incremental-snapshot tests.
    [[nodiscard]] uint64_t last_snapshot_pages_written() const
    {
        return snapshot_pages_written_.load();
    }

    // # of mapping-table segment images physically written by the most
    // recent snapshot (plan-tree #14c/#14d: the rest were !is_dirty() and
    // reused their existing image_addr/generation as-is). For
    // incremental-snapshot tests -- the segment-level analogue of
    // last_snapshot_pages_written().
    [[nodiscard]] uint64_t last_snapshot_segments_written() const
    {
        return snapshot_segments_written_.load();
    }

    // Batched diagnostics snapshot -- see EngineStats. O(1): every field
    // reads an already-tracked atomic counter or BufferPool::stats() (also
    // O(1)), so this is safe to poll periodically (e.g. from a metrics
    // scrape or console panel refresh).
    [[nodiscard]] EngineStats stats() const;
    [[nodiscard]] TreeSummary tree_summary() const;

    // Destructive read of the per-step scan profile since the last call: flushes
    // the scan LatencySummary/Counter handles and returns per-step sum/max/avg.
    // Returns an all-zero profile if init_metrics() was never called.
    [[nodiscard]] ScanProfile scan_profile() const;

    // Create the internal MetricsRegistry and register all handles
    // using the provided name prefix (e.g. "s.1.g.0"). Called from open().
    void init_metrics(const std::string &prefix, const std::string &backend_label);

    // Flush all C++ metrics into a formatted string (for FFI return to
    // Rust). Uses open_memstream internally. `width` overrides the
    // per-section max name length for column alignment with the Rust
    // section (0 = use internal max).
    std::string flush_metrics_str(double window_secs, const char *timestamp, size_t width = 0, size_t count_w = 0,
                                  size_t tps_w = 0);

    // Return the current max metric name length (for Rust's shared-width
    // computation).
    size_t max_name_len() const;

    // Evict clean, delta-free resident leaf bases down to at most
    // `max_resident_leaves`, re-tagging their slots unloaded and epoch-retiring the
    // pages; returns the number evicted. Safe against lock-free
    // readers (epoch-deferred frame reuse); evicted pages reload on next access.
    [[nodiscard]] size_t evict_clean_leaves(size_t max_resident_leaves);

    // plan-tree #17 D3: same contract as evict_clean_leaves, but for clean,
    // delta-free resident *inner* bases, ranked and budgeted entirely
    // separately -- see evict_clean_inner_locked's doc comment (crowdb-tree.cpp)
    // for why a combined leaf+inner budget is unsafe (breaks the
    // just-touched-leaf-survives-eviction guarantee). Never evicts a leaf;
    // evict_clean_leaves never evicts an inner base.
    [[nodiscard]] size_t evict_clean_inner(size_t max_resident_inner);

    // Effective key size limit (opt_.max_key_size or frame_bytes/2). Keys larger
    // than this are rejected at apply() (plan-tree #15).
    [[nodiscard]] size_t max_key_size() const
    {
        return opt_.max_key_size != 0 ? opt_.max_key_size : opt_.frame_bytes / 2;
    }

  private:
    friend class NativeFrameIterator;
    friend Status      rebuild_range(Crowdbtree &source, const KeyRange &range, Config destination_options,
                                     std::unique_ptr<Crowdbtree> *out, RangeRebuildStats *stats);
    [[nodiscard]] bool seek_reverse_guarded(Slice start_key, bool has_start_bound, bool inclusive, Slice begin_key,
                                            const std::vector<MemTableSource> &memtables, uint64_t root_page_id,
                                            uint64_t gc_floor, scan_entry *out) const;

    [[nodiscard]] bool try_scan_reverse_no_load(Slice prefix, Slice start_after, Slice end_key, size_t limit,
                                                size_t byte_budget, bool keys_only, uint64_t deadline_ms,
                                                ScanPackedBuf *out_packed, size_t *out_count, bool *truncated,
                                                uint64_t *out_pending_page_id) const;
#ifdef CROWDB_TREE_TEST_UTIL
    friend struct MemTableAccess_for_tests;
    void (*after_flush_group_for_tests_)() = nullptr;
#endif
    struct FlushBoundary
    {
        std::deque<std::shared_ptr<MemTable>> tables;
        uint64_t                              frontier = 0;
    };

    FlushBoundary capture_flush_locked();
    void          publish_flush_locked(FlushBoundary &boundary);
    // apply a batch's ops into L0 at `slot` (intra-batch last-op-wins).
    void          apply_batch(uint64_t slot, const Batch &batch);
    MemTableBatch admit_batch();
    void          finish_batch(MemTableBatch &batch, uint64_t slot, const std::vector<std::string> &keys);
    // Shared apply()/apply_encoded() tail: slot bookkeeping (max_seen_slot_,
    // received_slots_, contiguous frontier) then a possible L0 size-based swap.
    void note_applied_slot(uint64_t slot);
    // Fold newly received slots into the contiguous prefix, then prune the
    // tracker below the new frontier. Caller holds slot_mutex_.
    void recompute_contiguous_locked();

    [[nodiscard]] std::shared_ptr<MemTable>   current_active() const;
    [[nodiscard]] std::vector<MemTableSource> all_memtables() const;
    [[nodiscard]] std::vector<MemTableSource> local_memtables(uint64_t covered = UINT64_MAX) const;
    bool                                      maybe_freeze_active(bool force);
    void                                      maybe_swap_active();
    bool drain_all_frozen_locked(std::deque<std::shared_ptr<MemTable>> &to_drain, uint64_t cs);
    bool publish_group_to_leaf_locked(uint64_t page_id, uint64_t cs, std::vector<leaf_entry> group);

    void                  consolidate_locked(uint64_t page_id);          // caller holds write_mutex_
    void                  maybe_split_or_merge_locked(uint64_t page_id); // dispatch on leaf size
    void                  set_children_parent_locked(uint64_t page_id, uint64_t parent_page_id);
    void                  store_preserving_parent_locked(uint64_t page_id, PageBase *new_page);
    void                  sync_page_count_gauges();
    std::vector<uint64_t> path_to_page_id_locked(uint64_t target_page_id) const;
    void                  split_leaf_to_threshold_locked(uint64_t leaf_page_id);
    void                  split_leaf_locked(uint64_t leaf_page_id, std::vector<uint64_t> path);
    void                  propagate_split_locked(std::vector<uint64_t> path, uint64_t child_page_id, std::string sep,
                                                 uint64_t right_page_id);
    void                  try_merge_leaf_locked(uint64_t leaf_page_id, const std::vector<uint64_t> &path);
    void                  try_merge_inner_locked(uint64_t inner_page_id, std::vector<uint64_t> path);

    [[nodiscard]] uint32_t inner_merge_keys() const
    {
        if (opt_.inner_merge_keys != 0) {
            return opt_.inner_merge_keys;
        }
        uint32_t q = opt_.inner_max_keys / 4;
        return q != 0 ? q : 1;
    }

    void   retire_page(PageBase *p);
    void   preserve_native_page_locked(uint64_t page_id, PageBase *page);
    void   preserve_native_generation_locked();
    void   detach_native_iterators();
    Status install_range_snapshot_native(std::vector<NativeFrame> frames, uint64_t root_page_id, uint64_t at_slot,
                                         uint64_t next_page_id, bool mapping_inherited);
    void   retire_orphaned_page(uint64_t page_id, PageBase *p);
    void   free_subtree(uint64_t page_id, bool retire);
    void   free_all_resident_pages(bool retire);

    [[nodiscard]] size_t max_inline_value() const
    {
        return opt_.max_inline_value != 0 ? opt_.max_inline_value : opt_.frame_bytes / 4;
    }

    [[nodiscard]] static std::vector<leaf_entry>
    resolve_leaf_chain_for_rebuild(PageBase *head, uint64_t gc_floor, std::vector<uint64_t> *dead_overflow,
                                   size_t *out_tombstones_dropped = nullptr, size_t *out_bytes_dropped = nullptr);
    [[nodiscard]] uint64_t  spill_value_to_overflow_chain_locked(const std::string &value);
    [[nodiscard]] LeafBase *build_leaf_spilling_locked(std::vector<leaf_entry> entries, uint64_t right_sibling);
    void                    retire_overflow_chain_locked(uint64_t head_page_id);
    void                    evict_overflow_chain_locked(uint64_t head_page_id);
    void                    free_overflow_chain(uint64_t head_page_id);
    size_t                  evict_clean_leaves_locked(size_t max_resident_leaves); // caller holds write_mutex_
    size_t                  evict_clean_inner_locked(size_t max_resident_inner);   // caller holds write_mutex_
    void                    maybe_evict_locked(); // capacity-driven auto-evict (caller holds write_mutex_)
    [[nodiscard]] PageBase *resident(uint64_t page_id) const;

    void capture_overflow_chain(uint64_t head_page_id, std::vector<PageBase *> &out);

    [[nodiscard]] PageBase *install_loaded_page(uint64_t page_id, uint64_t addr, uint32_t plen,
                                                const std::vector<uint8_t> &blob) const;

    [[nodiscard]] bool try_get_view_no_load(Slice key, GetView *result, uint64_t *out_pending_page_id) const;

    void get_async_attempt(std::shared_ptr<std::string> key_owned, std::function<void(Status, GetView)> on_done,
                           bool same_thread) const;

    static GetView materialize_owned(GetView &&v);

    [[nodiscard]] bool try_scan_no_load(Slice prefix, Slice start_after, Slice end_key, size_t limit,
                                        size_t byte_budget, bool keys_only, uint64_t deadline_ms,
                                        std::vector<scan_entry> *out, bool *truncated, uint64_t *out_pending_page_id,
                                        ScanPackedBuf *out_packed = nullptr, size_t *out_count = nullptr) const;

    void scan_async_attempt(std::shared_ptr<std::string>        prefix_owned,
                            const std::shared_ptr<std::string> &start_after_owned,
                            const std::shared_ptr<std::string> &end_key_owned, size_t limit, size_t byte_budget,
                            bool keys_only, uint64_t deadline_ms, std::shared_ptr<ScanPackedBuf> accumulated,
                            std::shared_ptr<std::string> last_key, size_t accumulated_count,
                            std::function<void(Status, ScanPackedBuf, bool)> on_done) const;

    void scan_reverse_async_attempt(std::shared_ptr<std::string>        prefix_owned,
                                    const std::shared_ptr<std::string> &start_after_owned,
                                    const std::shared_ptr<std::string> &end_key_owned, size_t limit, size_t byte_budget,
                                    bool keys_only, uint64_t deadline_ms, std::shared_ptr<ScanPackedBuf> accumulated,
                                    std::shared_ptr<std::string> last_key, size_t accumulated_count,
                                    std::function<void(Status, ScanPackedBuf, bool)> on_done) const;

    Status prepare_snapshot_locked(PreparedSnapshot *out, std::vector<PrefetchedPage> prefetched = {},
                                   std::set<uint32_t> relocation_blocks = {});
    struct SnapshotPrepareContext;
    Status prepare_snapshot_pages_locked(SnapshotPrepareContext &ctx);
    Status fold_snapshot_page_locked(uint64_t page_id, uint64_t gc, PageBase **page);
    Status queue_snapshot_page_locked(SnapshotPrepareContext &ctx, uint64_t page_id, PageBase *page,
                                      const uint8_t *frame, uint32_t frame_len, bool relocate, uint64_t *addr,
                                      uint32_t *logical_len) const;
    Status prepare_snapshot_resident_page_locked(SnapshotPrepareContext &ctx, uint64_t page_id, uint64_t seg_idx);
    Status prepare_snapshot_segments_locked(SnapshotPrepareContext &ctx);
    static Status prepare_snapshot_slot_locked(SnapshotPrepareContext &ctx, uint64_t page_id, uint64_t word,
                                               uint64_t *durable_word, uint32_t *live_count);
    static Status prepare_snapshot_segment_locked(SnapshotPrepareContext &ctx, uint64_t seg_idx,
                                                  MappingSegment *segment);
    void          prepare_snapshot_metadata_locked(SnapshotPrepareContext &ctx);
    Status        prefetch_sparse_pages(std::vector<PrefetchedPage> *out, std::set<uint32_t> *selected_blocks);
    Status persist_compaction_snapshot(std::vector<PrefetchedPage> prefetched, std::set<uint32_t> selected_blocks,
                                       PreparedSnapshot *prepared);
    void   record_compaction_metrics(const MergeGcStats &stats, uint64_t elapsed_ns);

    void commit_prepared_snapshot(const PreparedSnapshot &prepared);
    void finalize_prepared_snapshot(PreparedSnapshot &prepared);

    void acquire_snapshot_slot();
    void release_snapshot_slot();

    void snapshot_write_next_async(std::shared_ptr<PreparedSnapshot> prepared, size_t idx,
                                   std::function<void(Status, uint64_t last_applied)> on_done);

    Config                      opt_;
    std::string                 name_;
    std::shared_ptr<BufferPool> pool_;
    MappingTable                mapping_;

    // The catalog lock protects source ownership and its published L1 floor.
    // Writers register on the selected table before mutation; close/capture/
    // successor publication is one catalog transition. Frozen tables retain
    // their immutable future versions until a later prefix covers the table.
    // Borrowers keep source ownership and epoch protection independently.
    std::shared_ptr<std::atomic<Crowdbtree *>> reclamation_owner_ = std::make_shared<std::atomic<Crowdbtree *>>(this);
    std::shared_ptr<std::atomic<uint64_t>>     async_flushes_     = std::make_shared<std::atomic<uint64_t>>(0);
    bool                                       publication_incomplete_ = false; // protected by write_mutex_
    MemTableCounters                           memtable_counters_;
    GenerationGate                             generation_;
    mutable std::shared_mutex                  memtable_mutex_;
    std::shared_ptr<MemTable>                  active_;
    std::deque<std::shared_ptr<MemTable>>      frozen_;
    std::vector<std::shared_ptr<MemTable>>     split_shared_memtables_;
    std::atomic<Crowdbtree *>                  split_overlay_source_{nullptr};
    std::atomic<uint64_t>                      split_overlay_frontier_{0};
    uint64_t                                   split_memtable_generation_ = 0;
    uint64_t                                   split_memtable_frontier_   = 0;
    std::atomic<uint64_t>                      memtable_next_id_{1}; // monotonic MemTable id for logging

    // internal_error slot tracker (replaces the caller-supplied contiguous_slot). Holds
    // received-but-not-yet-contiguous slots above contiguous_slot_; the contiguous
    // prefix is folded forward on each apply/force_advance_slot and pruned below
    // the frontier to stay bounded. Guarded by slot_mutex_.
    mutable std::mutex    slot_mutex_;
    std::set<uint64_t>    received_slots_;
    uint64_t              max_seen_slot_ = 0;
    std::atomic<uint64_t> auto_slot_{0}; // next auto-assigned slot for put/del/batch_put

    std::atomic<uint64_t> root_page_id_{kInvalidPageId};
    std::atomic<uint64_t> contiguous_slot_{0};
    std::atomic<uint64_t> last_applied_slot_{0};
    std::atomic<uint64_t> version_{0};
    std::atomic<uint64_t> durable_snapshot_seq_{0};
    std::atomic<uint64_t> durable_snapshot_last_applied_slot_{0};

    struct MappingMaterializationState
    {
        uint64_t              version      = 0;
        uint64_t              next_segment = 0;
        std::vector<uint64_t> stack;
        std::vector<uint64_t> reachable;
    };

    std::unique_ptr<MappingMaterializationState> mapping_materialization_;
    uint64_t                                     mapping_pruned_version_ = std::numeric_limits<uint64_t>::max();
    std::atomic<uint64_t>                        gc_floor_{0};
    std::atomic<uint64_t>                        snapshot_pages_written_{0}; // pages written by last snapshot
    std::atomic<uint64_t> snapshot_pages_total_{0};      // cumulative pages written across all snapshots
    std::atomic<uint64_t> snapshot_segments_written_{0}; // segment images written by last snapshot
    // Freshly built and native-imported trees have globally verified routing
    // separators. Lazy persistent recovery verifies them on the first bounded
    // export before allowing separator-based subtree pruning.
    std::atomic<bool>         routing_fences_trusted_{true};
    mutable std::atomic<bool> io_failed_{false}; // latched demand-load media fault

    // Cumulative operation counters (monotonic since open, exposed via stats()).
    // mutable: get_view() is const but increments these counters.
    mutable std::atomic<uint64_t> mt_upsert_total_{0};     // apply() writes into L0
    mutable std::atomic<uint64_t> mt_get_total_{0};        // get() lookups in L0
    mutable std::atomic<uint64_t> mt_get_hit_total_{0};    // L0 lookups that found a cell
    mutable std::atomic<uint64_t> flush_drain_total_{0};   // drain_all_frozen_locked calls
    mutable std::atomic<uint64_t> flush_entries_total_{0}; // entries drained from L0 to L1
    mutable std::atomic<uint64_t> snapshot_total_{0};      // snapshot() calls (durable checkpoints)
    mutable std::atomic<uint64_t> l1_get_total_{0};        // get() lookups that descended to L1
    mutable std::atomic<uint64_t> l1_get_hit_total_{0};    // L1 lookups that found a cell
    mutable std::atomic<uint64_t> map_lookup_total_{0};    // mapping table lookups (find_leaf_page_id / resident)
    mutable std::atomic<uint64_t> demand_load_total_{0};   // demand-load page faults

    // Live leaf/inner page counts (O(1) gauges, maintained at SMO sites and
    // restored from the commit anchor on open()). See leaf_count_atomic() /
    // inner_count_atomic(). An empty tree starts at leaf=1 (root leaf), inner=0.
    std::atomic<uint64_t>         leaf_count_{1};
    std::atomic<uint64_t>         inner_count_{0};
    mutable std::atomic<uint64_t> summary_root_version_{0};
    mutable std::atomic<uint64_t> summary_covered_slot_{0};
    mutable std::atomic<uint64_t> summary_live_kv_{0};
    mutable std::atomic<uint64_t> summary_live_key_bytes_{0};
    mutable std::atomic<uint64_t> summary_live_value_bytes_{0};
    mutable std::atomic<bool>     summary_available_{false};

    // Logical clock for CLOCK-informed eviction ranking (plan-tree #17).
    // `resident()`'s hot path bumps this and stamps the touched page's own
    // `PageBase::last_touch_tick` on every access (a single relaxed atomic
    // fetch_add + store -- no lock, so the existing lock-free read path
    // stays lock-free). `evict_clean_leaves_locked` then ranks its
    // DFS-gathered evictable set by that stamp (oldest first) instead of
    // arbitrary DFS order. This is deliberately *not* `BufferPool::pin`'s
    // own mutex-guarded page_id/CLOCK tracking: wiring every `resident()`
    // hit through that would mean taking a global pool mutex on every page
    // access, regressing the lock-free read path #5 B3/#12/#13 built (see
    // "residency/eviction driven by real access recency, not arbitrary
    // order" goal without that cost.
    mutable std::atomic<uint64_t> touch_tick_{0};

    mutable std::mutex                                    write_mutex_; // serializes flush / consolidate / split-merge
    std::vector<std::weak_ptr<NativeFrameIterator::Impl>> native_frame_iterators_;
    mutable std::mutex                                    load_mutex_; // serializes cold-path demand loads
    // Serializes snapshot(_async) generations against each other across
    // snapshot_async's async write phase, where write_mutex_ itself can't be
    // held (see acquire_snapshot_slot's doc comment and snapshot_async's).
    std::atomic<bool> snapshot_inflight_{false};

    // Block indices that were empty in the previous snapshot (two-generation
    // rule for block deletion). A block is only deleted after it's empty in
    // two consecutive snapshots — the crash fallback anchor still references it.

    // Tree-owned epoch-based reclamation (plan-tree #7; formerly on CrowdbtreeEnv).
    // Declared last so it is destroyed first: ~Crowdbtree frees the live tree via
    // free_subtree(root, /*retire=*/false) (no readers at teardown), then epoch_'s
    // destructor reclaims any pages still pending from earlier retire()s (eviction,
    // consolidation, install_snapshot) while pool_ / mapping_ are still alive.
    // mutable: readers take a guard in const get().
    mutable EpochManager epoch_;

    // ── Metrics handles (registered in init_metrics) ──
    struct MetricsHandles
    {
        LatencySummary *mt_version_copy_l = nullptr;
        // Buffer pool (backend I/O)
        Counter *buf_evictions  = nullptr;
        Counter *buf_writebacks = nullptr;
        Gauge   *buf_resident   = nullptr;
        Gauge   *buf_dirty      = nullptr;
        // Flush (L0 → L1)
        LatencySummary *flush_l              = nullptr;
        Counter        *flush_drain_c        = nullptr;
        Counter        *flush_entries_c      = nullptr;
        LatencySummary *split_view_begin_l   = nullptr;
        LatencySummary *split_view_publish_l = nullptr;
        LatencySummary *split_view_release_l = nullptr;
        // MemTable (L0) operation latency
        LatencySummary *mt_apply_l   = nullptr;
        Counter        *mt_get_c     = nullptr;
        Counter        *mt_get_hit_c = nullptr;
        LatencySummary *mt_get_l     = nullptr;
        CallbackGauge  *mt_frozen_g  = nullptr;
        CallbackGauge  *mt_records_g = nullptr;
        Counter        *mt_freeze_c  = nullptr;
        // L1 (B-tree) query counters + latency
        Counter        *l1_get_c     = nullptr;
        Counter        *l1_get_hit_c = nullptr;
        LatencySummary *l1_get_l     = nullptr;
        // B+tree page mutation (during drain/split/merge/consolidate)
        LatencySummary *page_write_l       = nullptr;
        Counter        *page_split_c       = nullptr;
        Counter        *page_merge_c       = nullptr;
        Counter        *page_consolidate_c = nullptr;
        // Tree structure
        CallbackGauge *tree_height_g        = nullptr;
        Gauge         *tree_leaf_count_g    = nullptr;
        Gauge         *tree_inner_count_g   = nullptr;
        CallbackGauge *tree_retired_count_g = nullptr;
        // Mapping table
        Counter       *page_find_c           = nullptr;
        Counter       *page_map_alloc_c      = nullptr;
        CallbackGauge *page_map_total_pids_g = nullptr;
        CallbackGauge *page_map_segments_g   = nullptr;
        // Demand-load (page fault I/O) latency
        LatencySummary *page_load_l = nullptr;
        // Page writeback (eviction I/O) latency
        LatencySummary *page_writeback_l = nullptr;
        // Page writeback bandwidth (eviction I/O, non-snapshot)
        Bandwidth *page_writeback_bw = nullptr;
        // fsync/barrier latency
        LatencySummary *fsync_l = nullptr;
        // Snapshot — logical (full snapshot wall time)
        LatencySummary *snapshot_l = nullptr;
        // Snapshot I/O sub-metrics (backend)
        LatencySummary *snapshot_apply_l            = nullptr;
        LatencySummary *snapshot_page_write_l       = nullptr;
        Counter        *snapshot_page_write_cache_c = nullptr;
        Bandwidth      *snapshot_page_write_bw      = nullptr;
        Bandwidth      *snapshot_meta_write_bw      = nullptr;
        Bandwidth      *page_read_bw                = nullptr;
        Counter        *snapshot_pages_c            = nullptr;
        // Scan
        Counter        *scan_entries_c = nullptr;
        LatencySummary *scan_l         = nullptr;
        LatencySummary *scan_l0_l      = nullptr;
        LatencySummary *scan_l1_l      = nullptr;
        LatencySummary *scan_merge_l   = nullptr;
        Counter        *scan_retry_c   = nullptr;
        // GC (snapshot folding + block compaction)
        Counter        *gc_tombstones_c      = nullptr; // tombstones dropped by snapshot folding
        Counter        *merge_gc_blocks_c    = nullptr; // source blocks selected per compaction pass
        Counter        *merge_gc_relocated_c = nullptr; // pages relocated per compaction pass
        Counter        *merge_gc_deleted_c   = nullptr; // blocks deleted per compaction pass
        LatencySummary *merge_gc_l           = nullptr; // compaction pass latency
    };

    MetricsHandles metrics_;

    // Internal metrics registry (owned by the engine).
    std::unique_ptr<MetricsRegistry> metrics_registry_;
};

} // namespace crowdb::tree
