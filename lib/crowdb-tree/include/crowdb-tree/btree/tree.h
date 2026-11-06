// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// A Tree facade delegates every operation to one immutable-publication core.

#pragma once

#include "crowdb-common/metrics/metrics.h"
#include "crowdb-tree/btree/cell.h"
#include "crowdb-tree/btree/diagnostics.h"
#include "crowdb-tree/btree/read_result.h"
#include "crowdb-tree/btree/scan_packed.h"
#include "crowdb-tree/config.h"
#include "crowdb-tree/maptable/page.h"
#include "crowdb-tree/snapshot/native_frame.h"
#include "crowdb-tree/snapshot/snapshot.h"
#include "crowdb-tree/status.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace crowdb::tree
{

struct RangeRebuildStats;

using crowdb::common::metrics::Bandwidth;
using crowdb::common::metrics::CallbackGauge;
using crowdb::common::metrics::Counter;
using crowdb::common::metrics::Gauge;
using crowdb::common::metrics::LatencySummary;
using crowdb::common::metrics::MetricsRegistry;

class AsyncPageStore;
class TreeCore;

struct batch_op
{
    std::string key;
    OpKind      kind;
    std::string value;
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

    // Recover durable state or initialize an empty backend.
    static Status open(const Config &opt, std::unique_ptr<Crowdbtree> *out);
    // Host-only migration. Stop and drain every legacy writer first, claim its
    // catalog authority, and inject the runtime Chunk factory. The exact old
    // generation stays pinned until the new root CAS is confirmed. On failure
    // reopen with a fresh catalog incarnation before resolving/retrying.
    static Status migrate_legacy_chunk(const Config &opt, uint64_t expected_generation,
                                       std::unique_ptr<Crowdbtree> *out);

    // Finite source handoff, frame writes, durable root commit, and publication reconciliation.
    Status snapshot(uint64_t *out_last_applied = nullptr, uint64_t *out_snapshot_seq = nullptr);

    Status materialize_ownership(uint64_t *bytes_written, bool *complete);

    // Completion runs on the shared host runtime; close is a host-thread operation.
    void snapshot_async(std::function<void(Status, uint64_t last_applied)> on_done);

    // Explicit-slot apply preserves higher versions beyond a contiguous gap.
    Status apply(uint64_t slot, const Batch &batch);

    struct encoded_op
    {
        std::string key;
        buffer      cell;
    };

    Status apply_encoded(uint64_t slot, std::vector<encoded_op> ops);

    struct external_op
    {
        std::string key;
        uint8_t     flags = 0;
        buffer      value;
    };

    Status apply_external(uint64_t slot, std::vector<external_op> ops);

    void force_advance_slot(uint64_t slot);

    // Automatic slots advance past explicit apply and NoOp slots.
    Status put(Slice key, Slice value);
    Status del(Slice key);
    Status batch_put(const Batch &batch);

    [[nodiscard]] Status validate_key(Slice key) const;

    [[nodiscard]] std::optional<std::string> approximate_split_key() const;

    void set_gc_watermark(uint64_t snapshot_slot, uint64_t safe_slot);

    [[nodiscard]] uint64_t gc_watermark() const;

    Status compact_sparse_blocks(MergeGcStats *out_stats);

    Status flush();

    Status begin_split_memtable_view(uint64_t *out_generation, uint64_t *out_journal_frontier);

    Status install_split_memtable_overlay(Crowdbtree &source, uint64_t journal_frontier);

    Status clear_split_memtable_overlay(Crowdbtree &source);

    Status release_split_memtable_view(uint64_t generation);

    Status publish_split_memtable_view(uint64_t generation, uint64_t journal_frontier, Crowdbtree &destination,
                                       const KeyRange &range);

    void flush_async(std::function<void(Status)> on_done);

    [[nodiscard]] bool get(Slice key, uint64_t *out_slot, std::string *out_value) const;

    // The returned payload owns its bytes and remains valid after Tree close.
    [[nodiscard]] GetView get_view(Slice key) const;
    // Preserve transient admission and storage errors for status-aware callers.
    [[nodiscard]] Status get_view(Slice key, GetView *out) const;

    void get_async(Slice key, std::function<void(Status, GetView)> on_done) const;

    [[nodiscard]] std::vector<get_result> multi_get(const std::vector<Slice> &keys) const;

    Status scan(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget, bool keys_only,
                uint64_t deadline_ms, std::vector<scan_entry> *out, bool *truncated, bool include_tombstones = false,
                ScanPackedBuf *out_packed = nullptr, size_t *out_count = nullptr, bool has_start_bound = false,
                bool start_inclusive = false, bool strict_byte_budget = false) const;

    Status seek_reverse(Slice start_key, bool inclusive, Slice begin_key, scan_entry *out, bool *found) const;

    Status scan_reverse(Slice start_key, bool has_start_bound, bool start_inclusive, Slice begin_key, size_t limit,
                        size_t byte_budget, std::vector<scan_entry> *out, bool *truncated) const;

    void scan_async(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget, bool keys_only,
                    uint64_t deadline_ms, std::function<void(Status, ScanPackedBuf, bool truncated)> on_done) const;

    void scan_directional_async(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget,
                                bool keys_only, uint64_t deadline_ms, bool reverse,
                                std::function<void(Status, ScanPackedBuf, bool truncated)> on_done) const;

    [[nodiscard]] std::shared_ptr<Snapshot> snapshot_view();

    Status install_snapshot(std::vector<leaf_entry> sorted_entries, uint64_t at_slot);

    Status collect_native_frames(std::vector<NativeFrame> *out, uint64_t *out_root_page_id, uint64_t *out_at_slot,
                                 uint64_t *out_next_page_id = nullptr, const KeyRange *filter = nullptr,
                                 uint64_t *out_subtrees_skipped = nullptr);
    Status open_native_frame_iterator(const KeyRange *filter, std::unique_ptr<NativeFrameIterator> *out);
    Status install_snapshot_native(std::vector<NativeFrame> frames, uint64_t root_page_id, uint64_t at_slot,
                                   uint64_t next_page_id = 0);

    // Install an empty namespace atomically; a subsequent snapshot makes it durable.
    Status clear();

    [[nodiscard]] uint64_t last_applied_slot() const;
    [[nodiscard]] uint64_t contiguous_slot() const;
    [[nodiscard]] uint64_t version() const;

    Status inspect_page(const std::vector<uint32_t> &path, uint64_t expected_version, NativeFrame *out,
                        uint64_t *out_version, uint64_t *out_root, uint32_t *out_deltas) const;

    [[nodiscard]] uint64_t durable_snapshot_seq() const;
    [[nodiscard]] uint64_t durable_snapshot_last_applied_slot() const;
    [[nodiscard]] uint64_t root_page_id() const;

    [[nodiscard]] bool io_failed() const
    {
        return io_failed_.load();
    }

    void clear_io_error()
    {
        io_failed_.store(false);
    }

    [[nodiscard]] size_t memtable_count() const;

    [[nodiscard]] size_t frozen_table_count() const;

    [[nodiscard]] int    height() const;
    [[nodiscard]] size_t leaf_count() const;

    [[nodiscard]] uint64_t leaf_count_atomic() const;
    [[nodiscard]] uint64_t inner_count_atomic() const;

    // Physical frames written by the latest successful durable checkpoint; excludes reused frames.
    [[nodiscard]] uint64_t last_snapshot_pages_written() const;

    // Deprecated dirty-segment observation. Use actual written-frame counts; returns zero.
    [[nodiscard]] uint64_t last_snapshot_segments_written() const
    {
        return 0;
    }

    [[nodiscard]] EngineStats          stats() const;
    [[nodiscard]] TreeStatsSnapshot    tree_state_stats() const;
    [[nodiscard]] BackingStatsSnapshot backing_stats() const noexcept;
    [[nodiscard]] TreeSummary          tree_summary() const;

    // Deprecated empty compatibility profile; use the work metrics collector.
    [[nodiscard]] ScanProfile scan_profile() const;

    void init_metrics(const std::string &prefix, const std::string &backend_label);

    std::string flush_metrics_str(double window_secs, const char *timestamp, size_t width = 0, size_t count_w = 0,
                                  size_t tps_w = 0);

    size_t max_name_len() const;

    [[nodiscard]] size_t evict_clean_leaves(size_t max_resident_leaves);

    [[nodiscard]] size_t evict_clean_inner(size_t max_resident_inner);

    [[nodiscard]] size_t max_key_size() const
    {
        return opt_.max_key_size != 0 ? opt_.max_key_size : opt_.frame_bytes / 2;
    }

  private:
    friend struct TreeCoreAccess;
    friend Status rebuild_range(Crowdbtree &, const KeyRange &, Config, std::unique_ptr<Crowdbtree> *,
                                RangeRebuildStats *);
    Crowdbtree(Config opt, std::shared_ptr<TreeCore> core);
    void record_compaction_metrics(const MergeGcStats &stats, uint64_t elapsed_ns);
    void init_core_metrics(const std::string &prefix);

    Config                           opt_;
    std::string                      name_;
    std::shared_ptr<TreeCore>        core_;
    AllocationLease                  core_facade_charge_;
    std::shared_ptr<MetricsRegistry> metrics_registry_;
    mutable std::atomic<bool>        io_failed_{false};
};
} // namespace crowdb::tree
