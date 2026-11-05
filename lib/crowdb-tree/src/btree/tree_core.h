// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/flush_operation.h"
#include "btree/page_cache.h"
#include "btree/published_read.h"
#include "crowdb-tree/btree/tree.h"
#include "runtime/tree_operation_lifetime.h"
#include "runtime/tree_runtime_binding.h"

namespace crowdb::tree
{
struct BackingStatsSampler;

namespace detail
{
class ChunkRuntimePageStore;
class LegacyChunkReadStore;
} // namespace detail
struct LocalBackendState;
class CheckpointBackend;
class CheckpointOperation;
class ChunkCollectionOperation;
class LocalCompactionOperation;
struct LocalCompactionOptions;
class TreeApplyOperation;
struct CoreWriteInput;
struct ReadablePageCatalog;
struct CheckpointOptions;
struct SplitGeneration;
struct SplitSourceSession;

struct TreeCoreBootstrap
{
    std::shared_ptr<const PublishedTreeState> initial;
    std::shared_ptr<LocalBackendState>        local;
    std::shared_ptr<CheckpointBackend>        chunk = nullptr;
};

// Borrowed inputs are copied into the operation before async submission.
struct CoreScanOptions
{
    Slice    prefix;
    Slice    start;
    Slice    end;
    Slice    begin;
    size_t   limit              = 0;
    size_t   byte_budget        = 0;
    uint64_t deadline_ms        = 0;
    bool     reverse            = false;
    bool     keys_only          = false;
    bool     include_tombstones = false;
    bool     has_start_bound    = false;
    bool     start_inclusive    = false;
    bool     strict_byte_budget = false;
};

// Stable owner across immutable publication/source generations. The public
// facade retains it until accepted operations and all backend aliases drain.
class PublishedSnapshotContext;

class TreeCore : public std::enable_shared_from_this<TreeCore>
{
    struct State;

  public:
    static Status create(Config config, std::shared_ptr<TreeRuntimeBinding> binding, TreeCoreBootstrap bootstrap,
                         std::shared_ptr<TreeCore> *out);
    static Status open(const Config &config, std::shared_ptr<TreeCore> *out);
    static Status migrate_chunk(const Config &config, uint64_t expected_generation, std::shared_ptr<TreeCore> *out);
    ~TreeCore();
    TreeCore(const TreeCore &)            = delete;
    TreeCore &operator=(const TreeCore &) = delete;

    // Prepare owns only input bytes and credit; arm fences its generation before
    // proposal.
    Status prepare_chunk_collection(std::shared_ptr<ChunkCollectionOperation> *out);
    Status prepare_apply(size_t count, const detail::InlineFunction<CoreWriteInput(size_t)> &read,
                         detail::BudgetClass category, size_t retained_metadata_bytes,
                         std::shared_ptr<TreeApplyOperation> *out);
    // The caller owns immutable input storage until terminal. Its owner is
    // retained while the bounded replay window awaits its complete grant.
    using ReplayOwnerFactory =
        detail::InlineFunction<std::shared_ptr<const void>(const std::shared_ptr<detail::AllocationReservation> &)>;
    Status prepare_replay_adopt(uint64_t slot, size_t count, detail::InlineFunction<CoreWriteInput(size_t)> read,
                                ReplayOwnerFactory retain, size_t retained_metadata_bytes,
                                std::shared_ptr<TreeApplyOperation> *out);
    Status prepare_replay(uint64_t slot, size_t count, detail::InlineFunction<CoreWriteInput(size_t)> read,
                          std::shared_ptr<const void> caller_owner, size_t retained_metadata_bytes,
                          std::shared_ptr<TreeApplyOperation> *out);
    Status apply(uint64_t slot, const Batch &batch);
    Status apply_auto(const Batch &batch);
    Status apply_encoded(uint64_t slot, std::vector<Crowdbtree::encoded_op> operations);
    Status apply_external(uint64_t slot, std::vector<Crowdbtree::external_op> operations);
    Status apply_external_inputs(uint64_t slot, size_t count,
                                 const detail::InlineFunction<CoreWriteInput(size_t)> &read);
    Status complete_noop(uint64_t slot);
    void   force_advance_slot(uint64_t slot) noexcept;
    void   request_gc_floor(uint64_t floor) noexcept;
    [[nodiscard]] uint64_t requested_gc_floor() const noexcept;
    Status                 clear();
    Status                 install_split_overlay(TreeCore &source, uint64_t journal_frontier);
    Status                 clear_split_overlay(TreeCore &source);
    Status                 begin_split_view(uint64_t *generation, uint64_t *journal_frontier);
    Status                 release_split_view(uint64_t generation);
    Status                 publish_split_view(uint64_t generation, uint64_t journal_frontier, TreeCore &destination,
                                              const KeyRange &range);
    Status                 install_portable(std::vector<leaf_entry> entries, uint64_t coverage);
    Status                 prepare_import_credit(std::shared_ptr<detail::AllocationReservation> *out);
    Status                 prepare_import_credit(size_t requested, std::shared_ptr<detail::AllocationReservation> *out);
    Status                 install_readable(std::shared_ptr<const ReadablePageCatalog>            catalog,
                                            const std::shared_ptr<detail::AllocationReservation> &credit);
    Status                 install_native(std::vector<NativeFrame> frames, NativeStreamInfo info);
#ifdef CROWDB_TREE_TEST_UTIL
    void set_before_generation_install_for_tests(std::function<void()> callback);
#endif

    void   get_async(Slice key, std::function<void(Status, GetView)> complete);
    void   scan_async(CoreScanOptions options, std::function<void(Status, ScanPackedBuf, bool)> complete);
    Status get(Slice key, GetView *out);
    Status scan(CoreScanOptions options, std::vector<scan_entry> *out, bool *truncated, ScanPackedBuf *packed = nullptr,
                size_t *count = nullptr);

    Status prepare_flush(std::shared_ptr<FlushOperation> *out);
    Status prepare_flush(std::shared_ptr<FlushOperation> *out, std::shared_ptr<TreeOperationLifetime::Ticket> accepted);
    void   flush_async(std::function<void(Status)> complete);
    Status flush();
    Status prepare_checkpoint(const CheckpointOptions &options, std::shared_ptr<CheckpointOperation> *out);
    Status prepare_checkpoint(const CheckpointOptions &options, std::shared_ptr<CheckpointOperation> *out,
                              std::shared_ptr<TreeOperationLifetime::Ticket> accepted);
    // Runs on the shared runtime; staged completion never advances WAL coverage.
    Status prepare_writeback(std::shared_ptr<CheckpointOperation> *out);
    Status capture_snapshot(std::shared_ptr<PublishedSnapshotContext> *out);
    Status acquire_snapshot_window(const std::shared_ptr<detail::AllocationReservation> &metadata,
                                   std::shared_ptr<TreeOperationLifetime::Ticket> *ticket, AsyncPageStore **backend);
    Status reserve_snapshot_ids(const PublishedTreeState &expected, uint64_t count,
                                const std::shared_ptr<detail::AllocationReservation> &metadata);
    Status prepare_compaction(const LocalCompactionOptions &options, std::shared_ptr<LocalCompactionOperation> *out);
    Status compact_sparse_blocks(MergeGcStats *out);
    size_t evict_clean_pages(PageCacheKind kind, size_t max_resident);
    [[nodiscard]] uint64_t last_snapshot_pages_written() const noexcept;
    [[nodiscard]] uint64_t snapshot_pages_total() const noexcept;
    Status                 checkpoint(uint64_t *coverage = nullptr, uint64_t *sequence = nullptr);
    void                   checkpoint_async(std::function<void(Status, uint64_t)> complete);

    // This boundary is host-only, never called from an accepted callback.
    void                                                     close_and_drain() noexcept;
    [[nodiscard]] bool                                       closed() const noexcept;
    [[nodiscard]] const Config                              &config() const noexcept;
    [[nodiscard]] const std::shared_ptr<TreeRuntimeBinding> &binding() const noexcept;
    [[nodiscard]] const std::shared_ptr<SharedPageCache>    &cache() const noexcept;
    [[nodiscard]] const std::shared_ptr<LocalBackendState>  &local_backend() const noexcept;
    [[nodiscard]] const std::shared_ptr<CheckpointBackend>  &chunk_backend() const noexcept;
    [[nodiscard]] uint64_t                                   durable_coverage() const noexcept;
    [[nodiscard]] uint64_t                                   snapshot_sequence() const noexcept;
    [[nodiscard]] AsyncPageStore                            *async_backend() const noexcept;
    [[nodiscard]] PageLoadLimits                             load_limits() const noexcept;
    [[nodiscard]] PublishedReadLimits                        read_limits() const noexcept;
    [[nodiscard]] std::shared_ptr<const PublishedTreeState>  capture() const;
    [[nodiscard]] PublicationCoordinator                    &publisher() const noexcept;
    [[nodiscard]] TreeOperationLifetime                     &operation_lifetime() const noexcept;
    [[nodiscard]] TreeStatsSnapshot                          state_stats() const;
    [[nodiscard]] BackingStatsSnapshot                       backing_stats() const noexcept;
    [[nodiscard]] BackingStatsSampler                        backing_sampler() const noexcept;
    [[nodiscard]] uint64_t                                   contiguous_slot() const noexcept;
    [[nodiscard]] uint64_t                                   max_seen_slot() const noexcept;
    [[nodiscard]] size_t                                     memtable_count() const;
    [[nodiscard]] SourceMemoryStats                          source_stats() const;
    [[nodiscard]] std::array<ResourceUsage, 7>               admission_resource_usage() const noexcept;
    [[nodiscard]] size_t                                     frozen_table_count() const;
    Status                                                   enable_work_histogram(uint32_t stage);

  private:
    void get_impl(Slice key, std::function<void(Status, GetView)> complete, bool dispatch);
    void scan_impl(CoreScanOptions options, std::function<void(Status, ScanPackedBuf, bool)> complete, bool dispatch);
    friend class detail::ChunkRuntimePageStore;
    static Status open_chunk(const Config &config, std::shared_ptr<TreeCore> *out);
    static Status open_chunk_migration(const Config                                        &config,
                                       const std::shared_ptr<detail::LegacyChunkReadStore> &legacy,
                                       uint64_t snapshot_sequence, std::shared_ptr<TreeCore> *out);
    friend class TreeApplyOperation;
    friend class TreeGenerationLease;
    friend class PreparedSplitOverlay;
    // Candidate already owns a fully validated immutable graph. Null creates
    // a new empty generation. All failures precede old-source closure.
    Status install_generation(std::shared_ptr<const PublishedTreeState> candidate,
                              const SplitGeneration                    *split = nullptr);
    Status prepare_readable(std::shared_ptr<const ReadablePageCatalog>            catalog,
                            const std::shared_ptr<detail::AllocationReservation> &credit,
                            std::shared_ptr<const PublishedTreeState>            *out);
    Status capture_snapshot(std::shared_ptr<const PublishedTreeState>  view,
                            std::shared_ptr<PublishedSnapshotContext> *out);
    Status install_split_baseline(const SplitSourceSession &session, uint64_t identity, uint64_t end);
    // Caller holds both split gates and the source ingress.
    Status install_split_overlay_guarded(TreeCore &source, uint64_t journal_frontier);
    explicit TreeCore(detail::ReservedUnique<State> state);
    detail::ReservedUnique<State> state_;
};
} // namespace crowdb::tree
