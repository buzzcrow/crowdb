// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/btree/diagnostics.h"
#include "crowdb-tree/status.h"
#include "flush_dispatch_metrics.h"
#include "pending_page_times.h"
#include "tree_wait_metrics.h"
#include "tree_work_counters.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <set>
#include <vector>

namespace crowdb::tree
{
namespace detail
{
class RetirementQueue;
struct RetirementObservation;
class AllocationReservation;
} // namespace detail
class AllocationBudget;

// Each live PID contributes once, regardless of how many old readers retain
// earlier page representations. Candidate membership is distinct from jobs.
struct PageMetricsContribution
{
    bool     leaf                     = true;
    bool     dirty                    = false;
    uint64_t overlay_runs             = 0;
    uint64_t overlay_entries          = 0;
    bool     page_size_known          = false;
    uint64_t page_size_bytes          = 0;
    uint64_t page_size_estimate_bytes = 0;
    bool     kv_known                 = false;
    uint64_t kv_count                 = 0;
    uint64_t kv_bytes                 = 0;
    bool     consolidate_candidate    = false;
    bool     split_candidate          = false;
    bool     split_ready              = false;
    bool     merge_candidate          = false;
    bool     writeback_candidate      = false;
    bool     evict_candidate          = false;

    bool operator==(const PageMetricsContribution &) const noexcept = default;
};

// Coordinator-only: apply to an unpublished copy, then publish with mapping.
// A null contribution denotes an absent page. Invalid deltas leave stats
// intact.
[[nodiscard]] bool apply_page_metrics(TreeLogicalStats &stats, const PageMetricsContribution *old_page,
                                      const PageMetricsContribution *new_page) noexcept;
[[nodiscard]] bool apply_merge_pair_metrics(TreeLogicalStats &stats, uint64_t removed_pairs, uint64_t removed_ready,
                                            uint64_t added_pairs, uint64_t added_ready) noexcept;

// Compare only additive per-page fields; frontier, pending membership and
// historical maxima require their own ledgers.
[[nodiscard]] bool equal_page_metrics(const TreeLogicalStats &left, const TreeLogicalStats &right) noexcept;

// Coordinator-owned membership, not a collector cache. One checkpoint may be
// in flight. Changes after capture survive its commit; representation-only
// changes must not call change_page(). Allocation belongs to admitted metadata.
class CheckpointPendingMetrics
{
    struct CaptureRetirement;

  public:
    // Prepare an empty set before publication. commit() swaps the old set
    // into the returned retirement owner; the publisher reclaims it off-path.
    class PreparedCapture
    {
      public:
        explicit PreparedCapture(CheckpointPendingMetrics                      &owner,
                                 std::shared_ptr<detail::AllocationReservation> metadata       = {},
                                 std::shared_ptr<const void>                    resource_owner = {});
        [[nodiscard]] std::shared_ptr<const void> commit(uint64_t token) noexcept;

      private:
        CheckpointPendingMetrics          &owner_;
        std::shared_ptr<CaptureRetirement> retired_;
    };

    // Only one prepared delta exists at a time under the publication gate.
    // Each PID occurs at most once. Insertions are rolled back on destruction;
    // removals and mapping flags change only at the no-throw commit boundary.
    class PreparedDelta
    {
      public:
        PreparedDelta(CheckpointPendingMetrics &owner, size_t max_pages);
        ~PreparedDelta();
        PreparedDelta(const PreparedDelta &)            = delete;
        PreparedDelta &operator=(const PreparedDelta &) = delete;
        void           change_page(uint64_t pid);
        void           remove_page(uint64_t pid);
        void           change_mapping() noexcept;
        void           copy_to(TreeLogicalStats &stats) const noexcept;
        void           commit() noexcept;

      private:
        struct Edit
        {
            uint64_t pid;
            bool     inserted_pending = false;
            bool     inserted_since   = false;
            bool     removed          = false;
        };

        CheckpointPendingMetrics &owner_;
        std::pmr::vector<Edit>    edits_;
        uint64_t                  changed_at_ns_   = 0;
        bool                      mapping_changed_ = false;
        bool                      committed_       = false;
    };

    // A caller-provided resource must outlive this ledger and can be backed by
    // an admitted metadata arena. The ledger is never copied on publication.
    explicit CheckpointPendingMetrics(std::pmr::memory_resource *resource = std::pmr::get_default_resource());
    CheckpointPendingMetrics(const CheckpointPendingMetrics &)            = delete;
    CheckpointPendingMetrics &operator=(const CheckpointPendingMetrics &) = delete;
    void                      change_page(uint64_t pid);
    void                      remove_page(uint64_t pid);
    void                      change_mapping() noexcept;
    // Standalone convenience; publication uses PreparedCapture to defer frees.
    [[nodiscard]] bool     capture(uint64_t token);
    [[nodiscard]] bool     finish(uint64_t token, bool committed) noexcept;
    [[nodiscard]] bool     can_capture(uint64_t token) const noexcept;
    [[nodiscard]] bool     can_finish(uint64_t token) const noexcept;
    void                   copy_to(TreeLogicalStats &stats) const noexcept;
    [[nodiscard]] bool     contains(uint64_t pid) const;
    bool                   retire_step() noexcept;
    [[nodiscard]] uint64_t oldest_since_ns() const noexcept;

    [[nodiscard]] bool inflight() const noexcept
    {
        return inflight_;
    }

  private:
    // Node allocation is bounded per changed PID. A small flush must never
    // reserve a tree-sized hash reallocation merely to mark one more page.
    PendingPageTimes pending_;
    PendingPageTimes since_capture_;
    uint64_t         mapping_since_ns_         = 0;
    uint64_t         mapping_capture_since_ns_ = 0;
    uint64_t         token_                    = 0;
    bool             inflight_                 = false;
    bool             mapping_pending_          = false;
    bool             mapping_since_capture_    = false;
};

enum class TreeResourceMetric : uint8_t {
    AllocatedBytes,
    ReservedBytes,
    PayloadBytes,
    FrameBytes,
    MetadataBytes,
    IoBytes,
    ScratchBytes,
    RetiredBytes,
    ResidentFrames,
    CacheBytes,
    CachePinnedBytes,
    ProxyPages,
    ReadInflightBytes,
    WriteInflightBytes,
    MaterializeInflightBytes,
    AllocatedBackingBytes,
    MaintenanceQueued,
    MaintenanceInflight,
    Count
};

class BackingMetricExtent;

class TreeResourceMetrics
{
  public:
    // The allocation/extent owner reports actual acquisition and final release.
    // Reference transfers only change the overlapping referenced gauges.
    [[nodiscard]] bool              add(TreeResourceMetric metric, uint64_t amount) noexcept;
    [[nodiscard]] bool              remove(TreeResourceMetric metric, uint64_t amount) noexcept;
    [[nodiscard]] TreeResourceStats snapshot() const noexcept;

  private:
    friend class BackingMetricExtent;
    std::array<std::atomic<uint64_t>, static_cast<size_t>(TreeResourceMetric::Count)> values_{};
};

enum class TreeStage : uint8_t {
    FlushBatch,
    FlushAdmissionWait,
    FlushCapture,
    FlushWriterDrain,
    FlushHandoff,
    FlushRange,
    PublishQueueWait,
    PublishCommit,
    MaintenancePlan,
    OverlayCoalesce,
    PageSplit,
    PageMerge,
    PageLayout,
    PageEncode,
    BudgetWait,
    PageLoadPipeline,
    PageWritebackPipeline,
    SnapshotCapture,
    SnapshotCommit,
    MemoryRetireWait,
    FlushRepair,
    Count
};

enum class TreeOutcome : uint8_t { Success, Error, Cancelled, Retry, Stale, Count };

inline constexpr size_t kTreeStageCount   = static_cast<size_t>(TreeStage::Count);
inline constexpr size_t kTreeOutcomeCount = static_cast<size_t>(TreeOutcome::Count);

struct TreeStageTotals
{
    uint64_t                                attempts = 0;
    uint64_t                                inflight = 0;
    uint64_t                                count    = 0;
    uint64_t                                sum_ns   = 0;
    uint64_t                                max_ns   = 0;
    std::array<uint64_t, kTreeOutcomeCount> outcomes{};
};

struct TreeStageWindow
{
    uint64_t count       = 0;
    uint64_t sum_ns      = 0;
    uint64_t max_ns      = 0;
    uint64_t total_count = 0;
};

using TreeWorkTotals = std::array<TreeStageTotals, kTreeStageCount>;
using TreeWorkWindow = std::array<TreeStageWindow, kTreeStageCount>;

// Fixed diagnostic storage never participates in operation admission. When
// the slots are full, the sample explicitly reports incomplete age coverage.
inline constexpr size_t kTreeWorkAgeCapacity = 256;

struct TreeStageAge
{
    uint64_t inflight      = 0;
    uint64_t tracked       = 0;
    uint64_t untracked     = 0;
    uint64_t oldest_age_ns = 0;
    bool     complete      = false;
};

using TreeWorkAges = std::array<TreeStageAge, kTreeStageCount>;

class TreeMetricsCollector;
class TreeMetricsOperation;
struct TreeStageHistogram;
struct TreeWorkHistograms;

class TreeWorkMetrics : public std::enable_shared_from_this<TreeWorkMetrics>
{
  public:
    ~TreeWorkMetrics();
    // Enable is additive and samples terminal events from publication onward.
    // The final metrics owner frees each paid instance, including after Tree
    // close.
    Status                           enable_histogram(TreeStage stage, const std::shared_ptr<AllocationBudget> &budget);
    [[nodiscard]] static size_t      histogram_allocation_bytes() noexcept;
    [[nodiscard]] bool               histogram_enabled(TreeStage stage) const noexcept;
    [[nodiscard]] TreeWorkHistograms histogram_totals() const noexcept;
    FlushDispatchMetrics             dispatch;
    [[nodiscard]] TreeWorkTotals     totals() const noexcept;
    [[nodiscard]] TreeWorkAges       ages(uint64_t now_ns) const noexcept;
    // Exactly one draining owner. It distributes copies of the returned window
    // to consumers; ordinary totals() readers never reset any observation.
    [[nodiscard]] std::shared_ptr<TreeMetricsCollector>
    claim_collector(const std::shared_ptr<AllocationBudget> &budget = {});

  private:
    friend class TreeMetricsCollector;
    friend class TreeMetricsOperation;
    friend class detail::RetirementQueue;
    friend struct detail::RetirementObservation;

    struct Stage
    {
        std::atomic<uint64_t>                                attempts{0}, inflight{0}, count{0}, sum_ns{0}, max_ns{0};
        std::atomic<uint64_t>                                window_count{0}, window_sum_ns{0}, window_max_ns{0};
        std::array<std::atomic<uint64_t>, kTreeOutcomeCount> outcomes{};
        std::atomic<uint64_t>                                untracked_ages{0};
    };

    size_t                       begin(TreeStage stage, uint64_t started_at_ns) noexcept;
    size_t                       track_age(TreeStage stage, uint64_t started_at_ns) noexcept;
    void                         finish_age(TreeStage stage, size_t slot) noexcept;
    void                         complete(TreeStage stage, TreeOutcome outcome, uint64_t ns, size_t age_slot) noexcept;
    [[nodiscard]] TreeWorkWindow drain_window() noexcept;
    std::array<Stage, kTreeStageCount>                      stages_{};
    std::atomic<bool>                                       collector_claimed_{false};
    std::array<std::atomic<uint64_t>, kTreeWorkAgeCapacity> active_ages_{};
    std::atomic<size_t>                                     next_age_slot_{0};

  public:
    TreeWorkCounters counters;
    TreeWaitMetrics  waits;

  private:
    void                             observe_histogram(TreeStage stage, uint64_t ns) noexcept;
    [[nodiscard]] TreeWorkHistograms drain_histograms() noexcept;
    // Published once, never replaced. Every observing operation retains this
    // metrics owner, so no hazard allocation or shared_ptr atomic is required.
    std::array<std::atomic<TreeStageHistogram *>, kTreeStageCount> histograms_{};
};

// Holds the metrics owner across asynchronous completion and records only the
// first terminal notification. Destruction of unfinished work is cancellation.
class TreeMetricsOperation
{
  public:
    TreeMetricsOperation(std::shared_ptr<TreeWorkMetrics> metrics, TreeStage stage, uint64_t started_at_ns) noexcept;
    ~TreeMetricsOperation();
    TreeMetricsOperation(const TreeMetricsOperation &)            = delete;
    TreeMetricsOperation &operator=(const TreeMetricsOperation &) = delete;
    [[nodiscard]] bool    finish(TreeOutcome outcome, uint64_t ended_at_ns) noexcept;

  private:
    std::shared_ptr<TreeWorkMetrics> metrics_;
    TreeStage                        stage_;
    uint64_t                         started_at_ns_;
    size_t                           age_slot_ = kTreeWorkAgeCapacity;
    std::atomic<bool>                finished_{false};
};

class TreeMetricsCollector
{
  public:
    ~TreeMetricsCollector();
    TreeMetricsCollector(const TreeMetricsCollector &)                       = delete;
    TreeMetricsCollector            &operator=(const TreeMetricsCollector &) = delete;
    [[nodiscard]] TreeWorkWindow     capture_window() noexcept;
    [[nodiscard]] TreeWorkHistograms capture_histograms() noexcept;

  private:
    friend class TreeWorkMetrics;
    explicit TreeMetricsCollector(std::shared_ptr<TreeWorkMetrics> metrics);
    std::shared_ptr<TreeWorkMetrics> metrics_;
};

[[nodiscard]] uint64_t tree_metrics_now_ns() noexcept;

} // namespace crowdb::tree
