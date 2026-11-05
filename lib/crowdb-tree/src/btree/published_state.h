// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"
#include "budget_memory_resource.h"
#include "crowdb-tree/epoch.h"
#include "crowdb-tree/memtable/memtable.h"
#include "crowdb-tree/status.h"
#include "inline_function.h"
#include "merge_pair_metrics.h"
#include "owned_key.h"
#include "snapshot_changes.h"
#include "tree_metrics.h"

#include <array>
#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace crowdb::tree
{
struct PageState;
struct BackingRef;
struct PrimaryBackingMetadata;
struct SnapshotBaseline;
class SharedPageCache;
class TreeStateReconciliation;

struct InnerFrameProof
{
    uint64_t self_page_id   = UINT64_MAX;
    uint32_t frame_bytes    = 0;
    bool     has_fences     = false;
    bool     page_id_fences = false;
    OwnedKey lower;
    OwnedKey upper;
    uint64_t lower_leaf = UINT64_MAX;
    uint64_t upper_leaf = UINT64_MAX;
};

struct InnerPageState
{

    bool retire_step() noexcept
    {
        for (size_t i = 0; i < 64 && !separators.empty(); ++i) {
            separators.pop_back();
        }
        return separators.empty();
    }

    using KeyAllocator   = detail::ReservationAllocator<OwnedKey>;
    using ChildAllocator = detail::ReservationAllocator<uint64_t>;
    using Keys           = std::vector<OwnedKey, KeyAllocator>;
    using Children       = std::vector<uint64_t, ChildAllocator>;

    // Element bytes are counted separately; copies/inserts can overlap two
    // capacities for each owned array.
    [[nodiscard]] static size_t allocation_bound() noexcept
    {
        return detail::shared_allocation_bound<InnerPageState>();
    }

    explicit InnerPageState(const std::shared_ptr<detail::AllocationReservation> &metadata = {})
        : separators(KeyAllocator(metadata)),
          children(ChildAllocator(metadata))
    {
    }

    InnerPageState(const InnerPageState &source, const std::shared_ptr<detail::AllocationReservation> &metadata = {})
        : separators(source.separators.begin(), source.separators.end(), KeyAllocator(metadata)),
          children(source.children.begin(), source.children.end(), ChildAllocator(metadata))
    {
    }

    // separators[i] is the inclusive lower bound of children[i + 1].
    Keys     separators;
    Children children;
    // Only a decoded immutable frame carries this proof. Copying routing
    // descriptors for a new publication deliberately drops its old fences.
    std::optional<InnerFrameProof> frame_proof = std::nullopt;
};

struct PublishedPage
{
    bool retire_step() noexcept
    {
        return true;
    }

    uint64_t                              revision = 0;
    uint32_t                              level    = 0;
    std::shared_ptr<const PageState>      leaf     = nullptr;
    std::shared_ptr<const InnerPageState> inner    = nullptr;
    PageMetricsContribution               metrics;
    // Keeps the admitted metadata charge until the last page/view releases it.
    std::shared_ptr<const void> allocation_owner = nullptr;
    // A cold index retains a protected version; loading it does not mutate
    // the fixed mapping or replace a newer route.
    std::shared_ptr<const BackingRef>             inner_backing = nullptr;
    std::shared_ptr<const PrimaryBackingMetadata> primary       = nullptr;
    // Immutable classification lets workers skip unchanged pairs without
    // accessing the publication owner's private ledger.
    std::optional<MergePairProof> merge_outgoing = std::nullopt;
};

struct PageReplacement
{
    uint64_t                             page_id     = 0;
    std::shared_ptr<const PublishedPage> expected    = nullptr;
    std::shared_ptr<const PublishedPage> replacement = nullptr;
};

// Persistent radix directory: a transaction copies each touched path/segment
// once. Payloads and untouched directory branches are shared with old readers.
class MappingView
{
    struct Segment;
    struct Directory;

  public:
    static constexpr uint64_t kFanout    = 64;
    static constexpr uint64_t kPageLimit = uint64_t{1} << 30;

    struct CopyStats
    {
        uint64_t directories = 0;
        uint64_t segments    = 0;
        uint64_t bytes       = 0;
    };

    struct CopyBounds
    {
        size_t   leaf_pages                = 0;
        size_t   merge_candidate_pages     = 0;
        size_t   leaf_bytes                = 0;
        size_t   max_leaf_bytes            = 0;
        size_t   max_leaf_append_bytes     = 0;
        size_t   max_leaf_bases            = 0;
        bool     may_have_overflow_values  = false;
        size_t   max_inner_bytes           = 0;
        uint32_t max_level                 = 0;
        uint64_t candidate_since_ns        = 0;
        uint64_t writeback_since_ns        = 0;
        uint64_t oldest_writeback_pid      = kPageLimit;
        uint64_t writeback_page_since_ns   = 0;
        uint64_t oldest_writeback_page_pid = kPageLimit;
    };

    // Maintained on the same copied radix paths, without scanning cold pages.
    // Includes descriptor arrays and temporary overlay growth, not payloads.
    [[nodiscard]] CopyBounds    copy_bounds() const noexcept;
    [[nodiscard]] static size_t path_copy_bound() noexcept;

    // Incremental minimum over dirty leaf candidates; no traversal or I/O.
    [[nodiscard]] std::optional<uint64_t>              oldest_writeback_leaf() const noexcept;
    [[nodiscard]] std::optional<uint64_t>              oldest_writeback_page() const noexcept;
    [[nodiscard]] std::shared_ptr<const PublishedPage> get(uint64_t page_id) const;
    // Startup/snapshot traversal visits allocated radix nodes only. Runtime
    // updates and collectors use incremental summaries instead.
    using Visitor = void (*)(void *, uint64_t, const std::shared_ptr<const PublishedPage> &);
    void visit(void *context, Visitor visitor) const;
    // Explicit diagnostic traversal. Each unit inspects one radix slot and
    // at most five directory levels; missing subtrees are skipped as a unit.
    // Resume with the same immutable mapping and returned PID. Returns true
    // at the end, including empty mappings. Does not load cold pages.
    [[nodiscard]] bool visit_from(uint64_t &next_pid, size_t max_slots, void *context, Visitor visitor) const;

    [[nodiscard]] bool empty() const noexcept
    {
        return !root_;
    }

    [[nodiscard]] bool same_root(const MappingView &other) const noexcept
    {
        return root_ == other.root_;
    }

    // Input must be strictly increasing by PID and within kPageLimit.
    [[nodiscard]] MappingView replace(std::span<const PageReplacement> replacements, CopyStats &copied,
                                      const std::shared_ptr<detail::AllocationReservation> &metadata = {}) const;

  private:
    struct Segment
    {

        bool retire_step() noexcept
        {
            return true;
        }

        std::array<std::shared_ptr<const PublishedPage>, kFanout> pages;
        std::array<uint64_t, kFanout>                             candidate_since_ns{};
        std::array<uint64_t, kFanout>                             writeback_since_ns{};
        CopyBounds                                                bounds;
    };

    struct Directory
    {

        bool retire_step() noexcept
        {
            return true;
        }

        std::array<std::shared_ptr<const Directory>, kFanout> children;
        std::array<std::shared_ptr<const Segment>, kFanout>   segments;
        CopyBounds                                            bounds;
    };

    static void visit_directory(const Directory &directory, unsigned shift, uint64_t prefix, void *context,
                                Visitor visitor);
    static std::shared_ptr<const Directory>
    replace_directory(const std::shared_ptr<const Directory> &old, unsigned shift,
                      std::span<const PageReplacement> replacements, CopyStats &copied,
                      const std::shared_ptr<detail::AllocationReservation> &metadata, uint64_t now_ns);
    std::shared_ptr<const Directory> root_ = nullptr;
};

struct PublishedSources
{

    bool retire_step() noexcept
    {
        for (size_t i = 0; i < 64 && !tables.empty(); ++i) {
            tables.pop_back();
        }
        return tables.empty();
    }

    struct Inheritance
    {
        uint64_t          owner_identity = 0;
        uint64_t          session        = 0;
        uint64_t          frontier       = 0;
        size_t            local_count    = 0;
        SourceMemoryStats memory{};
        bool              operator==(const Inheritance &) const = default;
    };

    using TableAllocator = detail::ReservationAllocator<std::shared_ptr<MemTable>>;
    using Tables         = std::vector<std::shared_ptr<MemTable>, TableAllocator>;

    [[nodiscard]] static size_t allocation_bound() noexcept
    {
        return detail::shared_allocation_bound<PublishedSources>();
    }

    explicit PublishedSources(std::shared_ptr<detail::AllocationReservation> metadata = {})
        : tables(TableAllocator(std::move(metadata)))
    {
    }

    std::shared_ptr<const SourceMemoryLedger> memory = nullptr;
    Tables                                    tables;
    uint64_t                                  floor = 0;
    // The suffix consists only of independently retained, closed parent
    // tables. Parent and child slots above the cutover are separate namespaces.
    std::optional<Inheritance> inheritance = std::nullopt;

    [[nodiscard]] SourceMemoryStats source_stats() const noexcept
    {
        auto result = memory ? memory->snapshot() : SourceMemoryStats{};
        if (inheritance) {
            result += inheritance->memory;
        }
        return result;
    }

    [[nodiscard]] uint64_t frontier(size_t index) const noexcept
    {
        return inheritance && index >= inheritance->local_count ? inheritance->frontier : UINT64_MAX;
    }
};

struct PendingHandoff
{
    uint64_t batch                                    = 0;
    uint64_t frontier                                 = 0;
    bool     operator==(const PendingHandoff &) const = default;
};

struct PublishedTreeState
{

    void                       clear_merge_candidates(const std::shared_ptr<detail::AllocationReservation> &metadata);
    detail::RetirementObserver retirement;
    static constexpr bool      observe_retirement = true;

    bool retire_step() noexcept
    {
        return true;
    }

    explicit PublishedTreeState(std::shared_ptr<detail::AllocationReservation> metadata     = {},
                                uint64_t                                       source_floor = 0);

    MappingView                             mapping;
    SnapshotChanges                         snapshot_changes;
    SnapshotChanges                         snapshot_changes_since_capture;
    std::shared_ptr<const SnapshotBaseline> snapshot_baseline;
    std::shared_ptr<const PublishedSources> sources      = nullptr;
    uint64_t                                root_page_id = UINT64_MAX;
    uint64_t                                next_page_id = 0;
    uint64_t                                generation   = 1;
    uint64_t                                commit_seq   = 0;
    // Per-generation L1 layout and handoff ownership fence. Publishing only
    // a successor L0 catalog leaves a checkpoint's layout proof valid.
    uint64_t l1_seq                      = 0;
    uint64_t coverage                    = 0;
    uint64_t gc_floor                    = 0;
    uint64_t checkpoint_pending_since_ns = 0;
    uint64_t merge_candidate_since_ns    = 0;
    uint64_t checkpoint_token            = 0;
    uint64_t checkpoint_capture_coverage = 0;
    uint64_t checkpoint_capture_floor    = 0;
    bool     checkpoint_inflight         = false;
    // L1 contains part of this batch above coverage. Checkpoint capture and
    // unrelated structure changes wait until its coverage commit succeeds.
    std::optional<PendingHandoff> pending_handoff = std::nullopt;
    // Held before source closure while the batch owns its reserved bounds.
    // Readers continue using the immutable state; unrelated writers wait.
    uint64_t         flush_admission_owner = 0;
    TreeLogicalStats stats;
    // Exact encoded graph summary, installed only after a bounded validation pass.
    std::optional<TreeSummary> summary = std::nullopt;
};

struct InstallToken
{
    uint64_t generation                             = 1;
    uint64_t batch                                  = 0;
    uint64_t range                                  = 0;
    uint64_t sequence                               = 0;
    bool     operator==(const InstallToken &) const = default;
};

// The batch creates one record per token before dispatch and reuses it on
// every retry. It retains the record until all children and late callbacks end.
struct InstallRecord
{
    explicit InstallRecord(InstallToken value) : token(value)
    {
    }

    const InstallToken token;
    std::atomic<bool>  committed{false};
    uint64_t           commit_seq = 0;
    uint64_t           l1_seq     = 0;
    // Only capture records retain this artifact. Its sources are empty so
    // checkpoint lifetime does not retain a detached L0 catalog.
    std::shared_ptr<const PublishedTreeState> checkpoint_view = nullptr;

    [[nodiscard]] uint64_t publisher_id() const noexcept
    {
        return publisher_id_.load(std::memory_order_acquire);
    }

  private:
    friend class PublicationCoordinator;
    std::atomic<uint64_t> publisher_id_{0};
};

enum class CheckpointAction : uint8_t { Capture, Commit, Abort };

struct CheckpointPublication
{
    CheckpointAction                        action   = CheckpointAction::Capture;
    uint64_t                                token    = 0;
    std::shared_ptr<const SnapshotBaseline> baseline = nullptr;
};

enum class FlushAdmissionAction : uint8_t { Acquire, Release };

struct FlushAdmissionPublication
{
    FlushAdmissionAction action = FlushAdmissionAction::Acquire;
    uint64_t             owner  = 0;
};

struct MergePairPublication
{
    uint64_t                             left            = 0;
    std::shared_ptr<const PublishedPage> expected_left   = nullptr;
    std::shared_ptr<const PublishedPage> expected_right  = nullptr;
    std::shared_ptr<const PublishedPage> expected_parent = nullptr;
    std::optional<MergePairProof>        candidate       = std::nullopt;
};

struct PublicationTransaction
{
    using Pages = std::vector<PageReplacement, detail::ReservationAllocator<PageReplacement>>;
    using Records =
        std::vector<std::shared_ptr<InstallRecord>, detail::ReservationAllocator<std::shared_ptr<InstallRecord>>>;
    std::shared_ptr<InstallRecord>          record = nullptr;
    Pages                                   pages{detail::ReservationAllocator<PageReplacement>{}};
    std::optional<uint64_t>                 root_page_id          = std::nullopt;
    std::optional<uint64_t>                 expected_root_page_id = std::nullopt;
    std::optional<uint64_t>                 expected_coverage     = std::nullopt;
    std::optional<uint64_t>                 coverage              = std::nullopt;
    std::shared_ptr<const PublishedSources> expected_sources      = nullptr;
    std::shared_ptr<const PublishedSources> sources               = nullptr;
    // Coverage publication waits for every range, including empty ranges.
    Records required_installs{detail::ReservationAllocator<std::shared_ptr<InstallRecord>>{}};
    std::shared_ptr<detail::AllocationReservation> metadata = nullptr;
    // Persistent maintenance selection uses independent prepaid Routing credit.
    std::shared_ptr<detail::AllocationReservation> snapshot_metadata = nullptr;
    SnapshotChangeEffect                           snapshot_effect   = SnapshotChangeEffect::FullContent;
    // Capture names an exact frontier or an asynchronous minimum frontier.
    // Commit is submitted only after durable checkpoint commit succeeds.
    // Checkpoint actions cannot be combined with page/catalog mutations.
    std::optional<CheckpointPublication> checkpoint = std::nullopt;
    // Range installs set this atomically with their pages. A coverage
    // transaction with the same identity clears it after all required installs.
    std::optional<PendingHandoff>            handoff               = std::nullopt;
    std::optional<uint64_t>                  expected_commit_seq   = std::nullopt;
    std::optional<uint64_t>                  expected_l1_seq       = std::nullopt;
    std::optional<FlushAdmissionPublication> flush_admission       = std::nullopt;
    uint64_t                                 flush_admission_owner = 0;
    // Advances the shared PID authority without creating logical mapping
    // entries. Capture may reserve an overflow range in the same transaction.
    std::optional<uint64_t> expected_next_page_id = std::nullopt;
    std::optional<uint64_t> next_page_id          = std::nullopt;
    // Classification changes only the candidate ledger. The logical mapping
    // and checkpoint pending membership remain unchanged.
    std::optional<MergePairPublication> merge_pair = std::nullopt;
    // Standalone observation installation, fenced by expected_commit_seq.
    std::optional<TreeSummary> summary = std::nullopt;
};

struct PublicationResult
{
    Status                                    status;
    uint64_t                                  commit_seq        = 0;
    uint64_t                                  l1_seq            = 0;
    bool                                      already_committed = false;
    MappingView::CopyStats                    copied{};
    std::shared_ptr<const PublishedTreeState> checkpoint_view = nullptr;
};

class PublicationLane;

struct PublicationRequest
{
    explicit PublicationRequest(const std::shared_ptr<detail::AllocationReservation> &metadata = {})
        : transaction{.pages = PublicationTransaction::Pages(detail::ReservationAllocator<PageReplacement>(metadata)),
                      .required_installs = PublicationTransaction::Records(
                          detail::ReservationAllocator<std::shared_ptr<InstallRecord>>(metadata)),
                      .metadata = metadata}
    {
    }

    // Explicit bounded diagnostics use the same serial owner as publication.
    // The fixed view must still be current; diagnostics never publish a root.
    std::shared_ptr<TreeStateReconciliation> reconciliation       = nullptr;
    size_t                                   reconciliation_slots = 64;

    PublicationTransaction                          transaction;
    detail::InlineFunction<void(PublicationResult)> on_done;

  private:
    friend class PublicationCoordinator;
    std::atomic<bool>                   submitted_{false};
    PublicationRequest                 *next_            = nullptr;
    std::shared_ptr<PublicationRequest> queued_owner_    = nullptr;
    std::optional<TreeMetricsOperation> queue_metrics_   = std::nullopt;
    bool                                completion_slot_ = false;
    std::shared_ptr<PublicationLane>    lane_            = nullptr;
};

struct SourceCatalog;

struct PublicationBootstrap
{
    std::shared_ptr<detail::AllocationReservation> metadata       = nullptr;
    std::shared_ptr<const PublishedTreeState>      initial        = nullptr;
    std::shared_ptr<const SourceCatalog>           source_catalog = nullptr;
};

// Bounded MPSC ingress. The thread winning running_ drains short metadata
// commits; callers dispatch from the shared CPU runtime. Terminal callbacks run
// inline and must not block or wait for another publication. They may enqueue
// continuations; a callback exception cannot strand remaining requests.
class PublicationCoordinator
{
    struct CheckpointLedger;
    PublicationResult reconcile(const PublicationRequest &request);

  public:
    class Replacement;
    explicit PublicationCoordinator(size_t queue_capacity = 64, uint64_t generation = 1,
                                    std::shared_ptr<TreeWorkMetrics> metrics       = {},
                                    std::pmr::memory_resource *checkpoint_resource = std::pmr::get_default_resource(),
                                    std::shared_ptr<SharedPageCache> page_cache    = {},
                                    PublicationBootstrap             bootstrap     = {});
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t source_count = 0, size_t max_threads = 64,
                                                                const MappingView *mapping         = nullptr,
                                                                bool               track_residency = false) noexcept;
    ~PublicationCoordinator();
    PublicationCoordinator(const PublicationCoordinator &)            = delete;
    PublicationCoordinator &operator=(const PublicationCoordinator &) = delete;

    [[nodiscard]] std::shared_ptr<const PublishedTreeState> capture() const;
    // Capture the last complete L1 mapping and coverage during a partial
    // handoff. Snapshot consumers must ignore this header's live sources.
    [[nodiscard]] std::shared_ptr<const PublishedTreeState> capture_complete() const;
    [[nodiscard]] static size_t                             complete_view_reserve_bytes() noexcept;
    // Logical values come from one captured root. Resources are a separate,
    // approximate atomic sample; this path never loads or traverses pages.
    [[nodiscard]] TreeStatsSnapshot stats(const TreeResourceMetrics &resources) const;
    // Per-Tree bounded admission for independent maintenance factories. The
    // slots remain active across CPU turns and publication waits, even when
    // metrics collection is disabled. A full window defers unstarted work.
    static constexpr size_t kMaintenanceSlots = 64;
    [[nodiscard]] size_t    start_maintenance(uint64_t started_at_ns) noexcept;
    void                    finish_maintenance(size_t slot) noexcept;
    [[nodiscard]] uint64_t  oldest_maintenance_since_ns() const noexcept;

    [[nodiscard]] uint64_t maintenance_scan_pid() const noexcept
    {
        return maintenance_scan_pid_.load(std::memory_order_relaxed);
    }

    void set_maintenance_scan_pid(uint64_t pid) noexcept
    {
        maintenance_scan_pid_.store(pid, std::memory_order_relaxed);
    }

    [[nodiscard]] bool tracks_cache_residency() const noexcept
    {
        return page_cache_ != nullptr;
    }

    [[nodiscard]] const std::shared_ptr<TreeWorkMetrics> &work_metrics() const noexcept
    {
        return work_metrics_;
    }

    // false means no ownership was taken (full/closed/already submitted).
    // The coordinator owner must outlive producers and terminal callbacks.
    // close() rejects subsequent entry; producers already in try_submit() may
    // finish accepting their request and must be joined before destruction.
    bool try_submit(const std::shared_ptr<PublicationRequest> &request);
    // A Tree owns one prepaid window for its single active handoff. Prepare
    // before accepting proposals; the fixed lane cannot be enlarged later.
    Status prepare_lane(size_t capacity, const std::shared_ptr<detail::AllocationReservation> &metadata,
                        std::shared_ptr<PublicationLane> *out);
    bool   try_submit(const std::shared_ptr<PublicationRequest> &request, const std::shared_ptr<PublicationLane> &lane);
    // One dedicated slot finishes the already captured checkpoint even after
    // ordinary ingress closes. It never consumes ordinary queue capacity.
    bool try_submit_checkpoint_completion(const std::shared_ptr<PublicationRequest> &request);
    // Apply the requested floor only at a later atomic publication, clamped
    // to its completed coverage. Existing captured views remain unchanged.
    void                   request_gc_floor(uint64_t floor) noexcept;
    [[nodiscard]] uint64_t requested_gc_floor() const noexcept;
    // Build all new owners while ordinary publications still run. Installation
    // requires externally fenced ingress and drained accepted operations.
    [[nodiscard]] static std::optional<size_t> replacement_allocation_bound(const MappingView &mapping,
                                                                            size_t             sources) noexcept;
    Status             prepare_replacement(PublicationBootstrap bootstrap, std::shared_ptr<Replacement> *out,
                                           std::shared_ptr<const PublishedSources> inherited = {}) const;
    void               install_replacement(std::shared_ptr<Replacement> replacement) noexcept;
    void               close() noexcept;
    [[nodiscard]] bool idle() const noexcept;
    // Reclamation is an explicit maintenance operation, never part of capture.
    size_t reclaim();
    // Admission cancellation reserves this separately from source/worker
    // allocations so a failed capture can still release publication ownership.
    [[nodiscard]] static size_t empty_commit_reserve_bytes() noexcept;
    [[nodiscard]] static size_t snapshot_change_reserve_bytes() noexcept;
    [[nodiscard]] static size_t merge_membership_reserve_bytes(size_t pages) noexcept;
#ifdef CROWDB_TREE_TEST_UTIL
    void set_before_root_for_tests(std::function<void()> callback)
    {
        before_root_for_tests_ = std::move(callback);
    }
#endif

  private:
    struct Root : EpochManager::Deferred
    {
        std::shared_ptr<const PublishedTreeState>      state    = nullptr;
        std::shared_ptr<detail::AllocationReservation> metadata = nullptr;
        // Retired checkpoint sets use this ledger's allocator after a later
        // generation has installed a different ledger.
        std::shared_ptr<CheckpointLedger> checkpoint_allocator = nullptr;

        std::shared_ptr<const void> checkpoint_retired = nullptr;
        std::shared_ptr<const void> merge_retired      = nullptr;
        // Source-free fallback only while this root owns an incomplete handoff.
        std::shared_ptr<const PublishedTreeState> complete_l1 = nullptr;
        Root(std::shared_ptr<const PublishedTreeState>      value,
             std::shared_ptr<detail::AllocationReservation> reservation);
        static Root *create(std::shared_ptr<const PublishedTreeState>             value,
                            const std::shared_ptr<detail::AllocationReservation> &metadata = {});
        static void  dispose(Root *root) noexcept;
    };

    static std::shared_ptr<const PublishedTreeState>
                      prepare_complete_view(const Root &current, const PublishedTreeState &next,
                                            const std::shared_ptr<detail::AllocationReservation> &metadata);
    PublicationResult commit(const PublicationTransaction &transaction);
    void              drain() noexcept;
    void              complete(PublicationRequest *request) noexcept;
    void              enqueue(const std::shared_ptr<PublicationRequest> &request);

    std::array<std::atomic<uint64_t>, kMaintenanceSlots> maintenance_times_{};
    std::atomic<uint64_t>                                maintenance_scan_pid_{0};
    const size_t                                         capacity_;
    const uint64_t                                       identity_;
    std::atomic<size_t>                                  queued_{0};
    std::atomic<size_t>                                  ordinary_queued_{0};
    std::atomic<bool>                                    checkpoint_completion_{false};
    std::atomic<bool>                                    lane_prepared_{false};
    std::atomic<bool>                                    closed_{false};
    std::atomic<uint64_t>                                requested_gc_floor_{0};
    std::atomic<bool>                                    running_{false};
    std::atomic<PublicationRequest *>                    incoming_{nullptr};
    std::atomic<Root *>                                  root_{nullptr};
    // Retired roots may still contain allocator-backed checkpoint sets when
    // epoch destruction drains them, so this resource must outlive the epoch.
    std::pmr::memory_resource *const       checkpoint_upstream_;
    std::shared_ptr<CheckpointLedger>      checkpoint_ = nullptr;
    mutable EpochManager                   epoch_;
    const std::shared_ptr<TreeWorkMetrics> work_metrics_ = nullptr;
    const std::shared_ptr<SharedPageCache> page_cache_   = nullptr;
#ifdef CROWDB_TREE_TEST_UTIL
    std::function<void()> before_root_for_tests_;
#endif
};
} // namespace crowdb::tree
