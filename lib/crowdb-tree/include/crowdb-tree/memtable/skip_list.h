// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Concurrent one-node-per-key map. Level zero establishes membership; upper
// links accelerate lookup. Published nodes remain linked for their lifetime.
// Callers protect borrowed pointers with the list's epoch manager.
#pragma once

#include "crowdb-tree/btree/cell.h"
#include "crowdb-tree/buffer.h"
#include "crowdb-tree/epoch.h"
#include "crowdb-tree/memory_budget.h"
#include "crowdb-tree/memtable/source_stats.h"
#include "crowdb-tree/slice.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace crowdb::tree
{

// A versioned cell value held by a skip-list node. Allocated separately so
// it can be atomically swapped (overwrite) and epoch-retired independently
// of the node. The buffer is the same contiguous-or-split form as
// cell_entry::cell (R30): kOwned = full [header][value]; kExternal =
// value-only borrowed from a Rust Bytes (drop_fn fires on destruction).
struct CellVersion
{
    static std::shared_ptr<CellVersion> create(buffer bytes, uint64_t version, uint8_t kind,
                                               std::shared_ptr<AllocationCounter> counter, AllocationLease credit,
                                               const std::shared_ptr<AllocationBudget> &budget);
    AllocationLease                     payload_credit;
    buffer                              cell;
    uint64_t                            slot;
    uint8_t                             flags;
    AllocationCharge                    allocation;

    CellVersion(buffer bytes, uint64_t version, uint8_t kind, std::shared_ptr<AllocationCounter> counter = nullptr,
                AllocationLease credit = {})
        : payload_credit(std::move(credit)),
          cell(std::move(bytes)),
          slot(version),
          flags(kind)
    {
        if (counter != nullptr) {
            allocation.set(std::move(counter), sizeof(CellVersion) + cell.size());
        }
    }
};

// Immutable descriptor. The common case has no history allocation. History
// is ordered newest first; payload ownership is shared across descriptors.
struct VersionSet : EpochManager::Deferred, detail::RetirementNode
{
    AllocationLease                          descriptor_credit;
    std::shared_ptr<detail::RetirementQueue> retirement;
    std::shared_ptr<const CellVersion>       current;
    std::vector<std::shared_ptr<const CellVersion>, MetadataAllocator<std::shared_ptr<const CellVersion>>> history;
    uint64_t                                                                                               bound = 0;
    size_t                                                                                                 bytes = 0;
    AllocationCharge                                                                                       allocation;

    explicit VersionSet(AllocationLease credit = {}, std::shared_ptr<AllocationBudget> budget = {});

    struct Deleter
    {
        void operator()(VersionSet *versions) const noexcept;
    };

    using Owner = std::unique_ptr<VersionSet, Deleter>;
    static Owner                                            create(const std::shared_ptr<AllocationBudget> &budget);
    [[nodiscard]] const CellVersion                        *at(uint64_t frontier) const;
    [[nodiscard]] const std::shared_ptr<const CellVersion> *owner_at(uint64_t frontier) const;
};

struct MutationStats
{
    uint64_t                     copy_count    = 0;
    uint64_t                     copy_ns       = 0;
    uint64_t                     copy_max_ns   = 0;
    bool                         measure_copy  = false;
    uint64_t                     cas_retries   = 0;
    uint64_t                     overwrite     = 0;
    uint64_t                     keep          = 0;
    uint64_t                     merged        = 0;
    uint64_t                     keep_gap      = 0;
    uint64_t                     keep_pending  = 0;
    uint64_t                     keep_boundary = 0;
    const std::atomic<uint64_t> *admission     = nullptr;
    bool                         gap           = false;
};

struct Node : EpochManager::Deferred
{
    AllocationLease           allocation_credit;
    AllocationCharge          allocation;
    std::atomic<VersionSet *> versions_{nullptr};
    uint32_t                  height_{1};  // tower height
    uint32_t                  key_len_{0}; // inline key length

    // The tower follows the fixed fields. Access via next_ptr(level).
    [[nodiscard]] std::atomic<Node *> *next_ptr(uint32_t level)
    {
        return reinterpret_cast<std::atomic<Node *> *>(reinterpret_cast<char *>(this) + sizeof(Node)) + level;
    }

    [[nodiscard]] const std::atomic<Node *> *next_ptr(uint32_t level) const
    {
        return reinterpret_cast<const std::atomic<Node *> *>(reinterpret_cast<const char *>(this) + sizeof(Node)) +
               level;
    }

    [[nodiscard]] Node *next(uint32_t level) const
    {
        return next_ptr(level)->load(std::memory_order_acquire);
    }

    void set_next(uint32_t level, Node *n)
    {
        next_ptr(level)->store(n, std::memory_order_release);
    }

    [[nodiscard]] const char *key_data() const
    {
        return reinterpret_cast<const char *>(this) + sizeof(Node) + (sizeof(std::atomic<Node *>) * height_);
    }

    [[nodiscard]] Slice key_slice() const
    {
        return {key_data(), key_len_};
    }

    // Total allocation size for a node with `height` and `key_len`.
    [[nodiscard]] static size_t alloc_size(uint32_t height, size_t key_len)
    {
        return sizeof(Node) + (sizeof(std::atomic<Node *>) * height) + key_len;
    }
};

class ConcurrentSkipList
{
  public:
    static constexpr uint32_t kMaxHeight = 12;

    // Preparation is thread-bound and owns every allocation needed by commit.
    // The source admission authority bounds all versions that may race this
    // write, including writers that have not reached their CAS yet.
    class PreparedWrite
    {
      public:
        PreparedWrite()                                     = default;
        PreparedWrite(PreparedWrite &&) noexcept            = default;
        PreparedWrite &operator=(PreparedWrite &&) noexcept = default;
        PreparedWrite(const PreparedWrite &)                = delete;
        PreparedWrite &operator=(const PreparedWrite &)     = delete;

        [[nodiscard]] Slice key() const noexcept
        {
            return candidate_->key_slice();
        }

        [[nodiscard]] const std::shared_ptr<const CellVersion> &cell_owner() const noexcept
        {
            return cell_;
        }

      private:
        friend class ConcurrentSkipList;

        struct NodeDeleter
        {
            void operator()(Node *node) const noexcept;
        };

        EpochManager::Guard                guard_;
        std::shared_ptr<const CellVersion> cell_;
        bool                               initialize_slot_ = true;
        VersionSet::Owner                  initial_;
        VersionSet::Owner                  replacement_;
        std::unique_ptr<Node, NodeDeleter> candidate_;
        uint64_t                           bound_ = 0;
    };

    [[nodiscard]] PreparedWrite prepare_write(Slice key, std::shared_ptr<CellVersion> cell, uint64_t bound,
                                              size_t max_versions, const std::shared_ptr<AllocationBudget> &budget);
    // Consumes preparation once. Concurrent version changes are rebuilt using
    // its fixed capacity; this path does not allocate or return an error.
    [[nodiscard]] PreparedWrite prepare_reference(Slice key, std::shared_ptr<const CellVersion> cell, uint64_t bound,
                                                  size_t max_versions, const std::shared_ptr<AllocationBudget> &budget);
    bool                        commit_write(PreparedWrite write, uint64_t slot) noexcept;

    // Ordered cursor with one coherent version candidate per position. The
    // caller retains source ownership and epoch protection for its lifetime.
    class Cursor
    {
      public:
        Cursor() = default;

        explicit Cursor(const Node *n, uint64_t frontier = UINT64_MAX, uint64_t floor = 0,
                        std::optional<Slice> upper = std::nullopt)
            : cur_(n),
              frontier_(frontier),
              floor_(floor),
              upper_(upper)
        {
            select();
        }

        [[nodiscard]] bool valid() const
        {
            return cur_ != nullptr;
        }

        [[nodiscard]] Slice key() const
        {
            return cur_->key_slice();
        }

        [[nodiscard]] const CellVersion *cell_version() const
        {
            return candidate_;
        }

        // Copy while the source and this thread's epoch guard are alive. The
        // result owns only this version, independently of the node/history.
        [[nodiscard]] std::shared_ptr<const CellVersion> cell_owner() const
        {
            return candidate_owner_ != nullptr ? *candidate_owner_ : nullptr;
        }

        // Select this exact key at a captured frontier without advancing to
        // another key. The caller keeps the source and its epoch guard alive.
        [[nodiscard]] std::shared_ptr<const CellVersion> cell_owner_at(uint64_t frontier) const
        {
            if (cur_ == nullptr) {
                return {};
            }
            const auto *versions = cur_->versions_.load(std::memory_order_acquire);
            const auto *owner    = versions->owner_at(frontier);
            return owner != nullptr ? *owner : nullptr;
        }

        void set_floor(uint64_t floor)
        {
            floor_ = floor;
            select();
        }

        // Advance to the next key with an eligible version.
        void advance();

        // Prefetch the next node's memory (the one advance() will move to).
        // A non-faulting hint — brings the next node into CPU cache before
        // the merge loop calls advance(), overlapping the cache fill with
        // the current merge step's work.
        void prefetch_next() const
        {
            if (cur_ != nullptr) {
                if (const Node *n = cur_->next(0); n != nullptr) {
                    __builtin_prefetch(n, 0, 1);
                }
            }
        }

      private:
        void                                      select();
        const Node                               *cur_       = nullptr;
        const CellVersion                        *candidate_ = nullptr;
        uint64_t                                  frontier_  = UINT64_MAX;
        uint64_t                                  floor_     = 0;
        std::optional<Slice>                      upper_;
        const std::shared_ptr<const CellVersion> *candidate_owner_ = nullptr;
    };

    explicit ConcurrentSkipList(EpochManager *epoch = nullptr, std::shared_ptr<AllocationBudget> budget = {});
    ~ConcurrentSkipList();

    ConcurrentSkipList(const ConcurrentSkipList &)            = delete;
    ConcurrentSkipList &operator=(const ConcurrentSkipList &) = delete;

#ifdef CROWDB_TREE_TEST_UTIL
    enum class PausePoint : uint8_t {
        kBeforePrepare,
        kBeforeSearch,
        kAfterLevelZero,
        kBeforeVersionCas,
        kBeforeInsertAccounting,
        kBeforeLeaveAccounting,
        kBeforeCloseAccounting
    };
    using TestHook = void (*)(void *, PausePoint, Slice);

    void set_hook_for_tests(void *context, TestHook hook)
    {
        test_context_ = context;
        test_hook_    = hook;
    }
#endif

    // Consumes the payload on every outcome. Bound is validated by admission.
    bool upsert(Slice key, std::shared_ptr<const CellVersion> cv, uint64_t bound, MutationStats *stats = nullptr,
                std::shared_ptr<AllocationBudget> budget = {});
    void prune(Slice key, uint64_t bound, MutationStats *stats = nullptr);
    [[nodiscard]] Cursor prefix_cursor(uint64_t frontier, uint64_t floor = 0) const;
    // Bounds are [lower, upper); an absent bound differs from an empty key.
    // Bound storage and source/epoch protection must outlive the cursor.
    [[nodiscard]] Cursor range_cursor(std::optional<Slice> lower, std::optional<Slice> upper, uint64_t frontier,
                                      uint64_t floor = 0) const;
    // Constant-time independently sampled logical/source capacities. These
    // counters describe the current linked versions, not physical allocation
    // identity or memory retained by old readers.
    [[nodiscard]] VersionMemory memory() const;
#ifdef CROWDB_TREE_TEST_UTIL
    [[nodiscard]] VersionMemory scan_memory_for_tests() const;
#endif

    [[nodiscard]] EpochManager &epoch() const
    {
        return *epoch_;
    }

    [[nodiscard]] const std::shared_ptr<AllocationBudget> &budget() const noexcept
    {
        return budget_;
    }

    // Point lookup: returns the CellVersion* for `key`, or nullptr. The
    // returned pointer is valid only while the caller's epoch guard is held
    // (a concurrent overwrite retires the old version via epoch).
    [[nodiscard]] const CellVersion *find(Slice key) const;

    // Return a cursor positioned at the first live node with key >
    // `start_after` (or the first live node if start_after is empty).
    [[nodiscard]] Cursor cursor(Slice start_after) const;

    // Return a cursor positioned at the first live node >= `start_key`, or
    // strictly greater when `inclusive` is false. Unlike cursor(), an empty
    // key is an explicit bound.
    [[nodiscard]] Cursor cursor_from(Slice start_key, bool inclusive) const;

    // Return the final live node <= `start_key`, or < it when `inclusive` is
    // false. When `has_start_bound` is false, return the final live node.
    [[nodiscard]] Cursor cursor_reverse(Slice start_key, bool has_start_bound, bool inclusive) const;

    [[nodiscard]] size_t count() const
    {
        return count_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool empty() const
    {
        return count_.load(std::memory_order_relaxed) == 0;
    }

    [[nodiscard]] size_t approx_bytes() const
    {
        return bytes_.load(std::memory_order_relaxed);
    }

    void add_bytes(size_t n)
    {
        bytes_.fetch_add(n, std::memory_order_relaxed);
        if (source_ledger_) {
            source_ledger_->replace({}, {.logical_bytes = n});
        }
    }

    void sub_bytes(size_t n)
    {
        bytes_.fetch_sub(n, std::memory_order_relaxed);
        if (source_ledger_) {
            source_ledger_->replace({.logical_bytes = n}, {});
        }
    }

    // Public so MemTable can pass it to epoch_.retire() as the deleter.
    static void free_node(void *p);

  private:
    friend class MemTable;
    bool                                retire_step() noexcept;
    std::shared_ptr<SourceMemoryLedger> source_ledger_;
    void                                account_versions(const VersionSet *previous, const VersionSet &next) noexcept;
    void                                account_node(const Node &node, const VersionSet &versions) noexcept;
    friend class Cursor;

    [[nodiscard]] static Node *alloc_node(uint32_t height, Slice key, const std::shared_ptr<AllocationBudget> &budget);

    static uint32_t random_height();
    Node           *find_or_insert(Slice key, VersionSet *versions, bool *inserted);
    void            link_upper(Node *node);
    bool            replace(Node *node, std::shared_ptr<const CellVersion> cv, uint64_t bound, MutationStats *stats,
                            const std::shared_ptr<AllocationBudget> &budget);

    Node *find_ge(Slice key, Node **prev, Node **successors = nullptr) const;

#ifdef CROWDB_TREE_TEST_UTIL
    void    *test_context_ = nullptr;
    TestHook test_hook_    = nullptr;
#endif
    Node                             *head_;
    std::atomic<uint32_t>             max_height_{1};
    std::atomic<size_t>               count_{0};
    std::atomic<size_t>               bytes_{0};
    AllocationLease                   epoch_credit_;
    std::unique_ptr<EpochManager>     owned_epoch_;
    EpochManager                     *epoch_;
    std::shared_ptr<AllocationBudget> budget_;
    std::atomic<uint64_t>             history_count_{0};
    std::atomic<uint64_t>             history_bytes_{0};
    std::atomic<uint64_t>             descriptor_bytes_{0};
    std::atomic<uint64_t>             node_bytes_{0};
    std::atomic<uint64_t>             payload_bytes_{0};
};

} // namespace crowdb::tree
