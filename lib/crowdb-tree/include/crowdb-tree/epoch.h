// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Epoch-based reclamation (plan-tree #12: lock-free reader fast path).
//
// Readers take a Guard for the duration of a lock-free page walk. The single
// writer retire()s pages it replaces; a retired page is freed only once no
// Guard that could still reference it remains open.
//
// Reader path (enter / Guard::release) is LOCK-FREE: each thread publishes its
// entry epoch into a per-thread, cache-line-padded participant slot via atomics
// only — no mutex on get()/scan(). Reclamation is writer-driven: retire() and
// try_reclaim() scan the participant slots to find the oldest epoch any reader
// might still see and free everything strictly older. The retired list and the
// scan are serialized by reclaim_mu_ (writer-side only, off the hot path).
//
// Memory ordering (crossbeam/folly-style EBR): enter's global-epoch load and
// slot publish, plus retire's epoch bump and the reclaim scan, are all seq_cst,
// giving a single total order in which either the reclaimer observes a reader's
// entry epoch (and keeps the object) or the reader observes the writer's new
// mapping (and never touches the retired object). The participant list itself
// uses acquire/release: a brand-new participant can only be missed by a
// concurrent reclaim for objects retired *before* it entered, which it provably
// cannot reference (the mapping slot was already swapped).
#pragma once

#include "crowdb-tree/memory_budget.h"
#include "crowdb-tree/memtable/allocation.h"

#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace crowdb::tree
{

class EpochManager
{
  private:
    struct Lifetime;

    // Per-thread participant slot. Owned by the manager (allocated lazily on a
    // thread's first enter(), linked into participants_, freed at destruction).
    // Cache-line padded so readers on different threads don't false-share.
    struct alignas(64) Participant
    {
        std::atomic<uint64_t>             local_epoch{0}; // 0 = inactive (not inside a guard)
        Participant                      *next{nullptr};  // intrusive list; published w/ release
        uint32_t                          nest{0};        // reentrancy depth (owner thread only)
        uint64_t                          thread_id{0};   // immutable after publication
        std::shared_ptr<AllocationBudget> budget;
    };

    // The closed flag and head share one CAS word: a registration that read
    // the old head cannot append after an immutable source seals its list.
    class ParticipantList
    {
      public:
        [[nodiscard]] Participant *load(std::memory_order order) const noexcept
        {
            return pointer(head_.load(order));
        }

        Participant *exchange(Participant *value, std::memory_order order = std::memory_order_seq_cst) noexcept
        {
            return pointer(head_.exchange(reinterpret_cast<uintptr_t>(value), order));
        }

        [[nodiscard]] bool sealed() const noexcept
        {
            return (head_.load(std::memory_order_acquire) & 1) != 0;
        }

        Participant *seal() noexcept
        {
            return pointer(head_.fetch_or(1, std::memory_order_acq_rel));
        }

        bool push(Participant *participant, uintptr_t head) noexcept;

        [[nodiscard]] uintptr_t registration_head() const noexcept
        {
            return head_.load(std::memory_order_acquire);
        }

      private:
        static Participant *pointer(uintptr_t value) noexcept
        {
            return std::bit_cast<Participant *>(value & ~uintptr_t{1});
        }

        std::atomic<uintptr_t> head_{0};
    };

  public:
    using Deleter = std::function<void(void *)>;

    // Embedded before publication, so handing off ownership cannot allocate or
    // invoke a destructor on the apply path. The callback destroys its ticket.
    struct Deferred
    {
        Deferred *next                       = nullptr;
        uint64_t  epoch                      = 0;
        void (*destroy)(Deferred *) noexcept = nullptr;
    };

    void defer(Deferred *entry) noexcept;

    // RAII reader guard. Holds an epoch open until destroyed. Thread-bound: a
    // Guard must be released on the thread that created it (do not move across
    // threads).
    class Guard
    {
      public:
        Guard() = default;

        explicit Guard(Participant *p, std::shared_ptr<Lifetime> lifetime) : p_(p), lifetime_(std::move(lifetime))
        {
        }

        Guard(Guard &&o) noexcept : p_(o.p_), lifetime_(std::move(o.lifetime_))
        {
            o.p_ = nullptr;
        }

        Guard &operator=(Guard &&o) noexcept
        {
            if (this != &o) {
                release();
                p_        = o.p_;
                lifetime_ = std::move(o.lifetime_);
                o.p_      = nullptr;
            }
            return *this;
        }

        Guard(const Guard &)            = delete;
        Guard &operator=(const Guard &) = delete;

        ~Guard()
        {
            release();
        }

      private:
        void                      release();
        Participant              *p_ = nullptr;
        std::shared_ptr<Lifetime> lifetime_;
    };

    explicit EpochManager(std::shared_ptr<AllocationBudget> budget = {}, bool enabled = true);
    ~EpochManager();

    [[nodiscard]] bool source_sealed() const noexcept;
#ifdef CROWDB_TREE_TEST_UTIL
    void set_before_participant_append_for_tests(void (*hook)(void *), void *context) noexcept
    {
        append_hook_    = hook;
        append_context_ = context;
    }
#endif

    EpochManager(const EpochManager &)            = delete;
    EpochManager &operator=(const EpochManager &) = delete;

    // Open a reader guard at the current epoch (lock-free).
    // A new thread's participant uses the current operation's credit when
    // supplied. Existing participants require no new allocation after seal.
    Guard enter(std::shared_ptr<AllocationBudget> budget = {});

    [[nodiscard]] static size_t participant_allocation_bytes() noexcept
    {
        return sizeof(Participant);
    }

    [[nodiscard]] static size_t infrastructure_allocation_bound() noexcept;

    // Defer deletion of `ptr` until no guard opened at-or-before now remains.
    void retire(void *ptr, Deleter deleter);

    // Convenience: retire a typed pointer with `delete`.
    template <class T> void retire_object(T *p)
    {
        retire(p, [](void *x) { delete static_cast<T *>(x); });
    }

    // Force a reclamation sweep. Returns the number of objects freed.
    size_t try_reclaim();

    [[nodiscard]] const std::shared_ptr<AllocationCounter> &memtable_allocation() const
    {
        return memtable_allocation_;
    }

    // Diagnostics.
    [[nodiscard]] size_t pending_retired();
    [[nodiscard]] size_t active_guards();

  private:
    friend class Guard;
    friend class MemTable;
    void seal_source() noexcept;
    friend class BlockDirectory;
    // Exclusive backend teardown: no remaining guards or accepted operations.
    bool         retire_backend_step() noexcept;
    Participant *participant_for_this_thread(const std::shared_ptr<AllocationBudget> &budget);
    static void  destroy_participant(Participant *participant) noexcept;
    size_t       reclaim_locked();   // caller holds reclaim_mu_
    uint64_t     min_active_epoch(); // oldest epoch any reader might still see
    size_t       reclaim_deferred_locked();

    struct Retired
    {
        uint64_t epoch;
        void    *ptr;
        Deleter  deleter;
    };

    std::shared_ptr<AllocationBudget>  budget_;
    std::shared_ptr<AllocationCounter> memtable_allocation_;
    std::shared_ptr<Lifetime>          lifetime_;
    const uint64_t                     id_; // stable key for per-thread cache
    std::atomic<uint64_t>              global_epoch_{1};
    ParticipantList                    participants_;
#ifdef CROWDB_TREE_TEST_UTIL
    void (*append_hook_)(void *) = nullptr;
    void *append_context_        = nullptr;
#endif

    // A retired object's deleter can retire another object on this same manager.
    // Reclamation permits that same-thread re-entry.
    std::recursive_mutex    reclaim_mu_; // guards retired_ (writer side)
    std::vector<Retired>    retired_;
    std::atomic<Deferred *> deferred_{nullptr};
    Deferred               *collected_ = nullptr;
};

} // namespace crowdb::tree
