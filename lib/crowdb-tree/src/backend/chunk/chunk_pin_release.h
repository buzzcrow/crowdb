// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/allocation_bound.h"
#include "chunk_stage.h"

#include <atomic>
#include <memory>

namespace crowdb::tree::detail
{
class RootCatalog;
class ChunkPinReleaseQueue;
struct ChunkPinReleaseNode;

class ChunkPinOwner
{
  public:
    ChunkPinOwner() = default;
    ~ChunkPinOwner();
    ChunkPinOwner(const ChunkPinOwner &)            = delete;
    ChunkPinOwner &operator=(const ChunkPinOwner &) = delete;
    // Arm before the first metadata call: an unknown reply still needs release.
    void arm() noexcept;

  private:
    friend class ChunkPinReleaseQueue;
    ChunkPinReleaseNode *node_ = nullptr;
};

// This paid queue owns neither a Tree nor an executor. The host backend service
// pumps it while its transport and metadata clients remain alive. Dropping a
// pin only transfers its preallocated node; it performs no metadata operation.
class ChunkPinReleaseQueue : public std::enable_shared_from_this<ChunkPinReleaseQueue>
{
  public:
    using Wake = void (*)(void *) noexcept;

    struct Snapshot
    {
        size_t   capacity;
        size_t   registered;
        size_t   pending;
        uint64_t denied;
        bool     closed;
    };

    struct Step
    {
        size_t completed;
        size_t failed;
        bool   more;
    };

    static Status create(const std::shared_ptr<AllocationBudget> &budget, size_t capacity,
                         std::shared_ptr<const void> wake_owner, Wake wake, void *context,
                         std::shared_ptr<ChunkPinReleaseQueue> *out);
    ~ChunkPinReleaseQueue();
    static size_t allocation_bound() noexcept;
    static size_t pin_allocation_bound() noexcept;
    Status prepare(const std::shared_ptr<AllocationBudget> &budget, std::shared_ptr<RootCatalog> catalog, uint64_t tree,
                   ChunkStageId stage, ChunkStageId pin, std::shared_ptr<ChunkPinOwner> *out);
    // A writer session is irreversible. Call arm only after claiming its exact
    // catalog incarnation; release after accepted backend work has drained.
    Status prepare_writer(const std::shared_ptr<AllocationBudget> &budget, std::shared_ptr<RootCatalog> catalog,
                          uint64_t tree, uint64_t epoch, std::shared_ptr<ChunkPinOwner> *out);
    // The single host backend consumer runs at most limit metadata operations.
    // Failed operations retain their exact identity and credit for a later tick.
    Step     pump(size_t limit) noexcept;
    void     stop_admission() noexcept;
    Snapshot snapshot() const noexcept;
    bool     drained() const noexcept;

  private:
    friend class ChunkPinOwner;
    Status prepare_node(const std::shared_ptr<AllocationBudget> &budget, std::shared_ptr<RootCatalog> catalog,
                        uint64_t tree, ChunkStageId stage, ChunkStageId pin, bool writer,
                        std::shared_ptr<ChunkPinOwner> *out);
    void   release(ChunkPinReleaseNode *node) noexcept;
    void   finish(ChunkPinReleaseNode *node) noexcept;
    void   push(ChunkPinReleaseNode *node) noexcept;
    static constexpr size_t            kClosed = size_t{1} << ((sizeof(size_t) * 8) - 1);
    std::shared_ptr<const void>        wake_owner_;
    Wake                               wake_     = nullptr;
    void                              *context_  = nullptr;
    size_t                             capacity_ = 0;
    std::atomic<size_t>                state_{0};
    std::atomic<size_t>                pending_{0};
    std::atomic<uint64_t>              denied_{0};
    std::atomic<ChunkPinReleaseNode *> incoming_{nullptr};
    ChunkPinReleaseNode               *active_       = nullptr;
    ChunkPinReleaseNode               *retry_        = nullptr;
    bool                               prefer_retry_ = false;
    std::atomic_flag                   consumer_     = ATOMIC_FLAG_INIT;
};
} // namespace crowdb::tree::detail
