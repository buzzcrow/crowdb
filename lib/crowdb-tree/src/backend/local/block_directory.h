// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/backend/backend_release_service.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "crowdb-tree/epoch.h"

#include <array>

namespace crowdb::tree
{
// Sparse stable radix nodes never move while this backend is open. A reader
// copies a file owner under a short epoch guard and releases the guard before
// doing I/O. Only removed file incarnations enter epoch reclamation.
class BlockDirectory
{
    struct Entry : EpochManager::Deferred
    {
        explicit Entry(std::shared_ptr<AllocationBudget> owner = {});
        std::shared_ptr<AllocationBudget> budget;
        std::shared_ptr<FileMedium>       medium;
    };

    struct Segment
    {
        explicit Segment(std::shared_ptr<AllocationBudget> owner) : budget(std::move(owner))
        {
        }

        std::shared_ptr<AllocationBudget>     budget;
        std::array<std::atomic<Entry *>, 256> slots{};
    };

    struct Node
    {
        explicit Node(std::shared_ptr<AllocationBudget> owner = {}) : budget(std::move(owner))
        {
        }

        std::shared_ptr<AllocationBudget>    budget;
        std::array<std::atomic<void *>, 256> children{};
    };

  public:
    explicit BlockDirectory(std::shared_ptr<AllocationBudget>      budget  = {},
                            std::shared_ptr<BackendReleaseService> release = {});
    ~BlockDirectory();
    void                                      release_owned() noexcept;
    [[nodiscard]] std::shared_ptr<FileMedium> get(uint32_t                                 index,
                                                  const std::shared_ptr<AllocationBudget> &budget = {}) const;
    Status ensure(uint32_t index, const char *path, bool direct, std::shared_ptr<FileMedium> *out,
                  const std::shared_ptr<AllocationBudget> &budget = {});

    Status ensure(uint32_t index, const std::string &path, bool direct, std::shared_ptr<FileMedium> *out)
    {
        return ensure(index, path.c_str(), direct, out);
    }

    // The caller must exclusively own the entire byte extent before unlinking.
    // Conflicting metadata operations return Unavailable rather than waiting.
    Status erase(uint32_t index, const char *path, const std::shared_ptr<AllocationBudget> &budget = {});

    Status erase(uint32_t index, const std::string &path)
    {
        return erase(index, path.c_str());
    }

    // Advance through populated radix branches only; no vector or dense gap scan.
    bool                        next(uint64_t *cursor, uint32_t *index, std::shared_ptr<FileMedium> *out,
                                     const std::shared_ptr<AllocationBudget> &budget = {}) const;
    [[nodiscard]] size_t        count() const noexcept;
    [[nodiscard]] static size_t extent_allocation_bound() noexcept;

  private:
    Segment       *segment(uint32_t index, bool create, const std::shared_ptr<AllocationBudget> &budget);
    const Segment *segment(uint32_t index) const;
    bool           next_node(const Node &node, unsigned shift, uint64_t prefix, uint64_t first, uint32_t *index,
                             std::shared_ptr<FileMedium> *out) const;
    static void    destroy_node(Node &node, unsigned shift);
    bool           retire_step() noexcept;

    struct Retirement : BackendReleaseService::Node
    {
        BlockDirectory *directory = nullptr;
    };

    Retirement              retirement_;
    std::array<void *, 4>   retire_path_{};
    std::array<uint16_t, 4> retire_index_{};
    uint8_t                 retire_depth_    = 0;
    bool                    retiring_        = false;
    bool                    entries_drained_ = false;

    std::shared_ptr<AllocationBudget> budget_;
    Node                              root_;
    Entry                             busy_;
    std::atomic<size_t>               live_{0};
    mutable EpochManager              epoch_;
};
} // namespace crowdb::tree
