// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"
#include "crowdb-tree/btree/diagnostics.h"

#include <array>
#include <compare>
#include <map>
#include <memory_resource>
#include <optional>
#include <span>

namespace crowdb::tree
{
// The left-to-right sibling relation gives every adjacent pair one identity.
// The proof contains logical versions only; a representation rebase preserves it.
struct MergePairProof
{
    uint64_t left          = 0;
    uint64_t right         = 0;
    uint64_t parent        = 0;
    uint64_t left_content  = 0;
    uint64_t right_content = 0;
    uint64_t left_route    = 0;
    uint64_t right_route   = 0;
    bool     ready         = false;

    bool operator==(const MergePairProof &) const = default;
};

class MergePairMetrics
{
    struct Pair
    {
        MergePairProof proof;
        uint64_t       since_ns;
    };
    enum class Kind : uint8_t { Endpoint, Parent, Time };

    struct Key
    {
        Kind     kind;
        uint64_t value;
        uint64_t left;
        auto     operator<=>(const Key &) const = default;
    };

    using Pairs = std::pmr::map<uint64_t, Pair>;
    using Index = std::pmr::map<Key, uint64_t>;
    struct Retired;

  public:
    struct Snapshot
    {
        uint64_t pages                              = 0;
        uint64_t pairs                              = 0;
        uint64_t ready                              = 0;
        uint64_t oldest_since_ns                    = 0;
        bool     operator==(const Snapshot &) const = default;
    };

    struct Membership
    {
        uint64_t pid;
        bool     present;
    };

    explicit MergePairMetrics(std::pmr::memory_resource *resource = std::pmr::get_default_resource());
    MergePairMetrics(const MergePairMetrics &)            = delete;
    MergePairMetrics &operator=(const MergePairMetrics &) = delete;

    // Only the publication owner accesses this ledger. Preparation leaves the
    // live maps untouched. Commit moves already allocated map nodes and hands
    // old nodes to a bounded retirement owner; it cannot allocate or fail.
    class Prepared
    {
      public:
        Prepared(MergePairMetrics &owner, const std::shared_ptr<detail::AllocationReservation> &metadata,
                 std::shared_ptr<const void> resource_owner);
        Prepared(const Prepared &)                                              = delete;
        Prepared                                   &operator=(const Prepared &) = delete;
        [[nodiscard]] std::span<const Membership>   membership_changes() const noexcept;
        [[nodiscard]] std::span<const uint64_t>     affected_pages() const noexcept;
        [[nodiscard]] std::optional<MergePairProof> pair(uint64_t left) const noexcept;
        [[nodiscard]] bool                          contains_page(uint64_t pid) const noexcept;
        void                                        invalidate_page(uint64_t pid);
        void                                        invalidate_parent(uint64_t pid);
        void                                        clear_pair(uint64_t left);
        void                                        set_pair(MergePairProof proof, uint64_t now_ns);
        void                                        finish();
        [[nodiscard]] Snapshot                      snapshot() const noexcept;
        [[nodiscard]] std::shared_ptr<const void>   commit() noexcept;

      private:
        void                               invalidate(Kind kind, uint64_t value);
        [[nodiscard]] bool                 removes(uint64_t left) const noexcept;
        MergePairMetrics                  &owner_;
        detail::ReservedVector<uint64_t>   removed_;
        detail::ReservedVector<uint64_t>   endpoints_;
        detail::ReservedVector<Membership> membership_;
        Pairs                              added_pairs_;
        Index                              added_index_;
        std::shared_ptr<Retired>           retired_;
        Snapshot                           next_;
        bool                               finished_  = false;
        bool                               committed_ = false;
    };

    [[nodiscard]] Snapshot                      snapshot() const noexcept;
    [[nodiscard]] std::optional<MergePairProof> pair(uint64_t left) const noexcept;
    [[nodiscard]] bool                          contains_page(uint64_t pid) const noexcept;
    [[nodiscard]] bool                          consistent() const noexcept;
    bool                                        retire_step() noexcept;
    // A leaf change invalidates at most its incoming and outgoing pair. An
    // index change additionally invalidates its own direct child pairs.
    [[nodiscard]] static size_t invalidation_bytes(size_t pairs) noexcept;
    [[nodiscard]] static size_t insertion_bytes() noexcept;

  private:
    Pairs    pairs_;
    Index    index_;
    Snapshot totals_;
};
} // namespace crowdb::tree
