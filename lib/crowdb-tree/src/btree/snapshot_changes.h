// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"
#include "page_key_range.h"

namespace crowdb::tree
{
// Arbitrary replacements require a complete comparison. Admitted handoff
// adds cells above its old frontier; maintenance changes source topology
// without changing the logical key/cell view. A pending PID accumulates bits.
enum class SnapshotChangeEffect : uint8_t { FullContent = 1, AcceptedPrefix = 2, SourceStructure = 4 };

class SnapshotChangeEffects
{
  public:
    constexpr SnapshotChangeEffects(SnapshotChangeEffect effect = SnapshotChangeEffect::FullContent) noexcept
        : bits_(static_cast<uint8_t>(effect))
    {
    }

    [[nodiscard]] constexpr bool contains(SnapshotChangeEffect effect) const noexcept
    {
        return (bits_ & static_cast<uint8_t>(effect)) != 0;
    }

    constexpr bool operator==(const SnapshotChangeEffects &) const noexcept = default;

    [[nodiscard]] friend constexpr SnapshotChangeEffects operator|(SnapshotChangeEffects left,
                                                                   SnapshotChangeEffects right) noexcept
    {
        left.bits_ |= right.bits_;
        return left;
    }

  private:
    uint8_t bits_;
};

[[nodiscard]] constexpr SnapshotChangeEffects operator|(SnapshotChangeEffect left, SnapshotChangeEffect right) noexcept
{
    return SnapshotChangeEffects(left) | SnapshotChangeEffects(right);
}

// Compact logical identities never own a PublishedPage, source or value buffer.
struct SnapshotPageIdentity
{
    PageKeyRange range{};
    uint64_t     content       = 0;
    uint64_t     route         = 0;
    uint64_t     revision      = 0;
    uint64_t     right_sibling = UINT64_MAX;
    uint32_t     level         = 0;
    bool         present       = false;
};

struct SnapshotPageChange
{
    uint64_t              page_id         = 0;
    uint64_t              publication_seq = 0;
    SnapshotPageIdentity  before{};
    SnapshotPageIdentity  after{};
    SnapshotChangeEffects effect = SnapshotChangeEffect::FullContent;
};

// Persistent compressed binary radix tree. Capture shares one root, and an
// update copies at most one branch per PID bit. Deleted pages remain explicit
// until the captured changes commit; no payload or old mapping is retained.
class SnapshotChanges
{
    struct Node;
    struct Leaf;
    struct Branch;
    using Owner = std::shared_ptr<const Node>;

  public:
    static constexpr uint64_t     kPageLimit = uint64_t{1} << 30;
    [[nodiscard]] SnapshotChanges replace(SnapshotPageChange                                    change,
                                          const std::shared_ptr<detail::AllocationReservation> &metadata = {}) const;
    [[nodiscard]] const SnapshotPageChange    *get(uint64_t pid) const noexcept;
    [[nodiscard]] size_t                       size() const noexcept;
    [[nodiscard]] bool                         empty() const noexcept;
    [[nodiscard]] bool                         same_root(const SnapshotChanges &other) const noexcept;
    [[nodiscard]] static size_t                change_allocation_bound() noexcept;
    [[nodiscard]] static std::optional<size_t> initial_allocation_bound(size_t pages) noexcept;
    [[nodiscard]] static SnapshotChanges
    from_sorted(std::span<const SnapshotPageChange>                   changes,
                const std::shared_ptr<detail::AllocationReservation> &metadata = {});

    using Visitor = void (*)(void *, const SnapshotPageChange &);
    // Resume at next_pid; each step returns one selected PID, never scans gaps.
    [[nodiscard]] bool visit_from(uint64_t &next_pid, size_t max_pages, void *context, Visitor visitor) const;

  private:
    static Owner       insert(const Owner &node, const std::shared_ptr<const Leaf> &leaf,
                              const std::shared_ptr<detail::AllocationReservation> &metadata);
    static const Leaf *lower_bound(const Owner &node, uint64_t pid) noexcept;
    static Owner       build(std::span<const SnapshotPageChange>                   changes,
                             const std::shared_ptr<detail::AllocationReservation> &metadata);
    Owner              root_;
};
} // namespace crowdb::tree
