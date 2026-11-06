// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot_changes.h"

#include <algorithm>
#include <array>
#include <bit>
#include <stdexcept>

namespace crowdb::tree
{
struct SnapshotChanges::Node
{
    uint64_t first = 0;
    uint64_t last  = 0;
    uint64_t bit   = 0;
    size_t   count = 1;

    bool retire_step() noexcept
    {
        return true;
    }
};

struct SnapshotChanges::Leaf : Node
{
    explicit Leaf(SnapshotPageChange value) : change(std::move(value))
    {
        first = last = change.page_id;
    }

    SnapshotPageChange change;
};

struct SnapshotChanges::Branch : Node
{
    Branch(uint64_t split, Owner left, Owner right) : children{std::move(left), std::move(right)}
    {
        bit   = split;
        first = children[0]->first;
        last  = children[1]->last;
        count = children[0]->count + children[1]->count;
    }

    std::array<Owner, 2> children;
};

SnapshotChanges::Owner SnapshotChanges::insert(const Owner &node, const std::shared_ptr<const Leaf> &leaf,
                                               const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    if (!node) {
        return leaf;
    }
    const auto differing = std::bit_floor(node->first ^ leaf->first);
    if (differing > node->bit) {
        const bool right = (leaf->first & differing) != 0;
        return detail::reserved_shared<Branch>(metadata, differing, right ? node : leaf, right ? leaf : node);
    }
    if (node->bit == 0) {
        return leaf;
    }
    const auto &branch = static_cast<const Branch &>(*node);
    const bool  right  = (leaf->first & node->bit) != 0;
    return detail::reserved_shared<Branch>(metadata, node->bit,
                                           right ? branch.children[0] : insert(branch.children[0], leaf, metadata),
                                           right ? insert(branch.children[1], leaf, metadata) : branch.children[1]);
}

SnapshotChanges SnapshotChanges::replace(SnapshotPageChange                                    change,
                                         const std::shared_ptr<detail::AllocationReservation> &metadata) const
{
    if (change.page_id >= kPageLimit) {
        throw std::invalid_argument("snapshot change exceeds the PID namespace");
    }
    if (const auto *previous = get(change.page_id)) {
        change.before = previous->before;
        change.effect = change.effect | previous->effect;
    }
    auto            leaf = detail::reserved_shared<Leaf>(metadata, std::move(change));
    SnapshotChanges result;
    result.root_ = insert(root_, leaf, metadata);
    return result;
}

const SnapshotPageChange *SnapshotChanges::get(uint64_t pid) const noexcept
{
    const auto *node = root_.get();
    while (node != nullptr && node->bit != 0) {
        node = static_cast<const Branch &>(*node).children[(pid & node->bit) != 0 ? 1U : 0U].get();
    }
    return node != nullptr && node->first == pid ? &static_cast<const Leaf &>(*node).change : nullptr;
}

const SnapshotChanges::Leaf *SnapshotChanges::lower_bound(const Owner &node, uint64_t pid) noexcept
{
    if (!node || node->last < pid) {
        return nullptr;
    }
    if (node->bit == 0) {
        return &static_cast<const Leaf &>(*node);
    }
    const auto &branch = static_cast<const Branch &>(*node);
    if (const auto *left = lower_bound(branch.children[0], pid)) {
        return left;
    }
    return lower_bound(branch.children[1], pid);
}

bool SnapshotChanges::visit_from(uint64_t &next_pid, size_t max_pages, void *context, Visitor visitor) const
{
    for (size_t i = 0; i < max_pages; ++i) {
        const auto *leaf = lower_bound(root_, next_pid);
        if (leaf == nullptr) {
            next_pid = kPageLimit;
            return true;
        }
        visitor(context, leaf->change);
        next_pid = leaf->first + 1;
    }
    return !root_ || next_pid > root_->last;
}

size_t SnapshotChanges::size() const noexcept
{
    return root_ ? root_->count : 0;
}

bool SnapshotChanges::empty() const noexcept
{
    return !root_;
}

bool SnapshotChanges::same_root(const SnapshotChanges &other) const noexcept
{
    return root_ == other.root_;
}

size_t SnapshotChanges::change_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<Leaf>() + (30 * detail::shared_allocation_bound<Branch>());
}

std::optional<size_t> SnapshotChanges::initial_allocation_bound(size_t pages) noexcept
{
    return detail::AllocationBound()
        .add_product(pages, detail::shared_allocation_bound<Leaf>() + detail::shared_allocation_bound<Branch>() +
                                sizeof(SnapshotPageChange))
        .value();
}

SnapshotChanges::Owner SnapshotChanges::build(std::span<const SnapshotPageChange>                   changes,
                                              const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    if (changes.size() == 1) {
        return detail::reserved_shared<Leaf>(metadata, changes.front());
    }
    const auto bit    = std::bit_floor(changes.front().page_id ^ changes.back().page_id);
    const auto middle = std::partition_point(changes.begin(), changes.end(),
                                             [bit](const auto &change) { return (change.page_id & bit) == 0; });
    const auto count  = static_cast<size_t>(middle - changes.begin());
    return detail::reserved_shared<Branch>(metadata, bit, build(changes.first(count), metadata),
                                           build(changes.subspan(count), metadata));
}

SnapshotChanges SnapshotChanges::from_sorted(std::span<const SnapshotPageChange>                   changes,
                                             const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    uint64_t next_pid = 0;
    for (const auto &change : changes) {
        if (change.page_id < next_pid || change.page_id >= kPageLimit) {
            throw std::invalid_argument("initial snapshot changes must have unique increasing PIDs");
        }
        next_pid = change.page_id + 1;
    }
    SnapshotChanges result;
    if (!changes.empty()) {
        result.root_ = build(changes, metadata);
    }
    return result;
}
} // namespace crowdb::tree
