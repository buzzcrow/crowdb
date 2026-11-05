// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "merge_pair_metrics.h"

#include "allocation_bound.h"

#include <algorithm>
#include <cassert>
#include <stdexcept>

namespace crowdb::tree
{
struct MergePairMetrics::Retired
{
    struct Nodes
    {
        Pairs::node_type                pair;
        std::array<Index::node_type, 4> index;
    };

    Retired(const std::shared_ptr<detail::AllocationReservation> &metadata, std::shared_ptr<const void> resource)
        : resource_owner(std::move(resource)),
          nodes(decltype(nodes)::allocator_type(metadata))
    {
    }

    bool retire_step() noexcept
    {
        for (size_t count = 0; count < 16 && !nodes.empty(); ++count) {
            nodes.pop_back();
        }
        return nodes.empty();
    }

    std::shared_ptr<const void>   resource_owner;
    detail::ReservedVector<Nodes> nodes;
};

MergePairMetrics::MergePairMetrics(std::pmr::memory_resource *resource) : pairs_(resource), index_(resource)
{
}

MergePairMetrics::Prepared::Prepared(MergePairMetrics                                     &owner,
                                     const std::shared_ptr<detail::AllocationReservation> &metadata,
                                     std::shared_ptr<const void>                           resource_owner)
    : owner_(owner),
      removed_(decltype(removed_)::allocator_type(metadata)),
      endpoints_(decltype(endpoints_)::allocator_type(metadata)),
      membership_(decltype(membership_)::allocator_type(metadata)),
      added_pairs_(owner.pairs_.get_allocator().resource()),
      added_index_(owner.index_.get_allocator().resource()),
      retired_(detail::reserved_shared<Retired>(metadata, metadata, std::move(resource_owner))),
      next_(owner.totals_)
{
}

void MergePairMetrics::Prepared::invalidate(Kind kind, uint64_t value)
{
    assert(!finished_);
    auto it = owner_.index_.lower_bound({.kind = kind, .value = value, .left = 0});
    while (it != owner_.index_.end() && it->first.kind == kind && it->first.value == value) {
        removed_.push_back(it->first.left);
        ++it;
    }
}

void MergePairMetrics::Prepared::invalidate_page(uint64_t pid)
{
    invalidate(Kind::Endpoint, pid);
}

void MergePairMetrics::Prepared::invalidate_parent(uint64_t pid)
{
    invalidate(Kind::Parent, pid);
}

void MergePairMetrics::Prepared::clear_pair(uint64_t left)
{
    assert(!finished_);
    if (owner_.pairs_.contains(left)) {
        removed_.push_back(left);
    }
}

void MergePairMetrics::Prepared::set_pair(MergePairProof proof, uint64_t now_ns)
{
    assert(!finished_ && added_pairs_.empty());
    if (proof.left == proof.right || proof.parent == proof.left || proof.parent == proof.right || now_ns == 0) {
        throw std::invalid_argument("invalid canonical merge pair");
    }
    // Each right endpoint has at most one predecessor in the live sibling
    // chain. Replacing an outgoing pair retires both old endpoint references.
    const auto before = owner_.pairs_.find(proof.left);
    if (before != owner_.pairs_.end()) {
        auto old  = before->second.proof;
        old.ready = proof.ready;
        if (old == proof) {
            now_ns = before->second.since_ns;
        }
        removed_.push_back(proof.left);
    }
    added_pairs_.emplace(proof.left, Pair{.proof = proof, .since_ns = now_ns});
    added_index_.emplace(Key{.kind = Kind::Endpoint, .value = proof.left, .left = proof.left}, proof.right);
    added_index_.emplace(Key{.kind = Kind::Endpoint, .value = proof.right, .left = proof.left}, proof.right);
    added_index_.emplace(Key{.kind = Kind::Parent, .value = proof.parent, .left = proof.left}, proof.right);
    added_index_.emplace(Key{.kind = Kind::Time, .value = now_ns, .left = proof.left}, proof.right);
}

bool MergePairMetrics::Prepared::removes(uint64_t left) const noexcept
{
    return std::binary_search(removed_.begin(), removed_.end(), left);
}

void MergePairMetrics::Prepared::finish()
{
    assert(!finished_);
    std::sort(removed_.begin(), removed_.end());
    removed_.erase(std::unique(removed_.begin(), removed_.end()), removed_.end());
    retired_->nodes.resize(removed_.size());
    endpoints_.reserve((removed_.size() + added_pairs_.size()) * 2);
    for (const auto left : removed_) {
        const auto &pair = owner_.pairs_.at(left);
        --next_.pairs;
        next_.ready -= static_cast<uint64_t>(pair.proof.ready);
        endpoints_.push_back(pair.proof.left);
        endpoints_.push_back(pair.proof.right);
    }
    for (const auto &[left, pair] : added_pairs_) {
        (void)left;
        ++next_.pairs;
        next_.ready += static_cast<uint64_t>(pair.proof.ready);
        endpoints_.push_back(pair.proof.left);
        endpoints_.push_back(pair.proof.right);
    }
    std::sort(endpoints_.begin(), endpoints_.end());
    endpoints_.erase(std::unique(endpoints_.begin(), endpoints_.end()), endpoints_.end());
    membership_.reserve(endpoints_.size());
    for (const auto pid : endpoints_) {
        bool after  = false;
        bool before = false;
        auto it     = owner_.index_.lower_bound({.kind = Kind::Endpoint, .value = pid, .left = 0});
        while (it != owner_.index_.end() && it->first.kind == Kind::Endpoint && it->first.value == pid) {
            before = true;
            after  = after || !removes(it->first.left);
            ++it;
        }
        auto added = added_index_.lower_bound({.kind = Kind::Endpoint, .value = pid, .left = 0});
        after =
            after || (added != added_index_.end() && added->first.kind == Kind::Endpoint && added->first.value == pid);
        if (before != after) {
            membership_.push_back({.pid = pid, .present = after});
        }
        next_.pages -= static_cast<uint64_t>(before);
        next_.pages += static_cast<uint64_t>(after);
    }
    next_.oldest_since_ns = 0;
    auto time             = owner_.index_.lower_bound({.kind = Kind::Time, .value = 0, .left = 0});
    while (time != owner_.index_.end() && removes(time->first.left)) {
        ++time;
    }
    if (time != owner_.index_.end()) {
        next_.oldest_since_ns = time->first.value;
    }
    for (const auto &[left, pair] : added_pairs_) {
        (void)left;
        if (next_.oldest_since_ns == 0 || pair.since_ns < next_.oldest_since_ns) {
            next_.oldest_since_ns = pair.since_ns;
        }
    }
    finished_ = true;
}

std::span<const MergePairMetrics::Membership> MergePairMetrics::Prepared::membership_changes() const noexcept
{
    assert(finished_);
    return membership_;
}

std::span<const uint64_t> MergePairMetrics::Prepared::affected_pages() const noexcept
{
    assert(finished_);
    return endpoints_;
}

std::optional<MergePairProof> MergePairMetrics::Prepared::pair(uint64_t left) const noexcept
{
    assert(finished_);
    const auto found = added_pairs_.find(left);
    if (found != added_pairs_.end()) {
        return found->second.proof;
    }
    return removes(left) ? std::nullopt : owner_.pair(left);
}

bool MergePairMetrics::Prepared::contains_page(uint64_t pid) const noexcept
{
    assert(finished_);
    const auto found = std::lower_bound(membership_.begin(), membership_.end(), pid,
                                        [](const Membership &entry, uint64_t value) { return entry.pid < value; });
    return found != membership_.end() && found->pid == pid ? found->present : owner_.contains_page(pid);
}

MergePairMetrics::Snapshot MergePairMetrics::Prepared::snapshot() const noexcept
{
    assert(finished_);
    return next_;
}

std::shared_ptr<const void> MergePairMetrics::Prepared::commit() noexcept
{
    assert(finished_ && !committed_);
    committed_      = true;
    size_t position = 0;
    for (const auto left : removed_) {
        const auto found = owner_.pairs_.find(left);
        assert(found != owner_.pairs_.end());
        const auto pair  = found->second;
        auto      &nodes = retired_->nodes[position++];
        nodes.pair       = owner_.pairs_.extract(found);
        nodes.index      = {owner_.index_.extract({.kind = Kind::Endpoint, .value = pair.proof.left, .left = left}),
                            owner_.index_.extract({.kind = Kind::Endpoint, .value = pair.proof.right, .left = left}),
                            owner_.index_.extract({.kind = Kind::Parent, .value = pair.proof.parent, .left = left}),
                            owner_.index_.extract({.kind = Kind::Time, .value = pair.since_ns, .left = left})};
    }
    while (!added_pairs_.empty()) {
        owner_.pairs_.insert(added_pairs_.extract(added_pairs_.begin()));
    }
    while (!added_index_.empty()) {
        owner_.index_.insert(added_index_.extract(added_index_.begin()));
    }
    owner_.totals_ = next_;
    return std::move(retired_);
}

MergePairMetrics::Snapshot MergePairMetrics::snapshot() const noexcept
{
    return totals_;
}

std::optional<MergePairProof> MergePairMetrics::pair(uint64_t left) const noexcept
{
    const auto found = pairs_.find(left);
    return found == pairs_.end() ? std::nullopt : std::optional(found->second.proof);
}

bool MergePairMetrics::contains_page(uint64_t pid) const noexcept
{
    const auto found = index_.lower_bound({.kind = Kind::Endpoint, .value = pid, .left = 0});
    return found != index_.end() && found->first.kind == Kind::Endpoint && found->first.value == pid;
}

bool MergePairMetrics::consistent() const noexcept
{
    Snapshot actual;
    uint64_t last = UINT64_MAX;
    for (const auto &[key, value] : index_) {
        const auto found = pairs_.find(key.left);
        if (found == pairs_.end() || found->second.proof.right != value) {
            return false;
        }
        if (key.kind == Kind::Endpoint && key.value != last) {
            ++actual.pages;
            last = key.value;
        }
    }
    for (const auto &[left, pair] : pairs_) {
        ++actual.pairs;
        actual.ready += static_cast<uint64_t>(pair.proof.ready);
        if (actual.oldest_since_ns == 0 || pair.since_ns < actual.oldest_since_ns) {
            actual.oldest_since_ns = pair.since_ns;
        }
        for (const auto key : {
                 Key{.kind = Kind::Endpoint, .value = pair.proof.left,   .left = left},
                 Key{.kind = Kind::Endpoint, .value = pair.proof.right,  .left = left},
                 Key{.kind = Kind::Parent,   .value = pair.proof.parent, .left = left},
                 Key{.kind = Kind::Time,     .value = pair.since_ns,     .left = left}
        }) {
            if (!index_.contains(key)) {
                return false;
            }
        }
    }
    return actual == totals_ && index_.size() == pairs_.size() * 4;
}

bool MergePairMetrics::retire_step() noexcept
{
    for (size_t n = 0; n < 64 && !index_.empty(); ++n) {
        index_.erase(index_.begin());
    }
    for (size_t n = 0; n < 16 && !pairs_.empty(); ++n) {
        pairs_.erase(pairs_.begin());
    }
    return index_.empty() && pairs_.empty();
}

size_t MergePairMetrics::invalidation_bytes(size_t pairs) noexcept
{
    return detail::AllocationBound(detail::shared_allocation_bound<Retired>())
        .add_product(pairs, sizeof(Retired::Nodes) + 192)
        .value()
        .value_or(SIZE_MAX);
}

size_t MergePairMetrics::insertion_bytes() noexcept
{
    // Five red-black nodes, their allocation headers and staging arrays.
    return invalidation_bytes(1) + 1536;
}
} // namespace crowdb::tree
