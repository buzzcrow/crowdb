// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "btree/retirement_queue.h"
#include "crowdb-tree/memtable/skip_list.h"

#include <algorithm>
#include <chrono>
#include <memory>

namespace crowdb::tree
{
VersionSet::VersionSet(AllocationLease credit, std::shared_ptr<AllocationBudget> budget)
    : descriptor_credit(std::move(credit)),
      retirement(budget ? budget->retirement_queue() : nullptr),
      history(MetadataAllocator<std::shared_ptr<const CellVersion>>(budget))
{
    destroy           = [](EpochManager::Deferred *entry) noexcept { Deleter{}(static_cast<VersionSet *>(entry)); };
    retirement_budget = std::move(budget);
    dispose           = [](detail::RetirementNode *entry) noexcept {
        auto *versions = static_cast<VersionSet *>(entry);
        for (size_t i = 0; i < 64 && !versions->history.empty(); ++i) {
            versions->history.pop_back();
        }
        if (!versions->history.empty()) {
            return false;
        }
        auto credit = std::move(versions->descriptor_credit);
        delete versions;
        return true;
    };
}

void VersionSet::Deleter::operator()(VersionSet *versions) const noexcept
{
    if (versions != nullptr && versions->retirement) {
        versions->retirement->retire(versions);
    }
    else if (versions != nullptr) {
        auto credit = std::move(versions->descriptor_credit);
        delete versions;
    }
}

VersionSet::Owner VersionSet::create(const std::shared_ptr<AllocationBudget> &budget)
{
    AllocationLease credit(budget, sizeof(VersionSet), AllocationKind::kMetadata);
    auto            result = Owner(new VersionSet(std::move(credit), budget));
    return result;
}

const CellVersion *VersionSet::at(uint64_t frontier) const
{
    const auto *owner = owner_at(frontier);
    return owner != nullptr ? owner->get() : nullptr;
}

const std::shared_ptr<const CellVersion> *VersionSet::owner_at(uint64_t frontier) const
{
    if (current->slot <= frontier) {
        return &current;
    }
    for (const auto &version : history) {
        if (version->slot <= frontier) {
            return &version;
        }
    }
    return nullptr;
}

namespace
{
struct CopyTimer
{
    using Clock = std::chrono::steady_clock;
    MutationStats    *stats;
    Clock::time_point started;

    explicit CopyTimer(MutationStats *sample)
        : stats(sample != nullptr && sample->measure_copy ? sample : nullptr),
          started(stats != nullptr ? Clock::now() : Clock::time_point{})
    {
    }

    ~CopyTimer()
    {
        if (stats != nullptr) {
            const auto ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started).count());
            ++stats->copy_count;
            stats->copy_ns += ns;
            stats->copy_max_ns = std::max(stats->copy_max_ns, ns);
        }
    }
};

VersionSet::Owner merged_versions(const VersionSet &old, const std::shared_ptr<const CellVersion> &incoming,
                                  uint64_t bound, MutationStats *stats, const std::shared_ptr<AllocationBudget> &budget)
{
    CopyTimer timer(stats);
    auto      fresh = VersionSet::create(budget);
    fresh->bound    = std::max(bound, old.bound);
    fresh->current  = incoming != nullptr && incoming->slot > old.current->slot ? incoming : old.current;
    bool anchor     = fresh->current->slot <= fresh->bound;
    auto retain     = [&](const auto &version) {
        if (version == fresh->current) {
            return;
        }
        if (version->slot <= fresh->bound) {
            if (anchor) {
                return;
            }
            anchor = true;
        }
        if (fresh->history.empty()) {
            // The immutable input bounds this candidate. Keep common one-cell
            // descriptors allocation-free, and allocate one history capacity
            // once a second retained version is actually needed.
            fresh->history.reserve(old.history.size() + 1);
        }
        fresh->history.push_back(version);
    };
    bool pending = incoming != nullptr && incoming != fresh->current;
    auto visit   = [&](const auto &version) {
        if (pending && incoming->slot > version->slot) {
            retain(incoming);
            pending = false;
        }
        retain(version);
    };
    visit(old.current);
    for (const auto &version : old.history) {
        visit(version);
    }
    if (pending) {
        retain(incoming);
    }
    fresh->bytes = fresh->current->cell.size();
    for (const auto &v : fresh->history) {
        fresh->bytes += v->cell.size();
    }
    return fresh;
}
} // namespace

bool ConcurrentSkipList::replace(Node *node, std::shared_ptr<const CellVersion> cv, uint64_t bound,
                                 MutationStats *stats, const std::shared_ptr<AllocationBudget> &budget)
{
    auto *old = node->versions_.load(std::memory_order_acquire);
    for (;;) {
        if (cv != nullptr) {
            const auto *same = old->at(cv->slot);
            if (same != nullptr && same->slot == cv->slot) {
                return false;
            }
        }
        auto       fresh    = merged_versions(*old, cv, bound, stats, budget);
        const bool accepted = cv != nullptr && fresh->at(cv->slot) == cv.get();
        if (!accepted && fresh->history.size() == old->history.size() && fresh->bound == old->bound) {
            return false;
        }
        fresh->allocation.set(epoch_->memtable_allocation(),
                              sizeof(VersionSet) + (fresh->history.capacity() * sizeof(fresh->current)));
        auto *published = fresh.get();
#ifdef CROWDB_TREE_TEST_UTIL
        if (test_hook_ != nullptr) {
            test_hook_(test_context_, PausePoint::kBeforeVersionCas, node->key_slice());
        }
#endif
        if (!node->versions_.compare_exchange_weak(old, published, std::memory_order_acq_rel)) {
            if (stats != nullptr) {
                ++stats->cas_retries;
            }
            continue;
        }
        published = fresh.release();
        // Unsigned add/sub deltas commute even when a later CAS accounts first.
        bytes_.fetch_add(published->bytes - old->bytes, std::memory_order_relaxed);
        account_versions(old, *published);
        if (stats != nullptr) {
            stats->overwrite += static_cast<uint64_t>(accepted && cv->slot > old->current->slot);
            const auto retained_incoming = static_cast<size_t>(accepted && cv->slot < published->current->slot);
            const auto retained_previous = static_cast<size_t>(published->current != old->current &&
                                                               published->at(old->current->slot) == old->current.get());
            const auto removed =
                old->history.size() + retained_incoming + retained_previous - published->history.size();
            stats->merged += removed;
            if (accepted) {
                const bool kept =
                    cv->slot < published->current->slot || published->at(old->current->slot) == old->current.get();
                if (kept) {
                    ++stats->keep;
                    const bool closed =
                        stats->admission != nullptr && (stats->admission->load(std::memory_order_acquire) >> 63) != 0;
                    if (closed) {
                        ++stats->keep_boundary;
                    }
                    else if (stats->gap) {
                        ++stats->keep_gap;
                    }
                    else {
                        ++stats->keep_pending;
                    }
                }
            }
        }
        epoch_->defer(old);
        return accepted;
    }
}

bool ConcurrentSkipList::upsert(Slice key, std::shared_ptr<const CellVersion> cv, uint64_t bound, MutationStats *stats,
                                std::shared_ptr<AllocationBudget> budget)
{
    if (epoch_->source_sealed()) {
        return false;
    }
    if (!budget) {
        budget = budget_;
    }
    auto  guard    = epoch_->enter(budget);
    auto *existing = find_ge(key, nullptr);
    if (existing != nullptr && existing->key_slice().compare(key) == 0) {
        return replace(existing, std::move(cv), bound, stats, budget);
    }
    auto initial     = VersionSet::create(budget);
    initial->current = cv;
    initial->bound   = bound;
    initial->bytes   = cv->cell.size();
    initial->allocation.set(epoch_->memtable_allocation(), sizeof(VersionSet));
    bool  inserted = false;
    auto *node     = find_or_insert(key, initial.get(), &inserted);
    if (inserted) {
        [[maybe_unused]] auto *published = initial.release();
        return true;
    }
    return replace(node, std::move(cv), bound, stats, budget);
}

void ConcurrentSkipList::prune(Slice key, uint64_t bound, MutationStats *stats)
{
    if (epoch_->source_sealed()) {
        return;
    }
    auto  guard = epoch_->enter();
    auto *node  = find_ge(key, nullptr);
    if (node != nullptr && node->key_slice().compare(key) == 0) {
        replace(node, nullptr, bound, stats, budget_);
    }
}

#ifdef CROWDB_TREE_TEST_UTIL
VersionMemory ConcurrentSkipList::scan_memory_for_tests() const
{
    auto          guard = epoch_->enter();
    VersionMemory result;
    for (auto *node = head_->next(0); node != nullptr; node = node->next(0)) {
        auto *versions = node->versions_.load(std::memory_order_acquire);
        result.history_count += versions->history.size();
        result.payload_bytes += versions->bytes + ((versions->history.size() + 1) * sizeof(CellVersion));
        for (const auto &version : versions->history) {
            result.history_bytes += version->cell.size();
        }
        result.descriptor_bytes += sizeof(VersionSet) + (versions->history.capacity() * sizeof(versions->current));
        result.node_bytes += Node::alloc_size(node->height_, node->key_len_);
    }
    return result;
}
#endif

} // namespace crowdb::tree
