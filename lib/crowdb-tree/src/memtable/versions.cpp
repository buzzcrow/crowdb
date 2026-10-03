// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/memtable/skip_list.h"

#include <algorithm>
#include <chrono>
#include <memory>

namespace crowdb::tree
{
VersionSet::VersionSet()
{
    destroy = [](EpochManager::Deferred *entry) noexcept { delete static_cast<VersionSet *>(entry); };
}

const CellVersion *VersionSet::at(uint64_t frontier) const
{
    if (current->slot <= frontier) {
        return current.get();
    }
    for (const auto &version : history) {
        if (version->slot <= frontier) {
            return version.get();
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

std::unique_ptr<VersionSet> merged_versions(const VersionSet &old, const std::shared_ptr<const CellVersion> &incoming,
                                            uint64_t bound, MutationStats *stats)
{
    CopyTimer timer(stats);
    auto      fresh = std::make_unique<VersionSet>();
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
                                 MutationStats *stats)
{
    auto *old = node->versions_.load(std::memory_order_acquire);
    for (;;) {
        if (cv != nullptr) {
            const auto *same = old->at(cv->slot);
            if (same != nullptr && same->slot == cv->slot) {
                return false;
            }
        }
        auto       fresh    = merged_versions(*old, cv, bound, stats);
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

bool ConcurrentSkipList::upsert(Slice key, std::shared_ptr<const CellVersion> cv, uint64_t bound, MutationStats *stats)
{
    auto  guard    = epoch_->enter();
    auto *existing = find_ge(key, nullptr);
    if (existing != nullptr && existing->key_slice().compare(key) == 0) {
        return replace(existing, std::move(cv), bound, stats);
    }
    auto initial     = std::make_unique<VersionSet>();
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
    return replace(node, std::move(cv), bound, stats);
}

void ConcurrentSkipList::prune(Slice key, uint64_t bound, MutationStats *stats)
{
    auto  guard = epoch_->enter();
    auto *node  = find_ge(key, nullptr);
    if (node != nullptr && node->key_slice().compare(key) == 0) {
        replace(node, nullptr, bound, stats);
    }
}

VersionMemory ConcurrentSkipList::memory() const
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
} // namespace crowdb::tree
