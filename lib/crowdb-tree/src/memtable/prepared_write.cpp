// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/memtable/skip_list.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <exception>
#include <stdexcept>

namespace crowdb::tree
{
namespace
{
void append_prepared(VersionSet &versions, const std::shared_ptr<const CellVersion> &version) noexcept
{
    // Never let an invalid admission bound turn commit into vector growth.
    // shared_ptr copy itself cannot throw once capacity is already present.
    if (versions.history.size() == versions.history.capacity()) {
        std::terminate();
    }
    try {
        versions.history.push_back(version);
    }
    catch (...) {
        std::terminate();
    }
}

void merge_prepared(VersionSet &fresh, const VersionSet &old, const std::shared_ptr<const CellVersion> &incoming,
                    uint64_t bound) noexcept
{
    assert(fresh.history.capacity() > old.history.size());
    fresh.history.clear();
    fresh.bound       = std::max(bound, old.bound);
    fresh.current     = incoming->slot > old.current->slot ? incoming : old.current;
    bool       anchor = fresh.current->slot <= fresh.bound;
    const auto retain = [&](const auto &version) {
        if (version == fresh.current) {
            return;
        }
        if (version->slot <= fresh.bound) {
            if (anchor) {
                return;
            }
            anchor = true;
        }
        append_prepared(fresh, version);
    };
    bool       pending = incoming != fresh.current;
    const auto visit   = [&](const auto &version) {
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
    fresh.bytes = fresh.current->cell.size();
    for (const auto &version : fresh.history) {
        fresh.bytes += version->cell.size();
    }
}
} // namespace

void ConcurrentSkipList::PreparedWrite::NodeDeleter::operator()(Node *node) const noexcept
{
    if (node != nullptr) {
        ConcurrentSkipList::free_node(node);
    }
}

ConcurrentSkipList::PreparedWrite ConcurrentSkipList::prepare_write(Slice key, std::shared_ptr<CellVersion> cell,
                                                                    uint64_t bound, size_t max_versions,
                                                                    const std::shared_ptr<AllocationBudget> &budget)
{
    auto result             = prepare_reference(key, std::move(cell), bound, max_versions, budget);
    result.initialize_slot_ = true;
    return result;
}

ConcurrentSkipList::PreparedWrite ConcurrentSkipList::prepare_reference(Slice                              key,
                                                                        std::shared_ptr<const CellVersion> cell,
                                                                        uint64_t bound, size_t max_versions,
                                                                        const std::shared_ptr<AllocationBudget> &budget)
{
    if (max_versions == 0 || !cell || epoch_->source_sealed()) {
        throw std::invalid_argument("prepared write needs an admitted version bound");
    }
#ifdef CROWDB_TREE_TEST_UTIL
    if (test_hook_ != nullptr) {
        test_hook_(test_context_, PausePoint::kBeforePrepare, key);
    }
#endif
    PreparedWrite result;
    result.initialize_slot_  = false;
    result.guard_            = epoch_->enter(budget);
    result.cell_             = std::move(cell);
    result.bound_            = bound;
    result.initial_          = VersionSet::create(budget);
    result.initial_->current = result.cell_;
    result.initial_->bound   = bound;
    result.initial_->bytes   = result.cell_->cell.size();
    result.initial_->allocation.set(epoch_->memtable_allocation(), sizeof(VersionSet));
    result.replacement_ = VersionSet::create(budget);
    result.replacement_->history.reserve(max_versions);
    result.replacement_->allocation.set(
        epoch_->memtable_allocation(),
        sizeof(VersionSet) + (result.replacement_->history.capacity() * sizeof(result.replacement_->current)));
    result.candidate_.reset(alloc_node(random_height(), key, budget));
    result.candidate_->allocation.set(epoch_->memtable_allocation(),
                                      Node::alloc_size(result.candidate_->height_, key.size()));
    result.candidate_->versions_.store(result.initial_.get(), std::memory_order_relaxed);
    return result;
}

bool ConcurrentSkipList::commit_write(PreparedWrite write, uint64_t slot) noexcept
{
    assert(write.cell_ && write.candidate_);
    if (epoch_->source_sealed()) {
        return false;
    }
    if (write.initialize_slot_) {
        // This is the parent's private, not-yet-published cell. Referenced
        // delta writes observe the completed parent CAS and never mutate it.
        auto &cell = *std::const_pointer_cast<CellVersion>(write.cell_);
        cell.slot  = slot;
        if (cell.cell.ownership() != buffer::mode::kExternal) {
            for (size_t index = 0; index < sizeof(slot); ++index) {
                cell.cell.data()[index] = static_cast<uint8_t>(slot >> (8 * index));
            }
        }
    }
    else {
        assert(write.cell_->slot == slot);
    }
    const auto                     key = write.candidate_->key_slice();
    std::array<Node *, kMaxHeight> previous{};
    std::array<Node *, kMaxHeight> next{};
    Node                          *existing = nullptr;
#ifdef CROWDB_TREE_TEST_UTIL
    if (test_hook_ != nullptr) {
        test_hook_(test_context_, PausePoint::kBeforeSearch, key);
    }
#endif
    for (;;) {
        find_ge(key, previous.data(), next.data());
        if (next[0] != nullptr && next[0]->key_slice() == key) {
            existing = next[0];
            break;
        }
        write.candidate_->set_next(0, next[0]);
        if (previous[0]->next_ptr(0)->compare_exchange_strong(next[0], write.candidate_.get(),
                                                              std::memory_order_acq_rel)) {
            auto *published = write.candidate_.release();
            auto *initial   = write.initial_.release();
#ifdef CROWDB_TREE_TEST_UTIL
            if (test_hook_ != nullptr) {
                test_hook_(test_context_, PausePoint::kBeforeInsertAccounting, key);
            }
#endif
            count_.fetch_add(1, std::memory_order_relaxed);
            bytes_.fetch_add(key.size() + initial->bytes, std::memory_order_relaxed);
            account_node(*published, *initial);
#ifdef CROWDB_TREE_TEST_UTIL
            if (test_hook_ != nullptr) {
                test_hook_(test_context_, PausePoint::kAfterLevelZero, key);
            }
#endif
            link_upper(published);
            return true;
        }
    }
    auto                                    *old      = existing->versions_.load(std::memory_order_acquire);
    const std::shared_ptr<const CellVersion> incoming = write.cell_;
    for (;;) {
        const auto *same = old->at(slot);
        if (same != nullptr && same->slot == slot) {
            return false;
        }
        merge_prepared(*write.replacement_, *old, incoming, write.bound_);
        const bool accepted = write.replacement_->at(slot) == incoming.get();
        if (!accepted && write.replacement_->history.size() == old->history.size() &&
            write.replacement_->bound == old->bound) {
            return false;
        }
        auto *replacement = write.replacement_.get();
#ifdef CROWDB_TREE_TEST_UTIL
        if (test_hook_ != nullptr) {
            test_hook_(test_context_, PausePoint::kBeforeVersionCas, key);
        }
#endif
        if (existing->versions_.compare_exchange_weak(old, replacement, std::memory_order_acq_rel)) {
            [[maybe_unused]] auto *published = write.replacement_.release();
            bytes_.fetch_add(replacement->bytes - old->bytes, std::memory_order_relaxed);
            account_versions(old, *replacement);
            epoch_->defer(old);
            return accepted;
        }
    }
}
} // namespace crowdb::tree
