// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/btree/delta.h"
#include "crowdb-tree/btree/descent.h"
#include "crowdb-tree/crowdb-tree.h"

#include <algorithm>
#include <chrono>

namespace crowdb::tree
{
Status Crowdbtree::install_split_memtable_overlay(Crowdbtree &source, uint64_t journal_frontier)
try {
    if (&source == this) {
        return Status::invalid_argument("split memtable overlay source must differ from destination");
    }
    Crowdbtree *expected = nullptr;
    if (!split_overlay_source_.compare_exchange_strong(expected, &source, std::memory_order_acq_rel) &&
        expected != &source) {
        return Status::invalid_argument("another split memtable overlay is already installed");
    }
    split_overlay_frontier_.store(journal_frontier);
    force_advance_slot(journal_frontier);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("operation allocation failed");
}
catch (const std::exception &error) {
    return Status::internal_error(error.what());
}

Status Crowdbtree::clear_split_memtable_overlay(Crowdbtree &source)
{
    if (last_applied_slot_.load() < split_overlay_frontier_.load()) {
        return Status::unavailable("split prefix publication is incomplete");
    }
    Crowdbtree *expected = &source;
    if (!split_overlay_source_.compare_exchange_strong(expected, nullptr, std::memory_order_acq_rel)) {
        if (expected == nullptr) {
            return Status::Ok();
        }
        return Status::invalid_argument("split memtable overlay source does not match");
    }
    return Status::Ok();
}

Status Crowdbtree::begin_split_memtable_view(uint64_t *out_generation, uint64_t *out_journal_frontier)
try {
    const auto started = std::chrono::steady_clock::now();
    if (out_generation == nullptr || out_journal_frontier == nullptr) {
        return Status::invalid_argument("split memtable view requires output parameters");
    }
    std::scoped_lock write_lk(write_mutex_);
    std::unique_lock memtable_lk(memtable_mutex_);
    if (!split_shared_memtables_.empty()) {
        return Status::invalid_argument("a split memtable view is already active");
    }
    auto successor = std::make_shared<MemTable>(memtable_next_id_.fetch_add(1), &epoch_);
    successor->set_durable_floor(last_applied_slot_.load());
    split_shared_memtables_.reserve(frozen_.size() + 1);
    split_shared_memtables_.insert(split_shared_memtables_.end(), frozen_.begin(), frozen_.end());
    frozen_.clear();
    split_shared_memtables_.push_back(active_);
    active_->close();
    const auto frontier      = contiguous_slot_.load(std::memory_order_acquire);
    split_memtable_frontier_ = frontier;
    active_                  = std::move(successor);
    ++split_memtable_generation_;
    *out_generation       = split_memtable_generation_;
    *out_journal_frontier = frontier;
    memtable_lk.unlock();
    for (const auto &table : split_shared_memtables_) {
        table->wait_frozen();
    }
    if (metrics_.split_view_begin_l != nullptr) {
        metrics_.split_view_begin_l->observe(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started).count()));
    }
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("operation allocation failed");
}
catch (const std::exception &error) {
    return Status::internal_error(error.what());
}

Status Crowdbtree::release_split_memtable_view(uint64_t generation)
try {
    const auto       started = std::chrono::steady_clock::now();
    std::scoped_lock write_lk(write_mutex_);
    std::unique_lock memtable_lk(memtable_mutex_);
    if (generation == 0 || generation != split_memtable_generation_ || split_shared_memtables_.empty()) {
        return Status::invalid_argument("split memtable view generation is not active");
    }
    auto pending = frozen_;
    for (const auto &table : split_shared_memtables_) {
        if (table->slot_range().max > last_applied_slot_.load()) {
            pending.push_back(table);
        }
    }
    frozen_.swap(pending);
    split_shared_memtables_.clear();
    if (metrics_.split_view_release_l != nullptr) {
        metrics_.split_view_release_l->observe(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started).count()));
    }
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("operation allocation failed");
}
catch (const std::exception &error) {
    return Status::internal_error(error.what());
}

Status Crowdbtree::publish_split_memtable_view(uint64_t generation, uint64_t journal_frontier, Crowdbtree &destination,
                                               const KeyRange &range)
try {
    const auto started = std::chrono::steady_clock::now();
    if (&destination == this) {
        return Status::invalid_argument("split memtable destination must differ from its source");
    }
    Status range_status = range.validate();
    if (!range_status.ok()) {
        return range_status;
    }
    std::scoped_lock       write_lk(write_mutex_, destination.write_mutex_);
    std::vector<mem_entry> entries;
    {
        std::shared_lock memtable_lk(memtable_mutex_);
        if (generation == 0 || generation != split_memtable_generation_ || split_shared_memtables_.empty()) {
            return Status::invalid_argument("split memtable view generation is not active");
        }
        if (journal_frontier > split_memtable_frontier_) {
            return Status::invalid_argument("split publication exceeds its captured frontier");
        }
        for (const auto &table : split_shared_memtables_) {
            auto snapshot = table->snapshot(journal_frontier);
            entries.insert(entries.end(), std::make_move_iterator(snapshot.begin()),
                           std::make_move_iterator(snapshot.end()));
        }
    }
    std::sort(entries.begin(), entries.end(), [](const mem_entry &left, const mem_entry &right) {
        return left.key == right.key ? left.slot > right.slot : left.key < right.key;
    });

    // The retained parent keeps appending while the view is captured in the
    // background. Publish only the inherited prefix, without crediting later
    // parent records to the child's independent sequence namespace.
    const uint64_t          cs      = journal_frontier;
    uint64_t                page_id = kInvalidPageId;
    Slice                   high_key;
    bool                    have_leaf = false;
    std::vector<leaf_entry> group;
    if (destination.last_applied_slot_.load() > cs) {
        return Status::invalid_argument("split prefix precedes destination publication");
    }
    destination.publication_incomplete_ = true;
    for (size_t index = 0; index < entries.size();) {
        mem_entry        &entry = entries[index];
        const std::string key   = entry.key;
        ++index;
        while (index < entries.size() && entries[index].key == key) {
            ++index;
        }
        if (entry.slot > cs || !range.contains(Slice(key))) {
            continue;
        }
        Slice key_slice(key);
        // An empty high key is the rightmost leaf's +infinity sentinel.  It
        // must retain the rest of this bulk publish as one leaf group; treating
        // it as an ordinary empty key would re-find and publish every entry
        // separately.
        if (!have_leaf || (!high_key.empty() && key_slice.compare(high_key) > 0)) {
            if (!group.empty()) {
                destination.publish_group_to_leaf_locked(page_id, cs, std::move(group));
                group.clear();
            }
            page_id    = find_leaf_page_id([&destination](uint64_t page) { return destination.resident(page); },
                                           destination.root_page_id_.load(), key_slice);
            auto *head = destination.resident(page_id);
            auto *leaf = chain_leaf_base(head);
            high_key   = leaf != nullptr ? leaf->high_key() : Slice();
            have_leaf  = true;
        }
        group.push_back({.key = key, .cell = std::move(entry.cell)});
    }
    if (!group.empty()) {
        destination.publish_group_to_leaf_locked(page_id, cs, std::move(group));
    }
    destination.force_advance_slot(cs);
    {
        std::unique_lock catalog(destination.memtable_mutex_);
        destination.last_applied_slot_.store(cs);
        destination.active_->set_durable_floor(cs);
        destination.publication_incomplete_ = false;
    }
    destination.version_.fetch_add(1);
    if (metrics_.split_view_publish_l != nullptr) {
        metrics_.split_view_publish_l->observe(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started).count()));
    }
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("operation allocation failed");
}
catch (const std::exception &error) {
    return Status::internal_error(error.what());
}

} // namespace crowdb::tree
