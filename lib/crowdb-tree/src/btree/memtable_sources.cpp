// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/log.h"
#include "crowdb-tree/crowdb-tree.h"

namespace crowdb::tree
{
std::shared_ptr<MemTable> Crowdbtree::current_active() const
{
    std::shared_lock<std::shared_mutex> lk(memtable_mutex_);
    return active_;
}

std::vector<MemTableSource> Crowdbtree::all_memtables() const
{
    auto out = local_memtables();
    if (Crowdbtree *source = split_overlay_source_.load(std::memory_order_acquire); source != nullptr) {
        // Parent flush coverage does not establish coverage in the child.
        auto inherited = source->local_memtables(last_applied_slot_.load());
        out.insert(out.end(), inherited.begin(), inherited.end());
    }
    return out;
}

std::vector<MemTableSource> Crowdbtree::local_memtables(uint64_t covered) const
{
    std::shared_lock<std::shared_mutex> lk(memtable_mutex_);
    std::vector<MemTableSource>         out;
    const auto floor = covered == UINT64_MAX ? last_applied_slot_.load(std::memory_order_acquire) : covered;
    out.reserve(split_shared_memtables_.size() + frozen_.size() + 1);
    for (const auto &mt : split_shared_memtables_) {
        out.emplace_back(mt, floor);
    }
    for (const auto &mt : frozen_) {
        out.emplace_back(mt, floor);
    }
    out.emplace_back(active_, floor);
    return out;
}

bool Crowdbtree::maybe_freeze_active(bool force)
{
    std::shared_ptr<MemTable> active = current_active();
    if (!force && active->approx_bytes() < opt_.memtable_flush_bytes && active->count() < opt_.memtable_flush_entries) {
        return false;
    }
    std::unique_lock<std::shared_mutex> lk(memtable_mutex_);
    // Re-check under the exclusive lock: another thread may have already
    // frozen this exact active_ (or installed a fresh, still-small one)
    // between the check above and taking the lock.
    if (active_ != active || (active_->empty() && active_->writers() == 0)) {
        return false;
    }
    if (!force) {
        size_t max_frozen = opt_.max_memtable_count > 1 ? static_cast<size_t>(opt_.max_memtable_count) - 1 : 1;
        if (frozen_.size() >= max_frozen) {
            if (active_->mark_backlog_warning()) {
                CRB_LOG_WARN("[{}] memtable backlog: frozen={} active_bytes={} contiguous={} published={}; "
                             "active remains writable, inspect stalled writers/readers or slot gaps",
                             name_, frozen_.size(), active_->approx_bytes(), contiguous_slot_.load(),
                             last_applied_slot_.load());
            }
            // At capacity: no free buffer slot. Let active_ keep growing past
            // its threshold rather than stall the writer -- an explicit
            // flush()/the background thread is expected to drain a slot free
            // (documented in Config::max_memtable_count).
            return false;
        }
    }
    auto successor = std::make_shared<MemTable>(memtable_next_id_.fetch_add(1, std::memory_order_relaxed), &epoch_);
    successor->set_durable_floor(last_applied_slot_.load());
    frozen_.push_back(active_);
    active_->close();
    active_ = std::move(successor);
    if (metrics_.mt_freeze_c != nullptr) {
        metrics_.mt_freeze_c->inc();
    }
    // Propagate the known-durable floor to the fresh table immediately (not
    // just on its first flush()) so a stale re-apply landing in it before
    // its own first drain is still correctly rejected.
    active_->set_durable_floor(last_applied_slot_.load());
    return true;
}

void Crowdbtree::maybe_swap_active()
{
    try {
        maybe_freeze_active(/*force=*/false);
    }
    catch (const std::bad_alloc &) {
        // Soft rotation is optional; the batch is already applied. All
        // allocations precede closure, so the active table remains usable.
        return;
    }
}

size_t Crowdbtree::memtable_count() const
{
    size_t n = 0;
    for (auto &mt : all_memtables()) {
        n += mt->count();
    }
    return n;
}

size_t Crowdbtree::frozen_table_count() const
{
    std::shared_lock<std::shared_mutex> lk(memtable_mutex_);
    return frozen_.size();
}

} // namespace crowdb::tree
