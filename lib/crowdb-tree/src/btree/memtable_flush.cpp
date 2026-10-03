// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/btree/delta.h"
#include "crowdb-tree/btree/descent.h"
#include "crowdb-tree/crowdb-tree.h"
#include "merge_sources.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <thread>

namespace crowdb::tree
{
bool Crowdbtree::publish_group_to_leaf_locked(uint64_t page_id, uint64_t cs, std::vector<leaf_entry> group)
{
    PageBase *head = resident(page_id);
    // In-frame delta fast path (PT12, opt-in): if the leaf is a bare base, try a
    // cheap COW-append of this group as in-frame deltas instead of a heap delta
    // node. Falls back to the heap path on no-room; folds at the delta cap.
    if (opt_.inframe_delta && head != nullptr && head->type == page_type::kLeafBase) {
        auto                *leaf  = static_cast<LeafBase *>(head);
        uint32_t             cur   = leaf->view().delta_count();
        uint32_t             after = cur + static_cast<uint32_t>(group.size());
        std::vector<uint8_t> out(leaf->page_bytes());
        if (after <= opt_.max_inframe_delta &&
            leaf_frame_append_deltas(leaf->frame(), leaf->page_bytes(), group, out.data())) {
            LeafBase *fresh = LeafBase::from_frame_copy(out.data(), leaf->page_bytes(), pool_, opt_.frame_bytes);
            store_preserving_parent_locked(page_id, fresh);
            retire_page(leaf);
            if (after >= opt_.max_inframe_delta || fresh->data_bytes() > opt_.leaf_split_bytes) {
                consolidate_locked(page_id);
                return true;
            }
            return false;
        }
    }
    BatchDelta *delta = BatchDelta::build(cs, std::move(group), head);
    store_preserving_parent_locked(page_id, delta);
#ifdef CROWDB_TREE_TEST_UTIL
    if (after_flush_group_for_tests_ != nullptr) {
        after_flush_group_for_tests_();
    }
#endif
    if (delta->delta_len > opt_.max_delta_len || delta->chain_bytes > opt_.max_delta_bytes) {
        consolidate_locked(page_id);
        return true;
    }
    return false;
}

bool Crowdbtree::drain_all_frozen_locked(std::deque<std::shared_ptr<MemTable>> &to_drain, uint64_t cs)
{
    // Phase 1: Open cursors on all frozen memtables for a non-destructive
    // k-way merge read. Entries stay in L0 until covered source detachment, so
    // concurrent scans always see them in L0 or L1, never in neither.
    std::vector<ConcurrentSkipList::Cursor> cursors;
    cursors.reserve(to_drain.size());
    bool has_any = false;
    for (auto &mt : to_drain) {
        cursors.push_back(mt->prefix_cursor(cs, last_applied_slot_.load()));
        if (cursors.back().valid()) {
            has_any = true;
        }
    }
    if (!has_any) {
        return false;
    }

    // Phase 2: K-way merge the cursors with highest-slot-wins dedup,
    // feeding the merged stream to a sort-aware descent + publish loop (O1+O5).
    // Only entries with slot <= cs are emitted; entries with slot > cs are
    // skipped (they remain in L0 for a future flush).
    std::vector<MergeSource> sources;
    sources.reserve(cursors.size());
    for (auto &c : cursors) {
        sources.push_back({.kind = MergeSource::kL0, .l0 = &c, .l1 = nullptr});
    }
    LoserTree lt;
    lt.init(sources.data(), static_cast<int>(sources.size()));

    flush_drain_total_.fetch_add(1, std::memory_order_relaxed);
    if (metrics_.flush_drain_c != nullptr) {
        metrics_.flush_drain_c->inc();
    }

    auto                    resolve = [this](uint64_t p) { return resident(p); };
    uint64_t                page_id = kInvalidPageId;
    Slice                   high_key;
    bool                    have_leaf         = false;
    uint64_t                entries_published = 0;
    std::vector<leaf_entry> group;
    buffer                  materialized_cell;

    while (lt.winner_valid()) {
        int      w    = lt.winner();
        Slice    key  = sources[w].key();
        uint64_t slot = sources[w].slot();

        if (slot > cs) {
            // Not yet contiguous — skip (remains in L0 for a future flush).
            lt.advance_winner();
            continue;
        }

        // Sort-aware descent (O1): reuse the cached leaf when the key is
        // within its range (key <= high_key). Re-descend when crossing a
        // leaf boundary. After a publish that triggers a consolidate (which
        // may split), the cached high_key is stale — the next key > high_key
        // check naturally re-descends.
        if (!have_leaf || key.compare(high_key) > 0) {
            if (!group.empty()) {
                publish_group_to_leaf_locked(page_id, cs, std::move(group));
                group.clear();
            }
            page_id        = find_leaf_page_id(resolve, root_page_id_.load(), key);
            PageBase *head = resident(page_id);
            LeafBase *leaf = chain_leaf_base(head);
            high_key       = leaf != nullptr ? leaf->high_key() : Slice();
            have_leaf      = true;
        }

        // Materialize the winning cell from the cursor's CellVersion.
        const CellVersion *cv = sources[w].l0->cell_version();
        if (cv != nullptr && cv->cell.ownership() != buffer::mode::kExternal) {
            materialized_cell = cv->cell.clone();
        }
        else if (cv != nullptr) {
            size_t vlen       = cv->cell.size();
            materialized_cell = buffer::alloc(vlen, kCellHeaderSize);
            uint8_t *p        = materialized_cell.data();
            for (int i = 0; i < 8; ++i) {
                p[i] = static_cast<uint8_t>((cv->slot >> (8 * i)) & 0xff);
            }
            p[8] = cv->flags;
            if (vlen > 0) {
                std::memcpy(p + kCellHeaderSize, cv->cell.data(), vlen);
            }
        }
        else {
            materialized_cell = buffer::alloc(0, kCellHeaderSize);
        }
        group.push_back({.key = key.to_string(), .cell = std::move(materialized_cell)});
        ++entries_published;
        lt.advance_winner();

        // Collision drain: advance all other sources on the same key
        // (duplicate — highest-slot-wins among <= cs already picked the winner).
        while (lt.winner_valid() && sources[lt.winner()].key().compare(key) == 0) {
            lt.drain_winner();
        }
    }

    // Publish the last pending group.
    if (!group.empty()) {
        publish_group_to_leaf_locked(page_id, cs, std::move(group));
    }

    flush_entries_total_.fetch_add(entries_published, std::memory_order_relaxed);
    if (metrics_.flush_entries_c != nullptr) {
        metrics_.flush_entries_c->inc_by(entries_published);
    }

    return entries_published > 0;
}

Crowdbtree::FlushBoundary Crowdbtree::capture_flush_locked()
{
    FlushBoundary boundary;
    auto         &captured = boundary.tables;
    auto         &frontier = boundary.frontier;
    {
        std::unique_lock catalog(memtable_mutex_);
        // Every allocation precedes closure. Publishing the successor after
        // the bound capture prevents new batches from widening this flush.
        auto successor = std::make_shared<MemTable>(memtable_next_id_.fetch_add(1), &epoch_);
        successor->set_durable_floor(last_applied_slot_.load());
        captured = frozen_;
        captured.insert(captured.end(), split_shared_memtables_.begin(), split_shared_memtables_.end());
        captured.push_back(active_);
        frozen_.push_back(active_);
        active_->close();
        frontier = contiguous_slot_.load(std::memory_order_acquire);
        active_  = std::move(successor);
    }
    return boundary;
}

void Crowdbtree::publish_flush_locked(FlushBoundary &boundary)
{
    auto      &captured = boundary.tables;
    const auto frontier = boundary.frontier;
    for (const auto &table : captured) {
        table->wait_frozen();
    }
    publication_incomplete_ = true;
    drain_all_frozen_locked(captured, frontier);
    {
        std::unique_lock catalog(memtable_mutex_);
        // All prefix publications precede the new catalog floor. Old readers
        // retain immutable sources; future records never move between tables.
        last_applied_slot_.store(frontier, std::memory_order_release);
        active_->set_durable_floor(frontier);
        publication_incomplete_ = false;
        for (auto it = frozen_.begin(); it != frozen_.end();) {
            const auto &table = *it;
            if (std::find(captured.begin(), captured.end(), table) != captured.end() &&
                table->slot_range().max <= frontier) {
                it = frozen_.erase(it);
            }
            else {
                ++it;
            }
        }
    }
}

Status Crowdbtree::flush()
try {
    auto             started = std::chrono::steady_clock::now();
    std::scoped_lock writer(write_mutex_);
    if (split_overlay_source_.load() != nullptr && last_applied_slot_.load() < split_overlay_frontier_.load()) {
        return Status::unavailable("split overlay does not establish published coverage");
    }
    auto guard    = epoch_.enter();
    auto boundary = capture_flush_locked();
    publish_flush_locked(boundary);
    version_.fetch_add(1);
    maybe_evict_locked();
    if (metrics_.flush_l != nullptr) {
        metrics_.flush_l->observe(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started).count()));
    }
    guard = {};
    boundary.tables.clear();
    epoch_.try_reclaim();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("flush allocation failed");
}
catch (const std::exception &error) {
    return Status::internal_error(error.what());
}

void Crowdbtree::flush_async(std::function<void(Status)> on_done)
{
    auto pending    = async_flushes_;
    auto completion = std::make_shared<std::function<void(Status)>>(std::move(on_done));
    pending->fetch_add(1);
    try {
        std::thread([this, pending, completion] {
            Status status;
            try {
                status = flush();
            }
            catch (const std::bad_alloc &) {
                status = Status::resource_exhausted("flush allocation failed");
            }
            catch (const std::exception &error) {
                status = Status::internal_error(error.what());
            }
            pending->fetch_sub(1);
            pending->notify_all();
            (*completion)(status);
        }).detach();
    }
    catch (const std::exception &error) {
        pending->fetch_sub(1);
        pending->notify_all();
        (*completion)(Status::resource_exhausted(error.what()));
    }
}

} // namespace crowdb::tree
