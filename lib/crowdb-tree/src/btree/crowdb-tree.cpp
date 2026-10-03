// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// B+tree engine implementation.

#include "crowdb-tree/crowdb-tree.h"

#include "async_completion_adapter.h"
#include "crowdb-common/log.h"
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/btree/delta.h"
#include "crowdb-tree/btree/descent.h"
#include "crowdb-tree/btree/leaf_cursor.h"
#include "crowdb-tree/maptable/compressor.h"
#include "crowdb-tree/maptable/mapping_slot.h"
#include "merge_sources.h"
#include "native_frames.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace crowdb::tree
{
using detail::NativeBounds;
using detail::set_native_frame_fences;
using detail::validate_native_snapshot_graph;

namespace
{

// Copy a byte range into a fresh owned cell buffer (SBO-inline for small cells).
buffer cell_of(Slice s)
{
    buffer b = buffer::alloc(s.size());
    if (!s.empty()) {
        std::memcpy(b.data(), s.data(), s.size());
    }
    return b;
}

inline std::vector<leaf_entry> resolve_chain_sorted(PageBase *head, uint64_t gc_floor)
{
    std::vector<leaf_entry> out;
    LeafChainCursor         cur(head, gc_floor);
    out.reserve(cur.remaining_hint());
    for (; cur.valid(); cur.next()) {
        out.push_back({.key = cur.key().to_string(), .cell = cell_of(cur.cell())});
    }
    return out;
}

// Collect all live entries in key order by walking the leaf chain via
// right_sibling, starting at the leftmost leaf (found the same way scan()'s
// range walk does: find_leaf_page_id with an empty key, which compares less
// than every real key). This is a full-tree equivalent of scan()'s
// right-sibling walk, so it inherits the same concurrency-safety argument
// (see scan()'s header comment) -- it does NOT do a top-down parent/children
// DFS, so it is safe to run under only an epoch guard (no write_mutex_)
// concurrently with a split or merge: split_leaf_locked publishes the new
// right half and repoints the parent *before* shrinking the original PID,
// and try_merge_leaf_locked gives the merged page the removed leaf's old
// right_sibling, so a leaf read at any point mid-SMO either still holds its
// full pre-SMO entry set (old right_sibling, no gap) or the new content with
// right_sibling already repointed correctly (no gap, no duplicate).
template <class Resolve>
void collect_in_order(Resolve &&resolve, uint64_t root_page_id, uint64_t gc_floor, std::vector<leaf_entry> *out)
{
    if (root_page_id == kInvalidPageId) {
        return;
    }
    uint64_t page_id = find_leaf_page_id(resolve, root_page_id, Slice());
    while (page_id != kInvalidPageId) {
        PageBase *head = resolve(page_id);
        if (head == nullptr) {
            return;
        }
        for (auto &e : resolve_chain_sorted(head, gc_floor)) {
            out->push_back(std::move(e));
        }
        LeafBase *base = chain_leaf_base(head);
        page_id        = base != nullptr ? base->right_sibling() : kInvalidPageId;
    }
}

// R58: uniform view over a merge source (L0 skip-list cursor, L1 leaf cursor,
// or a drained vector) for the loser tree. Wraps the source types behind a
// common key/slot/advance interface so the tree can compare them generically.

} // namespace

Crowdbtree::Crowdbtree(Config opt) : opt_(std::move(opt)), name_(opt_.name)
{
    pool_ = std::make_shared<BufferPool>(opt_.buffer_pool_bytes, opt_.frame_bytes, opt_.page_store);
    // Segment recycling (#14b) hands emptied segments to the tree-owned epoch
    // manager so a lock-free reader that already loaded a segment pointer
    // keeps a valid one until its guard drains.
    mapping_.set_epoch_manager(&epoch_);
    active_ = std::make_shared<MemTable>(memtable_next_id_.fetch_add(1, std::memory_order_relaxed), &epoch_);
    // Initialize with a single empty leaf as the root.
    uint64_t page_id = mapping_.allocate_page_id();
    mapping_.store(page_id, LeafBase::build({}, kInvalidPageId, pool_, opt_.frame_bytes));
    root_page_id_.store(page_id);
}

void Crowdbtree::sync_page_count_gauges()
{
    if (metrics_.tree_leaf_count_g != nullptr) {
        metrics_.tree_leaf_count_g->set(leaf_count_.load(std::memory_order_relaxed));
    }
    if (metrics_.tree_inner_count_g != nullptr) {
        metrics_.tree_inner_count_g->set(inner_count_.load(std::memory_order_relaxed));
    }
}

Crowdbtree::~Crowdbtree()
{
    auto pending = async_flushes_->load();
    while (pending != 0) {
        async_flushes_->wait(pending);
        pending = async_flushes_->load();
    }
    generation_.close();
    reclamation_owner_->store(nullptr, std::memory_order_release);
    active_.reset();
    frozen_.clear();
    split_shared_memtables_.clear();
    detach_native_iterators();
    try {
        free_all_resident_pages(/*retire=*/true);
    }
    catch (...) { // NOLINT(bugprone-empty-catch)
        // Destructors must not throw.
    }
    CRB_LOG_INFO("[{}] close: done last_applied={} contiguous={}", name_, last_applied_slot_.load(),
                 contiguous_slot_.load());
}

void Crowdbtree::retire_page(PageBase *p)
{
    // R6: the deleter sets kRetiredBit instead of deleting outright. If a
    // cross-thread pin (get_async handoff / PinnedSnapshot) is outstanding,
    // the delete defers to the last unpin(). Otherwise it frees immediately
    // (same cost as the old delete).
    preserve_native_page_locked(p->page_id, p);
    epoch_.retire(p, [](void *ptr) { static_cast<PageBase *>(ptr)->retire_with_pins(); });
}

void Crowdbtree::retire_orphaned_page(uint64_t page_id, PageBase *p)
{
    preserve_native_page_locked(page_id, p);
    epoch_.retire(p, [owner = reclamation_owner_, page_id](void *ptr) {
        if (auto *tree = owner->load(std::memory_order_acquire);
            tree != nullptr && tree->mapping_.get_resident(page_id) == ptr) {
            tree->mapping_.clear(page_id);
            static_cast<PageBase *>(ptr)->retire_with_pins();
        }
        // Replacement/teardown owns pages in detached mappings.
    });
}

PageBase *Crowdbtree::resident(uint64_t page_id) const
{
    map_lookup_total_.fetch_add(1, std::memory_order_relaxed);
    if (metrics_.page_find_c != nullptr) {
        metrics_.page_find_c->inc();
    }
    uint64_t w = mapping_.get_word(page_id);
    if (slot_word::is_empty(w) || !slot_word::is_unloaded(w)) {
        if (slot_word::is_resident(w)) {
            PageBase *v = slot_word::resident_ptr(w);
            // CLOCK-informed eviction ranking (plan-tree #17): stamp this
            // touch. Relaxed/relaxed: this is a recency *hint*, not a
            // synchronization point -- ordering across threads doesn't
            // matter, only that concurrent touches keep advancing the
            // stamp, which fetch_add guarantees without a lock.
            v->last_touch_tick.store(touch_tick_.fetch_add(1, std::memory_order_relaxed), std::memory_order_relaxed);
            return v;
        }
        return nullptr; // hot path / unset
    }
    // Cold path: demand-load this base page. Serialized by
    // load_mutex_; double-checked so only one loader installs. The unloaded
    // descriptor is inline in the slot word (no heap allocation), so there is
    // no descriptor to free -- just re-read and check.
    auto             dl_t0 = std::chrono::steady_clock::now();
    std::scoped_lock lk(load_mutex_);
    w = mapping_.get_word(page_id);
    if (slot_word::is_empty(w) || !slot_word::is_unloaded(w)) {
        return slot_word::is_resident(w) ? slot_word::resident_ptr(w) : nullptr; // another loader won
    }
    demand_load_total_.fetch_add(1, std::memory_order_relaxed);
    uint64_t addr     = 0;
    uint32_t phys_len = 0;
    Status   location = opt_.page_store->decode_mapping_location(w, &addr, &phys_len);
    if (!location.ok()) {
        io_failed_.store(true);
        return nullptr;
    }
    // phys_len is the IU-padded physical extent (PT9). The blob header records
    // the raw frame length so we size the decoded frame without other state.
    std::vector<uint8_t> blob(phys_len);
    Status               s = opt_.page_store->read_at(addr, blob.data(), blob.size());
    // A demand-load failure (I/O error or CRC mismatch) is a hard media fault for
    // a committed page; latch it so callers can detect it (the read still degrades
    // to a miss, since the lock-free path can't propagate a Status).
    if (!s.ok()) {
        CRB_LOG_ERROR("[{}] demand-load I/O fault: pid={} addr={} len={} status={}", name_, page_id, addr, phys_len,
                      s.to_string());
        io_failed_.store(true);
        if (metrics_.page_load_l != nullptr) {
            auto ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - dl_t0).count();
            metrics_.page_load_l->observe(static_cast<uint64_t>(ns));
        }
        if (metrics_.page_read_bw != nullptr) {
            metrics_.page_read_bw->observe(blob.size());
        }
        return nullptr;
    }
    if (metrics_.page_load_l != nullptr) {
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - dl_t0).count();
        metrics_.page_load_l->observe(static_cast<uint64_t>(ns));
    }
    if (metrics_.page_read_bw != nullptr) {
        metrics_.page_read_bw->observe(blob.size());
    }
    return install_loaded_page(page_id, addr, phys_len, blob);
}

PageBase *Crowdbtree::install_loaded_page(uint64_t page_id, uint64_t addr, uint32_t /*plen*/,
                                          const std::vector<uint8_t> &blob) const
{
    uint32_t raw_len = durable_blob_raw_len(blob.data(), blob.size());
    if (raw_len == 0) {
        CRB_LOG_ERROR("[{}] demand-load corrupt blob (raw_len=0): pid={} addr={}", name_, page_id, addr);
        io_failed_.store(true);
        return nullptr;
    }
    std::vector<uint8_t> frame(raw_len);
    if (!decode_durable_page(blob.data(), blob.size(), frame.data(), raw_len).ok()) {
        CRB_LOG_ERROR("[{}] demand-load decode failed: pid={} addr={} raw_len={}", name_, page_id, addr, raw_len);
        io_failed_.store(true);
        return nullptr;
    }
    if (!frame_validate_key_range(frame.data(), raw_len, opt_.key_range)) {
        CRB_LOG_ERROR("[{}] demand-load frame or range validation failed: pid={} addr={}", name_, page_id, addr);
        io_failed_.store(true);
        return nullptr;
    }
    page_type ft   = frame_page_type(frame.data());
    PageBase *page = nullptr;
    if (ft == page_type::kLeafBase) {
        page = LeafBase::from_frame_copy(frame.data(), raw_len, pool_, opt_.frame_bytes);
    }
    else if (ft == page_type::kInnerBase) {
        page = InnerBase::from_frame_copy(frame.data(), raw_len, pool_, opt_.frame_bytes);
    }
    else { // kOverflowFrame
        page = OverflowBase::from_frame_copy(frame.data(), raw_len, pool_, opt_.frame_bytes);
    }
    page->page_id      = page_id;
    page->durable_addr = addr; // loaded from here -> clean
    // durable_plen is the logical (unpadded) blob length, recovered from the
    // blob header rather than the IU-padded physical extent `plen` (which is
    // iu_count * iu from the packed slot word). This is what the manifest
    // records and what store_unloaded re-tags.
    page->durable_plen = durable_blob_logical_len(blob.data(), blob.size());
    page->last_touch_tick.store(touch_tick_.fetch_add(1, std::memory_order_relaxed), std::memory_order_relaxed);
    const_cast<MappingTable &>(mapping_).store(page_id, page); // publish resident
    return page;
}

void Crowdbtree::free_subtree(uint64_t page_id, bool retire)
{
    uint64_t w = mapping_.get_word(page_id);
    // Skip unset and *unloaded* slots: an unloaded slot has no heap page to free
    // (the descriptor is inline in the word); its subtree was never loaded.
    if (slot_word::is_empty(w) || slot_word::is_unloaded(w)) {
        return;
    }
    PageBase *head = slot_word::resident_ptr(w);
    // Resolve to the base node to learn the page kind / children.
    PageBase *base = head;
    while (base != nullptr && base->type == page_type::kBatchDelta) {
        base = base->next;
    }
    if (base != nullptr && base->type == page_type::kInnerBase) {
        auto *inner = static_cast<InnerBase *>(base);
        for (uint64_t child : inner->children()) {
            free_subtree(child, retire);
        }
    }
    else if (base != nullptr && base->type == page_type::kLeafBase) {
        // Free the overflow chains referenced by this leaf's pointer cells (they are
        // not reachable via child PIDs). Deltas above carry inline values only.
        LeafFrameView v = static_cast<LeafBase *>(base)->view();
        for (uint32_t i = 0; i < v.count(); ++i) {
            CellView c{v.cell(i)};
            if (c.is_overflow()) {
                if (retire) {
                    retire_overflow_chain_locked(c.overflow_head());
                }
                else {
                    free_overflow_chain(c.overflow_head());
                }
            }
        }
    }
    if (retire) {
        // Live tree (install_snapshot): clear the slot first so a new reader sees
        // "gone", then epoch-retire each node in the chain. A reader that already
        // loaded a node keeps using it under its guard; the frame is freed only once
        // that guard drains.
        mapping_.clear(page_id);
        PageBase *n = head;
        while (n != nullptr) {
            PageBase *next = n->next;
            retire_page(n);
            n = next;
        }
    }
    else {
        // Teardown / clear: no concurrent readers, delete the chain immediately.
        PageBase *n = head;
        while (n != nullptr) {
            PageBase *next = n->next;
            delete n;
            n = next;
        }
        mapping_.clear(page_id);
    }
}

void Crowdbtree::free_all_resident_pages(bool retire)
{
    // Segment-scan, not a root->children walk (see free_subtree's caution
    // comment on crowdb-tree.h for why that matters): every present segment's
    // slots are inspected directly, so a resident leaf/inner/overflow page is
    // found and freed regardless of whether any of its ancestors -- or, for
    // an overflow page, the leaf that spilled it -- happen to be unloaded.
    // This also means, unlike free_subtree, there is no need to separately
    // walk a leaf's cells to find its overflow chains: every overflow page
    // has its own mapping slot (spill_value_to_overflow_chain_locked stores
    // each one under its own allocated PID), so the scan below visits it
    // directly too.
    //
    // Two passes, like prepare_snapshot_locked's own segment scan: pass 1
    // only *reads* segment_at()/slots[i] to collect (page_id, head) pairs,
    // never mutating anything, so it can never race MappingSegment recycling
    // (#14b) -- clearing a slot in pass 2 below can bring a segment's
    // live_count to 0 and epoch-retire the MappingSegment itself; mutating
    // while *this* function's own scan is still walking that same segment's
    // `seg->slots[]` would risk exactly the kind of dangling-segment-pointer
    // access #14b's own design guards against elsewhere.
    struct ResidentEntry
    {
        uint64_t  page_id;
        PageBase *head;
    };

    std::vector<ResidentEntry> resident;
    for (uint64_t seg_idx = 0; seg_idx < MappingTable::kMaxSegments; ++seg_idx) {
        MappingSegment *seg = mapping_.segment_at(seg_idx);
        if (seg == nullptr) {
            continue;
        }
        for (uint32_t i = 0; i < seg->slot_count; ++i) {
            uint64_t w = seg->slots[i].load(std::memory_order_relaxed);
            if (slot_word::is_resident(w)) {
                resident.push_back(
                    {.page_id = (seg_idx * MappingTable::kSegmentSize) + i, .head = slot_word::resident_ptr(w)});
            }
        }
    }
    for (const auto &e : resident) {
        if (retire) {
            // Live tree (install_snapshot(_native)): clear the slot first so a
            // new reader sees "gone", then epoch-retire each node in the chain
            // -- same ordering as free_subtree's retire=true path, and for the
            // same reason (a reader that already loaded a node keeps using it
            // under its guard; the frame is freed only once that guard drains).
            mapping_.clear(e.page_id);
            for (PageBase *n = e.head; n != nullptr;) {
                PageBase *next = n->next;
                retire_page(n);
                n = next;
            }
        }
        else {
            // Teardown: no concurrent readers, but a PinnedSnapshot may still
            // hold refcount pins on these pages (R6). Use retire_with_pins()
            // instead of delete: if pins are outstanding, the delete defers
            // to the last unpin; if no pins, it frees immediately (same cost
            // as delete).
            for (PageBase *n = e.head; n != nullptr;) {
                PageBase *next = n->next;
                n->retire_with_pins();
                n = next;
            }
            mapping_.clear(e.page_id);
        }
    }
}

size_t Crowdbtree::evict_clean_leaves_locked(size_t max_resident_leaves)
{
    // Collect resident, delta-free, clean leaf pids (the evictable set, §4.6).
    // Descend only into already-resident inner children — never demand-load a page
    // just to evict it.
    // (page_id, last_touch_tick) so the candidate set can be ranked by real
    // access recency below (plan-tree #17) instead of arbitrary DFS order.
    std::vector<std::pair<uint64_t, uint64_t>> evictable_ranked;
    std::function<void(uint64_t)>              dfs = [&](uint64_t page_id) {
        uint64_t wv = mapping_.get_word(page_id);
        if (slot_word::is_empty(wv) || slot_word::is_unloaded(wv)) {
            return;
        }
        PageBase *v    = slot_word::resident_ptr(wv);
        PageBase *base = v;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        if (base == nullptr) {
            return;
        }
        if (base->type == page_type::kLeafBase) {
            // Clean (durable bytes match) and no deltas above (v == base) ⇒ evictable.
            if (v == base && v->durable_addr != kNoAddr) {
                evictable_ranked.emplace_back(page_id, v->last_touch_tick.load(std::memory_order_relaxed));
            }
            return;
        }
        for (uint64_t c : static_cast<InnerBase *>(base)->children()) {
            uint64_t cw = mapping_.get_word(c);
            if (slot_word::is_resident(cw)) {
                dfs(c);
            }
        }
    };
    dfs(root_page_id_.load());

    if (evictable_ranked.size() <= max_resident_leaves) {
        return 0;
    }
    // Oldest-touched first: evict genuinely cold pages ahead of recently
    // accessed ones, rather than whichever DFS happened to visit first.
    std::ranges::sort(evictable_ranked.begin(), evictable_ranked.end(),
                      [](const auto &a, const auto &b) { return a.second < b.second; });
    size_t to_evict = evictable_ranked.size() - max_resident_leaves;
    size_t evicted  = 0;
    for (const auto &[page_id, tick] : evictable_ranked) {
        if (evicted >= to_evict) {
            break;
        }
        uint64_t wv = mapping_.get_word(page_id); // re-check (belt-and-suspenders; we hold write_mutex_)
        if (slot_word::is_empty(wv) || slot_word::is_unloaded(wv)) {
            continue;
        }
        PageBase *v = slot_word::resident_ptr(wv);
        if (v->type != page_type::kLeafBase || v->durable_addr == kNoAddr) {
            continue;
        }
        // Evict this leaf's overflow chains too, so their pages don't orphan
        // (resident but unreachable from the now-unloaded leaf).
        LeafFrameView lv = static_cast<LeafBase *>(v)->view();
        for (uint32_t i = 0; i < lv.count(); ++i) {
            CellView c{lv.cell(i)};
            if (c.is_overflow()) {
                evict_overflow_chain_locked(c.overflow_head());
            }
        }
        // Re-tag the slot unloaded, then epoch-retire the resident page. A reader
        // that already loaded `v` keeps using it under its guard (frame freed only
        // once that guard drains); a later reader sees the tag and demand-loads.
        uint64_t unloaded = slot_word::kEmpty;
        if (!opt_.page_store->encode_mapping_location(v->durable_addr, v->durable_plen, &unloaded).ok()) {
            io_failed_.store(true);
            continue;
        }
        mapping_.store_word(page_id, unloaded);
        retire_page(v);
        ++evicted;
    }
    return evicted;
}

size_t Crowdbtree::evict_clean_leaves(size_t max_resident_leaves)
{
    std::scoped_lock lk(write_mutex_);
    return evict_clean_leaves_locked(max_resident_leaves);
}

// plan-tree #17 D3: inner bases get their *own* ranked budget/pass, entirely
// separate from evict_clean_leaves_locked's. An earlier attempt shared one
// combined ranked list between leaves and inner bases and broke
// Eviction.RecentlyTouchedLeafSurvivesEvictionOverColderOnes: a get() stamps
// last_touch_tick on every page it walks through, leaf *and* ancestor inner
// nodes alike, all in the same call -- a single combined budget can rank an
// ancestor behind some other, unrelated leaf and evict it, forcing an
// unwanted demand-load on the very next access to a leaf the test expects to
// stay fully resident with zero extra reads. Keeping the two passes disjoint
// means the leaf test's shared-budget contention can never happen: this
// function never evicts a kLeafBase, and evict_clean_leaves_locked never
// evicts a kInnerBase.
size_t Crowdbtree::evict_clean_inner_locked(size_t max_resident_inner)
{
    // Same DFS shape as evict_clean_leaves_locked (descend only into already-
    // resident children -- never demand-load a page just to evict it), but
    // collecting kInnerBase candidates instead of kLeafBase ones.
    std::vector<std::pair<uint64_t, uint64_t>> evictable_ranked;
    std::function<void(uint64_t)>              dfs = [&](uint64_t page_id) {
        uint64_t wv = mapping_.get_word(page_id);
        if (slot_word::is_empty(wv) || slot_word::is_unloaded(wv)) {
            return;
        }
        PageBase *v    = slot_word::resident_ptr(wv);
        PageBase *base = v;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        if (base == nullptr || base->type != page_type::kInnerBase) {
            return; // a leaf base: nothing to descend into, nothing to collect here
        }
        // Clean (durable bytes match) and no deltas above (v == base) ⇒
        // evictable. Inner bases are never delta-chained today (split/merge
        // always mapping_.store()s a fresh consolidated InnerBase), but this
        // mirrors the leaf pass's check rather than assuming that invariant.
        if (v == base && v->durable_addr != kNoAddr) {
            evictable_ranked.emplace_back(page_id, v->last_touch_tick.load(std::memory_order_relaxed));
        }
        for (uint64_t c : static_cast<InnerBase *>(base)->children()) {
            uint64_t cw = mapping_.get_word(c);
            if (slot_word::is_resident(cw)) {
                dfs(c);
            }
        }
    };
    dfs(root_page_id_.load());

    if (evictable_ranked.size() <= max_resident_inner) {
        return 0;
    }
    // Oldest-touched first, same rationale as the leaf pass.
    std::ranges::sort(evictable_ranked.begin(), evictable_ranked.end(),
                      [](const auto &a, const auto &b) { return a.second < b.second; });
    size_t to_evict = evictable_ranked.size() - max_resident_inner;
    size_t evicted  = 0;
    for (const auto &[page_id, tick] : evictable_ranked) {
        if (evicted >= to_evict) {
            break;
        }
        uint64_t wv = mapping_.get_word(page_id); // re-check (belt-and-suspenders; we hold write_mutex_)
        if (slot_word::is_empty(wv) || slot_word::is_unloaded(wv)) {
            continue;
        }
        PageBase *v = slot_word::resident_ptr(wv);
        if (v->type != page_type::kInnerBase || v->durable_addr == kNoAddr) {
            continue;
        }
        // Re-tag the slot unloaded, then epoch-retire the resident page -- same
        // mechanism as the leaf pass; a reader that already loaded `v` keeps
        // using it under its guard, a later reader demand-loads.
        uint64_t unloaded = slot_word::kEmpty;
        if (!opt_.page_store->encode_mapping_location(v->durable_addr, v->durable_plen, &unloaded).ok()) {
            io_failed_.store(true);
            continue;
        }
        mapping_.store_word(page_id, unloaded);
        retire_page(v);
        ++evicted;
    }
    return evicted;
}

size_t Crowdbtree::evict_clean_inner(size_t max_resident_inner)
{
    std::scoped_lock lk(write_mutex_);
    return evict_clean_inner_locked(max_resident_inner);
}

void Crowdbtree::maybe_evict_locked()
{
    if (!pool_) {
        return;
    }
    BufferPool::Stats st = pool_->stats();
    if (st.num_frames == 0) {
        return;
    }
    // High-water 85%: evict clean leaves down to ~70% of the arena. Best-effort —
    // inner pages and dirty/working-set frames are not evictable, so usage may
    // remain above target until the next snapshot cleans the working set.
    if (static_cast<uint64_t>(st.used) * 100 < static_cast<uint64_t>(st.num_frames) * 85) {
        return;
    }
    evict_clean_leaves_locked((static_cast<size_t>(st.num_frames) * 70) / 100);
}

void Crowdbtree::set_gc_watermark(uint64_t snapshot_slot, uint64_t safe_slot)
{
    uint64_t floor = std::min(snapshot_slot, safe_slot);
    uint64_t prev  = gc_floor_.load();
    while (floor > prev && !gc_floor_.compare_exchange_weak(prev, floor)) {
    }
}

Status Crowdbtree::put(Slice key, Slice value)
{
    Batch b;
    b.ops.push_back({
        .key   = std::string(key.data(), key.size()),
        .kind  = OpKind::kPut,
        .value = std::string(value.data(), value.size()),
    });
    return apply(auto_slot_.fetch_add(1) + 1, b);
}

Status Crowdbtree::del(Slice key)
{
    Batch b;
    b.ops.push_back({.key = std::string(key.data(), key.size()), .kind = OpKind::kDelete, .value = std::string()});
    return apply(auto_slot_.fetch_add(1) + 1, b);
}

Status Crowdbtree::batch_put(const Batch &batch)
{
    return apply(auto_slot_.fetch_add(1) + 1, batch);
}

Status Crowdbtree::validate_key(Slice key) const
{
    if (!opt_.key_range.contains(key)) {
        return Status::invalid_argument("key is outside the tree range");
    }
    return Status::Ok();
}

void Crowdbtree::consolidate_locked(uint64_t page_id)
{
    auto      t0   = std::chrono::steady_clock::now();
    PageBase *head = resident(page_id);
    if (head == nullptr) {
        return;
    }
    if (metrics_.page_consolidate_c != nullptr) {
        metrics_.page_consolidate_c->inc();
    }
    // A bare leaf base with no in-frame deltas (PT12) has nothing to fold; a base
    // carrying in-frame deltas DOES (we fold them into a fresh sorted base).
    if (head->type == page_type::kLeafBase && static_cast<LeafBase *>(head)->view().delta_count() == 0) {
        return;
    }

    LeafBase *old_leaf = chain_leaf_base(head);
    uint64_t  right    = old_leaf != nullptr ? old_leaf->right_sibling() : kInvalidPageId;

    // Fold the chain by highest-slot-wins per key (GC drops tombstones <= floor),
    // spilling new large values into overflow chains. Overflow chains superseded
    // by higher-slot writes are retired so they don't leak.
    std::vector<uint64_t>   dead_overflow;
    std::vector<leaf_entry> entries = resolve_leaf_chain_for_rebuild(head, gc_floor_.load(), &dead_overflow);
    LeafBase               *fresh   = build_leaf_spilling_locked(std::move(entries), right);
    store_preserving_parent_locked(page_id, fresh);

    // retire the old chain (deltas + old base).
    for (PageBase *node = head; node != nullptr;) {
        PageBase *next = node->next;
        retire_page(node);
        node = next;
    }
    for (uint64_t h : dead_overflow) {
        retire_overflow_chain_locked(h);
    }

    maybe_split_or_merge_locked(page_id);
    if (metrics_.page_write_l != nullptr) {
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
        metrics_.page_write_l->observe(static_cast<uint64_t>(ns));
    }
}

void Crowdbtree::set_children_parent_locked(uint64_t page_id, uint64_t parent_page_id)
{
    PageBase *head = resident(page_id);
    if (head == nullptr) {
        return;
    }
    PageBase *base = head;
    while (base != nullptr && base->type == page_type::kBatchDelta) {
        base = base->next;
    }
    if (base == nullptr || base->type != page_type::kInnerBase) {
        return;
    }
    for (uint64_t child : static_cast<InnerBase *>(base)->children()) {
        PageBase *child_head = resident(child);
        if (child_head != nullptr) {
            child_head->parent_page_id = parent_page_id;
        }
    }
}

void Crowdbtree::store_preserving_parent_locked(uint64_t page_id, PageBase *new_page)
{
    PageBase *old = resident(page_id);
    if (old != nullptr) {
        new_page->parent_page_id = old->parent_page_id;
        preserve_native_page_locked(page_id, old);
    }
    mapping_.store(page_id, new_page);
}

std::vector<uint64_t> Crowdbtree::path_to_page_id_locked(uint64_t target_page_id) const
{
    // O4: parent-pointer walk — O(depth) instead of O(tree size) DFS. Walk
    // from target up to the root via parent_page_id, then reverse. The root
    // has parent_page_id == kInvalidPageId. All parent pointers are maintained
    // under write_mutex_ on every split/merge/root-change, and this function
    // is only called under write_mutex_, so no synchronization is needed.
    // Fallback: if a page was demand-loaded (evicted then reloaded from disk),
    // its parent_page_id is kInvalidPageId (not persisted). In that case, fall
    // back to DFS from the root. This is O(tree size) but only on the cold
    // (demand-load) path — the hot path (split/merge) always has valid parent
    // pointers.
    std::vector<uint64_t> path;
    uint64_t              root = root_page_id_.load();
    if (target_page_id == root) {
        return path; // root has no parent path
    }
    // Try the fast parent-pointer walk first. Depth limit prevents
    // infinite loops from stale/cyclic parent pointers (defensive).
    bool      fast_ok   = true;
    const int kMaxDepth = 64;
    int       depth     = 0;
    for (uint64_t pid = target_page_id; pid != kInvalidPageId && pid != root && depth < kMaxDepth;) {
        PageBase *head = resident(pid);
        if (head == nullptr) {
            fast_ok = false;
            break;
        }
        uint64_t parent = head->parent_page_id;
        if (parent == kInvalidPageId || parent == pid) {
            // Broken chain (demand-loaded page) or self-loop: fall back to DFS.
            fast_ok = false;
            break;
        }
        path.push_back(parent);
        if (parent == root) {
            break;
        }
        pid = parent;
        ++depth;
    }
    if (depth >= kMaxDepth) {
        fast_ok = false; // too deep — fall back to DFS
    }
    if (fast_ok) {
        std::reverse(path.begin(), path.end());
        return path;
    }
    // Fallback: DFS from the root (O(tree size) — cold path only).
    path.clear();
    std::function<bool(uint64_t)> dfs = [&](uint64_t page_id) -> bool {
        if (page_id == target_page_id) {
            return true;
        }
        PageBase *head = resident(page_id);
        if (head == nullptr) {
            return false;
        }
        PageBase *base = head;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        if (base == nullptr || base->type != page_type::kInnerBase) {
            return false;
        }
        path.push_back(page_id);
        for (uint64_t child : static_cast<InnerBase *>(base)->children()) {
            if (dfs(child)) {
                return true;
            }
        }
        path.pop_back();
        return false;
    };
    dfs(root);
    return path;
}

void Crowdbtree::maybe_split_or_merge_locked(uint64_t page_id)
{
    PageBase *head = resident(page_id);
    if (head == nullptr || head->type != page_type::kLeafBase) {
        return;
    }
    auto *leaf = static_cast<LeafBase *>(head);
    if (leaf->count() >= 2 && leaf->data_bytes() > opt_.leaf_split_bytes) {
        split_leaf_to_threshold_locked(page_id);
    }
    else if (leaf->data_bytes() < opt_.leaf_merge_bytes && page_id != root_page_id_.load()) {
        // Includes empty leaves (count 0) so fully-deleted leaves merge away.
        try_merge_leaf_locked(page_id, path_to_page_id_locked(page_id));
    }
}

void Crowdbtree::split_leaf_to_threshold_locked(uint64_t leaf_page_id)
{
    // Consolidation can fold a large delta chain into a leaf that is many
    // times the split threshold. A single 2-way split leaves both halves
    // oversized when the leaf is > 2x the threshold. Iteratively split each
    // oversized half (via a worklist) until every leaf fits under the
    // threshold. Reuses split_leaf_locked for each halving; the worklist
    // ensures both the left half (still at leaf_page_id) and the right half
    // (a new sibling) get re-checked. Terminates because each split halves
    // the entry count and we stop at count < 2.
    std::vector<uint64_t> worklist;
    worklist.push_back(leaf_page_id);
    while (!worklist.empty()) {
        uint64_t pid = worklist.back();
        worklist.pop_back();
        PageBase *head = resident(pid);
        if (head == nullptr || head->type != page_type::kLeafBase) {
            continue;
        }
        auto *leaf = static_cast<LeafBase *>(head);
        if (leaf->count() < 2 || leaf->data_bytes() <= opt_.leaf_split_bytes) {
            continue;
        }
        split_leaf_locked(pid, path_to_page_id_locked(pid));
        // Both halves may still exceed the threshold; re-check them.
        PageBase *fresh = resident(pid);
        if (fresh != nullptr && fresh->type == page_type::kLeafBase) {
            worklist.push_back(pid);
            uint64_t right = static_cast<LeafBase *>(fresh)->right_sibling();
            if (right != kInvalidPageId) {
                worklist.push_back(right);
            }
        }
    }
}

void Crowdbtree::split_leaf_locked(uint64_t leaf_page_id, std::vector<uint64_t> path)
{
    if (metrics_.page_split_c != nullptr) {
        metrics_.page_split_c->inc();
    }
    auto                   *leaf = static_cast<LeafBase *>(resident(leaf_page_id));
    std::vector<leaf_entry> e    = leaf->entries(); // materialized owned copy
    size_t                  mid  = e.size() / 2;
    // leaf_entry is move-only (buffer cell): move the halves out, don't copy.
    std::vector<leaf_entry> lo(std::make_move_iterator(e.begin()),
                               std::make_move_iterator(e.begin() + static_cast<std::ptrdiff_t>(mid)));
    std::vector<leaf_entry> hi(std::make_move_iterator(e.begin() + static_cast<std::ptrdiff_t>(mid)),
                               std::make_move_iterator(e.end()));
    std::string             sep = hi.front().key;

    // Publish the right sibling, then repoint the parent(s) at it — all while
    // `leaf_page_id` still holds the FULL entry set. A concurrent reader routed to
    // `leaf_page_id` for an upper-half key still finds it (the parent only starts
    // routing upper-half keys to right_page_id once it references it). Only after the
    // whole path is repointed do we shrink `leaf_page_id` to the lower half.
    uint64_t  right_page_id = mapping_.allocate_page_id();
    LeafBase *right         = LeafBase::build(hi, leaf->right_sibling(), pool_, opt_.frame_bytes);
    mapping_.store(right_page_id, right);
    propagate_split_locked(std::move(path), leaf_page_id, std::move(sep), right_page_id);

    LeafBase *left = LeafBase::build(lo, right_page_id, pool_, opt_.frame_bytes);
    store_preserving_parent_locked(leaf_page_id, left);
    retire_page(leaf);
    leaf_count_.fetch_add(1, std::memory_order_relaxed);
    sync_page_count_gauges();
}

void Crowdbtree::propagate_split_locked(std::vector<uint64_t> path, uint64_t child_page_id, std::string sep,
                                        uint64_t right_page_id)
{
    if (path.empty()) {
        // child was the root: grow a new root one level up.
        uint64_t new_root = mapping_.allocate_page_id();
        mapping_.store(new_root,
                       InnerBase::build({std::move(sep)}, {child_page_id, right_page_id}, pool_, opt_.frame_bytes));
        root_page_id_.store(new_root);
        // O4: set parent pointers on the new root's children.
        set_children_parent_locked(new_root, new_root);
        inner_count_.fetch_add(1, std::memory_order_relaxed);
        sync_page_count_gauges();
        return;
    }
    uint64_t parent_page_id = path.back();
    path.pop_back();
    auto *parent = static_cast<InnerBase *>(resident(parent_page_id));

    // Locate child_page_id among the parent's children.
    const std::vector<uint64_t> &ch  = parent->children();
    size_t                       idx = 0;
    while (idx < ch.size() && ch[idx] != child_page_id) {
        ++idx;
    }

    std::vector<std::string> seps     = parent->separators();
    std::vector<uint64_t>    children = parent->children();
    seps.insert(seps.begin() + static_cast<std::ptrdiff_t>(idx), std::move(sep));
    children.insert(children.begin() + static_cast<std::ptrdiff_t>(idx + 1), right_page_id);

    if (seps.size() <= opt_.inner_max_keys) {
        store_preserving_parent_locked(parent_page_id, InnerBase::build(seps, children, pool_, opt_.frame_bytes));
        retire_page(parent);
        // O4: update parent pointers for all children (the new right_page_id
        // child and any existing children whose parent was just rebuilt).
        set_children_parent_locked(parent_page_id, parent_page_id);
        sync_page_count_gauges();
        return;
    }

    // Inner overflow: split this inner node, pushing the median separator up.
    size_t                   m      = seps.size() / 2;
    std::string              median = seps[m];
    std::vector<std::string> lseps(seps.begin(), seps.begin() + static_cast<std::ptrdiff_t>(m));
    std::vector<uint64_t>    lchildren(children.begin(), children.begin() + static_cast<std::ptrdiff_t>(m + 1));
    std::vector<std::string> rseps(seps.begin() + static_cast<std::ptrdiff_t>(m + 1), seps.end());
    std::vector<uint64_t>    rchildren(children.begin() + static_cast<std::ptrdiff_t>(m + 1), children.end());

    uint64_t rinner_page_id = mapping_.allocate_page_id();
    store_preserving_parent_locked(parent_page_id, InnerBase::build(lseps, lchildren, pool_, opt_.frame_bytes));
    mapping_.store(rinner_page_id, InnerBase::build(rseps, rchildren, pool_, opt_.frame_bytes));
    retire_page(parent);
    // O4: set parent pointers on children of both split inner pages.
    set_children_parent_locked(parent_page_id, parent_page_id);
    set_children_parent_locked(rinner_page_id, rinner_page_id);
    inner_count_.fetch_add(1, std::memory_order_relaxed);

    propagate_split_locked(std::move(path), parent_page_id, std::move(median), rinner_page_id);
}

void Crowdbtree::try_merge_leaf_locked(uint64_t leaf_page_id, const std::vector<uint64_t> &path)
{
    if (path.empty()) {
        return; // root leaf: nothing to merge with
    }
    if (metrics_.page_merge_c != nullptr) {
        metrics_.page_merge_c->inc();
    }
    uint64_t                     parent_page_id = path.back();
    auto                        *parent         = static_cast<InnerBase *>(resident(parent_page_id));
    const std::vector<uint64_t> &ch             = parent->children();
    size_t                       idx            = 0;
    while (idx < ch.size() && ch[idx] != leaf_page_id) {
        ++idx;
    }
    if (idx == 0) {
        return; // no left sibling under this parent (v1: left-merge only)
    }

    uint64_t left_page_id = ch[idx - 1];
    auto    *left_head    = resident(left_page_id);
    if (left_head == nullptr || left_head->type != page_type::kLeafBase) {
        return;
    }
    auto *left = static_cast<LeafBase *>(left_head);
    auto *leaf = static_cast<LeafBase *>(resident(leaf_page_id));

    // 1. Publish the merged left sibling (superset of left+leaf entries). Readers
    //    routed to left_page_id now find both halves; readers still routed to leaf_page_id
    //    (via the not-yet-updated parent) also still find leaf's entries.
    //    GC-drop tombstones <= floor so merged leaves don't accumulate garbage
    //    (otherwise the leftmost leaf bloats and the root never collapses).
    // Resolve each sibling's full entry set (main + in-frame deltas, PT12),
    // GC-dropping tombstones <= floor. The two key ranges are disjoint and each
    // resolve returns sorted storage cells, so concatenation stays sorted. Collect
    // overflow chains that a higher-slot write (e.g. a delete delta) superseded
    // within either chain so they are retired, not leaked.
    uint64_t                gc = gc_floor_.load();
    std::vector<uint64_t>   dead_overflow;
    std::vector<leaf_entry> merged       = resolve_leaf_chain_for_rebuild(left_head, gc, &dead_overflow);
    std::vector<leaf_entry> leaf_entries = resolve_leaf_chain_for_rebuild(leaf, gc, &dead_overflow);
    for (auto &e : leaf_entries) {
        merged.push_back(std::move(e));
    }
    LeafBase *fresh = build_leaf_spilling_locked(std::move(merged), leaf->right_sibling());
    store_preserving_parent_locked(left_page_id, fresh);
    retire_page(left);
    for (uint64_t h : dead_overflow) {
        retire_overflow_chain_locked(h);
    }
    leaf_count_.fetch_sub(1, std::memory_order_relaxed);

    // 2. Repoint the parent: drop separators_[idx-1] and children_[idx].
    std::vector<std::string> seps     = parent->separators();
    std::vector<uint64_t>    children = parent->children();
    seps.erase(seps.begin() + static_cast<std::ptrdiff_t>(idx - 1));
    children.erase(children.begin() + static_cast<std::ptrdiff_t>(idx));

    bool parent_underfull = false;
    if (children.size() == 1 && parent_page_id == root_page_id_.load()) {
        // Root now has a single child: collapse the root one level down.
        // `parent`'s own PID gets no replacement store() -- orphaned.
        root_page_id_.store(children[0]);
        // O4: the new root (single child) has no parent.
        PageBase *new_root_head = resident(children[0]);
        if (new_root_head != nullptr) {
            new_root_head->parent_page_id = kInvalidPageId;
        }
        retire_orphaned_page(parent_page_id, parent);
        inner_count_.fetch_sub(1, std::memory_order_relaxed);
    }
    else {
        size_t parent_seps = seps.size();
        store_preserving_parent_locked(parent_page_id, InnerBase::build(seps, children, pool_, opt_.frame_bytes));
        retire_page(parent);
        // O4: update parent pointers for the rebuilt parent's children.
        set_children_parent_locked(parent_page_id, parent_page_id);
        parent_underfull = parent_page_id != root_page_id_.load() && parent_seps < inner_merge_keys();
    }

    // 3. The leaf is now unreachable by new readers. retire_orphaned_page
    //    epoch-retires it (stragglers holding an old parent are protected by
    //    their epoch guard) and clears its mapping slot once that's safe
    //    -- deferred, not
    //    immediate, so it can never race a straggler still walking in via a
    //    stale parent from before this retirement (see retire_orphaned_
    //    page's doc comment). The PID itself is never recycled (D1).
    retire_orphaned_page(leaf_page_id, leaf);

    // 4. Inner-node underflow: if the parent dropped below the merge threshold,
    //    merge it with its left sibling (recurses up, may collapse the root).
    if (parent_underfull) {
        std::vector<uint64_t> ppath = path; // root..parent
        ppath.pop_back();                   // -> root..grandparent (parent's path)
        try_merge_inner_locked(parent_page_id, std::move(ppath));
    }
    sync_page_count_gauges();
}

void Crowdbtree::try_merge_inner_locked(uint64_t inner_page_id, std::vector<uint64_t> path)
{
    if (path.empty()) {
        return; // inner is the root: nothing to merge with
    }
    uint64_t gp_page_id = path.back();
    auto    *gp_head    = resident(gp_page_id);
    if (gp_head == nullptr || gp_head->type != page_type::kInnerBase) {
        return;
    }
    auto *gp = static_cast<InnerBase *>(gp_head);

    const std::vector<uint64_t> &gch = gp->children();
    size_t                       idx = 0;
    while (idx < gch.size() && gch[idx] != inner_page_id) {
        ++idx;
    }
    if (idx == 0 || idx >= gch.size()) {
        return; // no left sibling (v1: left-merge only)
    }

    uint64_t left_page_id = gch[idx - 1];
    auto    *left_head    = resident(left_page_id);
    if (left_head == nullptr || left_head->type != page_type::kInnerBase) {
        return;
    }
    auto *left       = static_cast<InnerBase *>(left_head);
    auto *inner_head = resident(inner_page_id);
    if (inner_head == nullptr || inner_head->type != page_type::kInnerBase) {
        return;
    }
    auto *inner = static_cast<InnerBase *>(inner_head);

    // Only merge if the combined node still fits the fanout bound; otherwise leave
    // the page underfull (correct, just less compact) rather than build an
    // immediately-oversized inner.
    size_t combined_seps = left->num_separators() + 1 + inner->num_separators();
    if (combined_seps > opt_.inner_max_keys) {
        return;
    }

    // 1. Publish the merged left sibling = left.children + inner.children, with the
    //    grandparent's separator-between spliced in. Readers via the old
    //    grandparent still reach `inner` (retired, epoch-safe) with its children;
    //    readers via the new grandparent reach merged-left with both subtrees.
    std::vector<std::string> mseps = left->separators();
    mseps.push_back(gp->separator_at(idx - 1));
    for (auto &s : inner->separators()) {
        mseps.push_back(std::move(s));
    }
    std::vector<uint64_t> mchildren = left->children();
    for (uint64_t c : inner->children()) {
        mchildren.push_back(c);
    }
    store_preserving_parent_locked(left_page_id, InnerBase::build(mseps, mchildren, pool_, opt_.frame_bytes));
    retire_page(left);
    // O4: update parent pointers for the merged left sibling's children.
    set_children_parent_locked(left_page_id, left_page_id);
    inner_count_.fetch_sub(1, std::memory_order_relaxed); // two inners → one

    // 2. Repoint the grandparent: drop separators[idx-1] and children[idx].
    std::vector<std::string> gseps     = gp->separators();
    std::vector<uint64_t>    gchildren = gp->children();
    gseps.erase(gseps.begin() + static_cast<std::ptrdiff_t>(idx - 1));
    gchildren.erase(gchildren.begin() + static_cast<std::ptrdiff_t>(idx));

    bool gp_underfull = false;
    if (gchildren.size() == 1 && gp_page_id == root_page_id_.load()) {
        // Root now has a single child: collapse one level down. `gp`'s own
        // PID gets no replacement store() -- orphaned.
        root_page_id_.store(gchildren[0]);
        // O4: the new root (single child) has no parent.
        PageBase *new_root_head = resident(gchildren[0]);
        if (new_root_head != nullptr) {
            new_root_head->parent_page_id = kInvalidPageId;
        }
        retire_orphaned_page(gp_page_id, gp);
        inner_count_.fetch_sub(1, std::memory_order_relaxed); // root inner retired
    }
    else {
        size_t gp_seps = gseps.size();
        store_preserving_parent_locked(gp_page_id, InnerBase::build(gseps, gchildren, pool_, opt_.frame_bytes));
        retire_page(gp);
        // O4: update parent pointers for the rebuilt grandparent's children.
        set_children_parent_locked(gp_page_id, gp_page_id);
        gp_underfull = gp_page_id != root_page_id_.load() && gp_seps < inner_merge_keys();
    }

    // 3. The merged-away inner is unreachable by new readers; retire_orphaned_page
    //    epoch-retires it (safe for stragglers) and clears its mapping slot once
    //    that's safe (deferred, not immediate -- see that method's doc comment).
    //    Its children are now owned by merged-left, so retiring this single page
    //    does not free them. PID itself never recycled (D1).
    retire_orphaned_page(inner_page_id, inner);

    // 4. Recurse: the grandparent may now be underfull.
    if (gp_underfull) {
        path.pop_back(); // -> root..great-grandparent (grandparent's path)
        try_merge_inner_locked(gp_page_id, std::move(path));
    }
    sync_page_count_gauges();
}

GetView Crowdbtree::get_view(Slice key) const
{
    auto    generation = generation_.enter();
    GetView result;
    if (!opt_.key_range.contains(key)) {
        return result;
    }
    result.guard_ = epoch_.enter();

    // L0: check every live MemTable (active_ + any not-yet-drained frozen_
    // buffers) and keep the highest-slot hit. Unlike the single-buffer
    // design, a key can legitimately be present in more than one live
    // MemTable at once with *different* slots (out-of-order slot delivery
    // can straddle a freeze boundary) -- see the active_/frozen_ member
    // comment (plan-tree #3) for the full argument. Any key present in ANY
    // live MemTable is still guaranteed strictly newer than L1, so a hit
    // here never needs to fall through to L1.
    //
    // R50: an L0 hit borrows the value directly from the CellVersion's
    // buffer — the epoch guard keeps the skip-list node (and its cell
    // version) alive past any concurrent overwrite/drain, exactly as it
    // keeps an L1 frame resident. No copy, no std::string staging.
    auto               l0_t0  = std::chrono::steady_clock::now();
    auto               tables = all_memtables();
    const CellVersion *best   = nullptr;
    for (auto &mt : tables) {
        const CellVersion *cv = mt->find(key);
        if (cv == nullptr) {
            continue;
        }
        mt_get_total_.fetch_add(1, std::memory_order_relaxed);
        if (metrics_.mt_get_c != nullptr) {
            metrics_.mt_get_c->inc();
        }
        if (best == nullptr || cv->slot >= best->slot) {
            best                 = cv;
            result.source_guard_ = mt.guard;
        }
    }
    if (best != nullptr) {
        mt_get_hit_total_.fetch_add(1, std::memory_order_relaxed);
        if (metrics_.mt_get_hit_c != nullptr) {
            metrics_.mt_get_hit_c->inc();
        }
        if (metrics_.mt_get_l != nullptr) {
            auto ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - l0_t0).count();
            metrics_.mt_get_l->observe(static_cast<uint64_t>(ns));
        }
        if ((best->flags & kFlagTombstone) != 0) {
            return result; // not found
        }
        result.found_ = true;
        result.slot_  = best->slot;
        // Borrow the value: contiguous cell -> value after the 9-byte header;
        // split (kExternal) cell -> the buffer itself is the value.
        if (best->cell.ownership() != buffer::mode::kExternal) {
            result.value_ = {best->cell.data() + kCellHeaderSize, best->cell.size() - kCellHeaderSize};
        }
        else {
            result.value_ = best->cell.slice();
        }
        return result;
    }
    if (metrics_.mt_get_l != nullptr) {
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - l0_t0).count();
        metrics_.mt_get_l->observe(static_cast<uint64_t>(ns));
    }

    // L1: descend to the leaf and resolve its chain. A non-overflow cell's
    // value lives directly in head's frame, which result.guard_ keeps
    // resident for result's lifetime -- borrow it, no copy.
    auto l1_t0 = std::chrono::steady_clock::now();
    l1_get_total_.fetch_add(1, std::memory_order_relaxed);
    if (metrics_.l1_get_c != nullptr) {
        metrics_.l1_get_c->inc();
    }
    uint64_t page_id = find_leaf_page_id([this](uint64_t p) { return resident(p); }, root_page_id_.load(), key);
    if (page_id == kInvalidPageId) {
        if (metrics_.l1_get_l != nullptr) {
            auto ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - l1_t0).count();
            metrics_.l1_get_l->observe(static_cast<uint64_t>(ns));
        }
        return result;
    }
    PageBase *head = resident(page_id);
    CellView  v;
    if (!resolve_chain(head, key, &v)) {
        if (metrics_.l1_get_l != nullptr) {
            auto ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - l1_t0).count();
            metrics_.l1_get_l->observe(static_cast<uint64_t>(ns));
        }
        return result;
    }
    if (v.is_tombstone()) {
        if (metrics_.l1_get_l != nullptr) {
            auto ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - l1_t0).count();
            metrics_.l1_get_l->observe(static_cast<uint64_t>(ns));
        }
        return result;
    }
    l1_get_hit_total_.fetch_add(1, std::memory_order_relaxed);
    if (metrics_.l1_get_hit_c != nullptr) {
        metrics_.l1_get_hit_c->inc();
    }
    if (metrics_.l1_get_l != nullptr) {
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - l1_t0).count();
        metrics_.l1_get_l->observe(static_cast<uint64_t>(ns));
    }
    result.found_ = true;
    result.slot_  = v.slot();
    if (v.is_overflow()) {
        // Assembled from multiple overflow pages -- no single frame to
        // borrow from, so materialize it like an L0 hit.
        result.owned_ = buffer::copy_of(assemble_overflow_value(v.overflow_head(), v.overflow_len()));
        result.value_ = result.owned_.slice();
    }
    else {
        result.value_ = v.value(); // borrowed: lives in head's frame
    }
    return result;
}

bool Crowdbtree::try_get_view_no_load(Slice key, GetView *result, uint64_t *out_pending_page_id) const
{
    auto generation = generation_.enter();
    result->guard_  = epoch_.enter();

    // L0: identical to get_view() -- never touches the page store, so there
    // is no I/O to avoid here. R50: borrows the value directly (no copy).
    auto               tables = all_memtables();
    const CellVersion *best   = nullptr;
    for (auto &mt : tables) {
        const CellVersion *cv = mt->find(key);
        if (cv == nullptr) {
            continue;
        }
        if (best == nullptr || cv->slot >= best->slot) {
            best                  = cv;
            result->source_guard_ = mt.guard;
        }
    }
    if (best != nullptr) {
        if ((best->flags & kFlagTombstone) != 0) {
            return true; // resolved: not found
        }
        result->found_ = true;
        result->slot_  = best->slot;
        if (best->cell.ownership() != buffer::mode::kExternal) {
            result->value_ = {best->cell.data() + kCellHeaderSize, best->cell.size() - kCellHeaderSize};
        }
        else {
            result->value_ = best->cell.slice();
        }
        return true;
    }

    // L1: same descent as get_view(), but `probe` bails out (returning
    // nullptr, as if the slot were unset) the moment it sees an unloaded
    // slot, instead of demand-loading it -- recording *which* page_id via
    // `blocked_page_id`. Never unpacks the unloaded descriptor (see this
    // method's doc comment on crowdb-tree.h): only slot_word::is_unloaded(), a
    // plain tag-bit check on the packed word, which needs no lock.
    uint64_t blocked_page_id = kInvalidPageId;
    auto     probe           = [this, &blocked_page_id](uint64_t p) -> PageBase               *{
        uint64_t w = mapping_.get_word(p);
        if (slot_word::is_empty(w)) {
            return nullptr;
        }
        if (slot_word::is_unloaded(w)) {
            blocked_page_id = p;
            return nullptr;
        }
        PageBase *v = slot_word::resident_ptr(w);
        v->last_touch_tick.store(touch_tick_.fetch_add(1, std::memory_order_relaxed), std::memory_order_relaxed);
        return v;
    };

    uint64_t page_id = find_leaf_page_id(probe, root_page_id_.load(), key);
    if (blocked_page_id != kInvalidPageId) {
        *out_pending_page_id = blocked_page_id;
        return false; // genuine miss
    }
    if (page_id == kInvalidPageId) {
        return true; // resolved: not found (empty/malformed tree)
    }
    // Re-probe the leaf head, mirroring get_view()'s separate resident()
    // call after find_leaf_page_id -- find_leaf_page_id doesn't return the
    // resolved pointer, only the page_id, and a fresh probe is cheap
    // (lock-free) and tolerates a concurrent mutation the same way
    // get_view() already does.
    PageBase *head = probe(page_id);
    if (blocked_page_id != kInvalidPageId) {
        *out_pending_page_id = blocked_page_id;
        return false; // genuine miss (raced with a concurrent eviction)
    }
    if (head == nullptr) {
        return true; // resolved: not found
    }
    CellView v;
    if (!resolve_chain(head, key, &v)) {
        return true; // resolved: not found
    }
    if (v.is_tombstone()) {
        return true; // resolved: not found
    }
    result->found_ = true;
    result->slot_  = v.slot();
    if (v.is_overflow()) {
        // Scope boundary (see get_async's doc comment on crowdb-tree.h):
        // overflow-chain misses stay synchronous.
        result->owned_ = buffer::copy_of(assemble_overflow_value(v.overflow_head(), v.overflow_len()));
        result->value_ = result->owned_.slice();
    }
    else {
        result->value_               = v.value(); // borrowed: lives in head's frame
        result->borrowed_chain_head_ = head;      // R6: pin target for slow path
    }
    return true; // resolved: found
}

GetView Crowdbtree::materialize_owned(GetView &&v)
{
    if (v.found_ && v.owned_.empty() && v.borrowed_chain_head_ != nullptr) {
        // R6: frame-borrowed value on the slow path. Pin the chain (head →
        // base) so the borrowed Slice survives the thread boundary, then
        // release the epoch guard on this (the entering) thread. The last
        // unpin (from ct_future_free on any thread) frees if the page was
        // retired in the meantime.
        for (PageBase *n = v.borrowed_chain_head_; n != nullptr; n = n->next) {
            n->pin();
            v.pins_.push_back(n);
        }
        v.borrowed_chain_head_ = nullptr;
    }
    else if (v.found_ && v.owned_.empty()) {
        // Overflow-chain value (assembled, no single frame to borrow): copy
        // as before. R6 doesn't change this path.
        v.owned_ = buffer::copy_of(v.value_);
        v.value_ = v.owned_.slice();
    }
    // Release on this (the entering) thread before on_done can hand this
    // GetView off across the FFI boundary to a ct_future_free that might
    // run on a different one. For the pin path, the pages stay alive via
    // refcount; for the copy path, the owned buffer is independent.
    v.guard_ = EpochManager::Guard();
    v.source_guard_.reset();
    return std::move(v);
}

void Crowdbtree::get_async(Slice key, std::function<void(Status, GetView)> on_done) const
{
    // Copy the key upfront: unlike get_view()'s Slice (borrowed, valid only
    // for this one synchronous call), get_async's key must survive across
    // an arbitrary number of async round trips, each on a different call
    // stack than this one.
    get_async_attempt(std::make_shared<std::string>(key.to_string()), std::move(on_done), /*same_thread=*/true);
}

void Crowdbtree::get_async_attempt(std::shared_ptr<std::string> key_owned, std::function<void(Status, GetView)> on_done,
                                   bool same_thread) const
{
    GetView  result;
    uint64_t pending_page_id = kInvalidPageId;
    if (try_get_view_no_load(Slice(*key_owned), &result, &pending_page_id)) {
        // same_thread: zero-copy fast path -- hand the GetView straight through, guard and all.
        // Otherwise this resolved on (or after being handed off from) the
        // Reactor thread, so materialize_owned() releases the guard here,
        // on the thread that entered it, before on_done can cross back out.
        on_done(Status::Ok(), same_thread ? std::move(result) : materialize_owned(std::move(result)));
        return;
    }

    if (opt_.async_page_store != nullptr) {
        // Re-verify under load_mutex_ before unpacking the unloaded
        // descriptor from the slot word (see try_get_view_no_load's doc
        // comment on crowdb-tree.h): the word may be concurrently replaced by
        // a loader installing the resident replacement, so re-read under
        // the lock -- mirrors resident()'s own double-checked locking
        // exactly, just split across the async submission below.
        uint64_t addr           = 0;
        uint32_t plen           = 0;
        bool     still_unloaded = false;
        uint64_t requested_word = 0;
        Status   location_status;
        {
            auto             generation = generation_.enter();
            std::scoped_lock lk(load_mutex_);
            uint64_t         w = mapping_.get_word(pending_page_id);
            if (slot_word::is_unloaded(w)) {
                requested_word  = w;
                location_status = opt_.page_store->decode_mapping_location(w, &addr, &plen);
                still_unloaded  = location_status.ok();
            }
        }
        if (!location_status.ok()) {
            io_failed_.store(true);
            on_done(location_status, {});
            return;
        }
        if (!still_unloaded) {
            // Another loader (sync resident() or a concurrent get_async)
            // already resolved this page_id between the lock-free probe
            // above and this re-check -- just retry, still on this thread.
            get_async_attempt(std::move(key_owned), std::move(on_done), same_thread);
            return;
        }
        uint32_t iu   = opt_.page_store->iu_size();
        auto     blob = std::make_shared<std::vector<uint8_t>>(round_up_to_iu(plen, iu));
        demand_load_total_.fetch_add(1, std::memory_order_relaxed);
        opt_.async_page_store->submit_read(
            addr, blob->data(), blob->size(),
            detail::own_async_completion([this, page_id = pending_page_id, requested_word, addr, plen, blob, key_owned,
                                          on_done](Status st) mutable {
                if (!st.ok()) {
                    CRB_LOG_ERROR("[{}] get_async: demand-load I/O fault: pid={} addr={} len={} status={}", name_,
                                  page_id, addr, plen, st.to_string());
                    if (st.code() != Code::kUnavailable && st.code() != Code::kResourceExhausted) {
                        io_failed_.store(true);
                    }
                    on_done(std::move(st), GetView());
                    return;
                }
                bool installed_ok = true;
                {
                    auto             generation = generation_.enter();
                    std::scoped_lock lk(load_mutex_);
                    uint64_t         w = mapping_.get_word(page_id);
                    if (w == requested_word) {
                        installed_ok = install_loaded_page(page_id, addr, plen, *blob) != nullptr;
                    }
                    // else: another loader already installed it -- retry below.
                }
                if (!installed_ok) {
                    // Decode/CRC/validation failure -- io_failed_ already
                    // latched by install_loaded_page; matches resident()'s
                    // own "degrades to a miss" contract.
                    on_done(Status::corruption("get_async: demand-load decode, structure, or range failure"),
                            GetView());
                    return;
                }
                // This callback runs on the Reactor's own thread (design's
                // thread model table) -- everything from here on is *not*
                // same_thread relative to the original caller.
                get_async_attempt(std::move(key_owned), std::move(on_done), /*same_thread=*/false);
            }));
        return;
    }
    // No async backend wired (e.g. a MemPageStore-backed tree -- design
    // §6.3: no MemAsyncPageStore, nothing is genuinely pending there) --
    // fall back to the existing synchronous demand-load and retry, still
    // on this same thread.
    {
        auto generation = generation_.enter();
        (void)resident(pending_page_id);
    }
    get_async_attempt(std::move(key_owned), std::move(on_done), same_thread);
}

bool Crowdbtree::get(Slice key, uint64_t *out_slot, std::string *out_value) const
{
    GetView v = get_view(key);
    if (!v.found()) {
        return false;
    }
    if (out_slot != nullptr) {
        *out_slot = v.slot();
    }
    if (out_value != nullptr) {
        *out_value = v.value().to_string(); // clone; v.guard_ releases at end of this function
    }
    return true;
}

std::vector<get_result> Crowdbtree::multi_get(const std::vector<Slice> &keys) const
{
    std::vector<get_result> results;
    results.reserve(keys.size());
    for (const Slice &k : keys) {
        get_result g;
        g.found = get(k, &g.slot, &g.value);
        results.push_back(std::move(g));
    }
    return results;
}

Status
Crowdbtree::scan(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget, bool keys_only,
                 uint64_t                 deadline_ms,
                 std::vector<scan_entry> *out, // NOLINT(readability-non-const-parameter) written to via push_back
                 bool *truncated, bool include_tombstones, ScanPackedBuf *out_packed,
                 size_t *out_count, // NOLINT(readability-non-const-parameter) written to via *out_count
                 bool has_start_bound, bool start_inclusive) const
{
    auto generation = generation_.enter();
    // Preserve scan()'s original `start_after` contract for direct C++
    // callers.  The explicit flag additionally makes an empty key usable as
    // a real lower bound through scan-from APIs.
    has_start_bound = has_start_bound || !start_after.empty();
    if (out == nullptr && out_packed == nullptr) {
        return Status::invalid_argument("scan requires an output buffer");
    }
    if (out != nullptr) {
        out->clear();
    }
    if (out_packed != nullptr) {
        *out_packed = ScanPackedBuf{};
    }
    size_t packed_count = 0;
    if (truncated != nullptr) {
        *truncated = false;
    }
    // plan-tree #5 B3: lock-free scan. An epoch guard (not write_mutex_) is
    // sufficient: L1 is walked leaf-by-leaf via right_sibling starting at the
    // leaf that would contain `prefix`, one leaf resolved at a time, instead of
    // materializing the whole reachable tree up front under a lock. This is
    // safe against a concurrent split/merge because both always keep a leaf's
    // right_sibling link consistent with the content it's attached to via a
    // single atomic mapping_.store: split_leaf_locked publishes the new right
    // half and repoints the parent *before* shrinking the original PID, so a
    // leaf read mid-split either still holds its full pre-split entry set (old
    // right_sibling, no gap) or the shrunk half with right_sibling already
    // pointing at the new right half (no gap, no duplicate); a merge folds the
    // removed leaf's entries into its left sibling and gives the merged page
    // the removed leaf's old right_sibling, so a stale reader still positioned
    // at the removed (retired, epoch-alive) PID reaches the correct successor
    // via its own unchanged right_sibling. See split_leaf_locked /
    // try_merge_leaf_locked for the exact ordering this relies on.
    EpochManager::Guard guard = epoch_.enter();

    auto dur_ns = [](std::chrono::steady_clock::time_point from) {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - from).count());
    };
    auto t_total = std::chrono::steady_clock::now();

    // L0: one lock-free cursor per live MemTable (active_ + any not-yet-
    // drained frozen_ buffers). R50: the cursor borrows key/cell Slices
    // directly off the skip-list node — no snapshot copy. The epoch guard
    // (taken above) keeps the node alive past any concurrent drain/overwrite.
    // Unlike the single-buffer design, more than one of these can hold the
    // same key with a *different* slot (out-of-order slot delivery can
    // straddle a freeze boundary -- see the active_/frozen_ member comment,
    // plan-tree #3), so the merge below picks the highest-slot cell among
    // whichever sources tie on a key, instead of unconditionally preferring
    // "the" L0 stream.
    struct L0Cursor
    {
        ConcurrentSkipList::Cursor cur;
    };

    auto                  t0 = std::chrono::steady_clock::now();
    std::vector<L0Cursor> l0;
    auto                  sources_owner = all_memtables();
    l0.reserve(sources_owner.size());
    for (auto &mt : sources_owner) {
        l0.push_back(
            {.cur = has_start_bound ? mt->cursor_from(start_after, start_inclusive) : mt->cursor(start_after)});
    }
    uint64_t l0_ns = dur_ns(t0);

    uint64_t l1_ns   = 0;
    uint64_t gc      = gc_floor_.load();
    uint64_t page_id = root_page_id_.load();
    // Descend at the cursor when present, else at the prefix start -- a
    // non-empty start_after lands directly on the leaf containing it,
    // skipping every earlier leaf in the prefix range.
    Slice descend_key = !start_after.empty() ? start_after : prefix;
    if (page_id != kInvalidPageId) {
        t0      = std::chrono::steady_clock::now();
        page_id = find_leaf_page_id([this](uint64_t p) { return resident(p); }, page_id, descend_key);
        l1_ns += page_id != kInvalidPageId ? dur_ns(t0) : 0;
    }
    // A lazy cursor over the current leaf's chain, not a materialized
    // vector -- it yields borrowed key/cell Slices in key order and only
    // resolves as far as the merge loop pulls, so a limit-bounded scan never
    // pays for the rest of the leaf. The Slices stay valid for this whole
    // synchronous call under the epoch guard entered above.
    LeafChainCursor l1;
    bool            first_leaf = true;

    // Pull the next non-exhausted leaf (an all-tombstone/GC'd leaf yields
    // nothing; keep walking right past it) until the cursor has an entry or the
    // chain is exhausted. Idempotent when the cursor is already positioned.
    auto refill_l1 = [&]() -> bool {
        while (!l1.valid() && page_id != kInvalidPageId) {
            PageBase *head = resident(page_id);
            if (head == nullptr) {
                page_id = kInvalidPageId;
                break;
            }
            auto rt = std::chrono::steady_clock::now();
            l1.reset(head, gc);
            // Only the first leaf can hold entries at or before the cursor:
            // the descent landed on it. Seek past them by binary search
            // instead of letting the merge loop step over them one by one.
            if (first_leaf && !descend_key.empty()) {
                l1.seek(descend_key, /*exclusive=*/has_start_bound && !start_inclusive);
            }
            first_leaf = false;
            l1_ns += dur_ns(rt);
            LeafBase *base = chain_leaf_base(head);
            page_id        = base != nullptr ? base->right_sibling() : kInvalidPageId;
            // R58: prefetch the right-sibling leaf's memory while the merge
            // loop works on the current leaf — overlaps the cache fill with
            // merge work. The page is already resident (sync scan path); this
            // targets CPU cache, not disk. Uses mapping_ directly to avoid
            // the touch_tick overhead of resident().
            if (page_id != kInvalidPageId) {
                uint64_t w = mapping_.get_word(page_id);
                if (slot_word::is_resident(w)) {
                    __builtin_prefetch(slot_word::resident_ptr(w), 0, 2);
                }
            }
        }
        return l1.valid();
    };

    size_t accumulated_bytes = 0;
    auto   consider          = [&](Slice key, Slice cell) -> bool {
        if (opt_.key_range.before(key)) {
            return true;
        }
        if (opt_.key_range.at_or_after_end(key)) {
            return false;
        }
        if (has_start_bound && (start_inclusive ? key.compare(start_after) < 0 : key.compare(start_after) <= 0)) {
            return true;
        }
        if (!key.starts_with(prefix)) {
            return true;
        }
        CellView v{cell};
        if (v.is_tombstone() && !include_tombstones) {
            return true;
        }
        assert(out_packed != nullptr || out != nullptr);
        size_t cur_count = out_packed != nullptr ? packed_count : out->size();
        if (limit != 0 && cur_count >= limit) {
            if (truncated != nullptr) {
                *truncated = true;
            }
            return false; // stop: a matching entry didn't fit
        }
        if (v.is_tombstone()) {
            size_t entry_bytes = key.size();
            if (byte_budget != 0 && cur_count > 0 && accumulated_bytes + entry_bytes > byte_budget) {
                if (truncated != nullptr) {
                    *truncated = true;
                }
                return false; // byte budget would be exceeded; keep what we have
            }
            if (out_packed != nullptr) {
                out_packed->pack_u32(static_cast<uint32_t>(key.size()));
                out_packed->append(key);
                out_packed->pack_u64(v.slot());
                out_packed->push_back(1);
                out_packed->pack_u32(0);
            }
            else {
                out->push_back({.key = key.to_string(), .slot = v.slot(), .value = "", .tombstone = true});
            }
            ++packed_count;
            accumulated_bytes += entry_bytes;
            return true;
        }
        std::string val;
        if (!keys_only) {
            val =
                v.is_overflow() ? assemble_overflow_value(v.overflow_head(), v.overflow_len()) : v.value().to_string();
        }
        size_t key_size    = key.size();
        size_t value_size  = val.size();
        size_t entry_bytes = key_size + value_size;
        if (byte_budget != 0 && cur_count > 0 && accumulated_bytes + entry_bytes > byte_budget) {
            if (truncated != nullptr) {
                *truncated = true;
            }
            return false; // byte budget would be exceeded; keep what we have
        }
        if (out_packed != nullptr) {
            out_packed->pack_u32(static_cast<uint32_t>(key_size));
            out_packed->append(key);
            out_packed->pack_u64(v.slot());
            out_packed->push_back(0);
            out_packed->pack_u32(static_cast<uint32_t>(value_size));
            out_packed->append(val);
        }
        else {
            out->push_back({.key = key.to_string(), .slot = v.slot(), .value = std::move(val), .tombstone = false});
        }
        ++packed_count;
        accumulated_bytes += entry_bytes;
        if (byte_budget != 0 && entry_bytes > byte_budget) {
            CRB_LOG_WARN("[{}] scan: oversized entry key_size={} value_size={} exceeds byte_budget={}", name_, key_size,
                         value_size, byte_budget);
        }
        return true;
    };

    // R58: merge loop with 2-source fast path + loser tree. On a key collision
    // across multiple sources (possible across L0 streams -- see the L0Cursor
    // comment above; L1 only ever collides with L0, never with itself), the
    // highest-slot cell wins and every cursor sitting on that key is advanced,
    // so a key present in more than one source still yields exactly one output
    // entry. The match function: lower key wins; tie → higher slot; tie →
    // lower source index (deterministic, matching the original iteration order).
    //
    // R50: L0 cursors borrow key/cell off the skip-list node. The winner is
    // tracked as a CellVersion* (L0) or a cell Slice (L1); slot comparison
    // uses cv->slot / CellView::slot respectively. The winning L0 cell is
    // materialized into a contiguous buffer only when it reaches the output
    // (O(limit), not O(N_l0)).
    size_t n_valid_l0 = 0;
    for (const auto &c : l0) {
        if (c.cur.valid()) {
            ++n_valid_l0;
        }
    }

    LoserTree                lt;
    std::vector<MergeSource> lt_sources;
    bool                     lt_built = false;

    auto   t_loop           = std::chrono::steady_clock::now();
    size_t deadline_counter = 0; // check deadline every kDeadlineCheckInterval entries
    while (true) {
        // Periodic deadline check: amortize the clock read over 1024 entries.
        // When exceeded, break with truncated = true and return the partial
        // result accumulated so far.
        if (deadline_ms != 0 && ++deadline_counter >= 1024) {
            deadline_counter = 0;
            auto now_ms      = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                         std::chrono::system_clock::now().time_since_epoch())
                                                         .count());
            if (now_ms >= deadline_ms) {
                if (truncated != nullptr) {
                    *truncated = true;
                }
                break;
            }
        }
        bool l1_was_valid_before = l1.valid();
        bool have_l1             = refill_l1();
        bool l1_refilled         = have_l1 && !l1_was_valid_before;

        size_t n_sources = n_valid_l0 + (have_l1 ? 1 : 0);
        if (n_sources == 0) {
            break;
        }

        Slice              winner_key;
        const CellVersion *l0_winner = nullptr;
        Slice              l1_winner_cell;
        buffer             l0_materialized;

        if (n_sources == 1) {
            // Single source: no merge compare needed.
            if (have_l1) {
                winner_key     = l1.key();
                l1_winner_cell = l1.cell();
                l1.next();
            }
            else {
                for (auto &c : l0) {
                    if (!c.cur.valid()) {
                        continue;
                    }
                    winner_key = c.cur.key();
                    l0_winner  = c.cur.cell_version();
                    c.cur.prefetch_next();
                    c.cur.advance();
                    if (!c.cur.valid()) {
                        --n_valid_l0;
                    }
                    break;
                }
            }
        }
        else if (n_sources == 2) {
            // 2-source fast path: 1 compare instead of 2×2. The common
            // steady-state case (1 active L0 + L1, no frozen memtables).
            ConcurrentSkipList::Cursor *c0 = nullptr;
            ConcurrentSkipList::Cursor *c1 = nullptr;
            for (auto &c : l0) {
                if (!c.cur.valid()) {
                    continue;
                }
                if (c0 == nullptr) {
                    c0 = &c.cur;
                }
                else {
                    c1 = &c.cur;
                    break;
                }
            }
            if (n_valid_l0 == 2) {
                int cmp = c0->key().compare(c1->key());
                if (cmp < 0) {
                    winner_key = c0->key();
                    l0_winner  = c0->cell_version();
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                }
                else if (cmp > 0) {
                    winner_key = c1->key();
                    l0_winner  = c1->cell_version();
                    c1->prefetch_next();
                    c1->advance();
                    if (!c1->valid()) {
                        --n_valid_l0;
                    }
                }
                else {
                    // Tie: higher slot wins, advance both.
                    const CellVersion *cv0 = c0->cell_version();
                    const CellVersion *cv1 = c1->cell_version();
                    uint64_t           s0  = cv0 != nullptr ? cv0->slot : 0;
                    uint64_t           s1  = cv1 != nullptr ? cv1->slot : 0;
                    if (s0 >= s1) {
                        winner_key = c0->key();
                        l0_winner  = cv0;
                    }
                    else {
                        winner_key = c1->key();
                        l0_winner  = cv1;
                    }
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                    c1->prefetch_next();
                    c1->advance();
                    if (!c1->valid()) {
                        --n_valid_l0;
                    }
                }
            }
            else {
                // 1 valid L0 + L1.
                if (c0 == nullptr) {
                    return Status::internal_error("scan cursor count disagrees with live sources");
                }
                int cmp = c0->key().compare(l1.key());
                if (cmp < 0) {
                    winner_key = c0->key();
                    l0_winner  = c0->cell_version();
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                }
                else if (cmp > 0) {
                    winner_key     = l1.key();
                    l1_winner_cell = l1.cell();
                    l1.next();
                }
                else {
                    // Tie: higher slot wins, advance both.
                    const CellVersion *cv = c0->cell_version();
                    uint64_t           s0 = cv != nullptr ? cv->slot : 0;
                    uint64_t           s1 = CellView{l1.cell()}.slot();
                    if (s0 >= s1) {
                        winner_key = c0->key();
                        l0_winner  = cv;
                    }
                    else {
                        winner_key     = l1.key();
                        l1_winner_cell = l1.cell();
                    }
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                    l1.next();
                }
            }
        }
        else {
            // Loser tree (k > 2): O(log k) per merge step.
            if (!lt_built) {
                lt_sources.clear();
                lt_sources.reserve(l0.size() + 1);
                for (auto &c : l0) {
                    lt_sources.push_back({.kind = MergeSource::kL0, .l0 = &c.cur, .l1 = nullptr});
                }
                lt_sources.push_back({.kind = MergeSource::kL1, .l0 = nullptr, .l1 = &l1});
                lt.init(lt_sources.data(), static_cast<int>(lt_sources.size()));
                lt_built = true;
            }
            else if (l1_refilled) {
                // L1 refilled (new leaf): replay L1 in the tree.
                lt.replay_source(static_cast<int>(lt_sources.size() - 1));
            }
            if (!lt.winner_valid()) {
                break;
            }
            int w      = lt.winner();
            winner_key = lt_sources[w].key();
            // Capture winner's cell BEFORE advancing.
            if (lt_sources[w].kind == MergeSource::kL0) {
                l0_winner = lt_sources[w].l0->cell_version();
            }
            else {
                l1_winner_cell = lt_sources[w].l1->cell();
            }
            // Prefetch + advance the winner.
            lt_sources[w].prefetch_next();
            lt.advance_winner();
            if (lt_sources[w].kind == MergeSource::kL0 && !lt_sources[w].valid()) {
                --n_valid_l0;
            }
            // Collision drain: advance all other sources on the same key
            // (duplicate — already emitted). After the winner advances, any
            // other cursor on the same key naturally bubbles to the root.
            while (lt.winner_valid() && lt_sources[lt.winner()].key().compare(winner_key) == 0) {
                int cw = lt.winner();
                lt_sources[cw].prefetch_next();
                lt.drain_winner();
                if (lt_sources[cw].kind == MergeSource::kL0 && !lt_sources[cw].valid()) {
                    --n_valid_l0;
                }
            }
        }

        // Materialize the winning cell for the consider lambda. L1: borrow
        // directly. L0: materialize a contiguous cell (only for the winner).
        Slice winner_cell;
        if (l0_winner != nullptr) {
            if (l0_winner->cell.ownership() != buffer::mode::kExternal) {
                winner_cell = l0_winner->cell.slice();
            }
            else {
                // Split cell (R30): build the contiguous [header][value].
                size_t vlen     = l0_winner->cell.size();
                l0_materialized = buffer::alloc(vlen, kCellHeaderSize);
                uint8_t *p      = l0_materialized.data();
                for (int i = 0; i < 8; ++i) {
                    p[i] = static_cast<uint8_t>((l0_winner->slot >> (8 * i)) & 0xff);
                }
                p[8] = l0_winner->flags;
                if (vlen > 0) {
                    std::memcpy(p + kCellHeaderSize, l0_winner->cell.data(), vlen);
                }
                winner_cell = l0_materialized.slice();
            }
        }
        else {
            winner_cell = l1_winner_cell;
        }

        // Early stop: every stream is non-decreasing, so once a key has moved
        // past the prefix range (not merely before it), no later key can match.
        if (!prefix.empty() && !winner_key.starts_with(prefix) && winner_key.compare(prefix) > 0) {
            break;
        }
        // Exclusive upper bound: once the winner reaches end_key, no later key
        // can be < end_key (streams are non-decreasing), so stop.
        if (!end_key.empty() && winner_key.compare(end_key) >= 0) {
            break;
        }
        if (!consider(winner_key, winner_cell)) {
            break;
        }
    }
    auto loop_ns = dur_ns(t_loop);
    // merge = loop overhead excluding L1 work (refill bookkeeping + min-key
    // select + winner + decode).
    uint64_t merge_ns = (loop_ns > l1_ns) ? loop_ns - l1_ns : 0;
    uint64_t total_ns = dur_ns(t_total);
    if (metrics_.scan_l != nullptr) {
        metrics_.scan_entries_c->inc_by(packed_count);
        metrics_.scan_l->observe(total_ns);
        metrics_.scan_l0_l->observe(l0_ns);
        metrics_.scan_l1_l->observe(l1_ns);
        metrics_.scan_merge_l->observe(merge_ns);
    }
    if (out_count != nullptr) {
        *out_count = packed_count;
    }
    return Status::Ok();
}

Status Crowdbtree::seek_reverse(Slice start_key, bool inclusive, Slice begin_key, scan_entry *out, bool *found) const
{
    auto generation = generation_.enter();
    if (out == nullptr || found == nullptr) {
        return Status::invalid_argument("seek_reverse output is null");
    }
    *out                             = {};
    EpochManager::Guard guard        = epoch_.enter();
    auto                memtables    = all_memtables();
    const uint64_t      root_page_id = root_page_id_.load();
    const uint64_t      gc_floor     = gc_floor_.load();
    *found = seek_reverse_guarded(start_key, true, inclusive, begin_key, memtables, root_page_id, gc_floor, out);
    return Status::Ok();
}

bool Crowdbtree::seek_reverse_guarded(Slice start_key, bool has_start_bound, bool inclusive, Slice begin_key,
                                      const std::vector<MemTableSource> &memtables, uint64_t root_page_id,
                                      uint64_t gc_floor, scan_entry *out) const
{

    struct ParentStep
    {
        InnerBase *page;
        size_t     child_index;
    };

    auto resolve_base = [](PageBase *head) {
        while (head != nullptr && head->type == page_type::kBatchDelta) {
            head = head->next;
        }
        return head;
    };
    auto l1_predecessor = [&](Slice bound, bool has_bound, bool include, Slice *key, Slice *cell) -> bool {
        std::vector<ParentStep> path;
        uint64_t                page_id = root_page_id;
        while (page_id != kInvalidPageId) {
            PageBase *head = resident(page_id);
            PageBase *base = resolve_base(head);
            if (base == nullptr) {
                return false;
            }
            if (base->type == page_type::kLeafBase) {
                LeafChainCursor cursor(head, gc_floor);
                if (has_bound) {
                    cursor.seek_reverse(bound, include);
                }
                else {
                    cursor.seek_last();
                }
                if (cursor.valid()) {
                    *key  = cursor.key();
                    *cell = cursor.cell();
                    return true;
                }
                break;
            }
            auto  *inner = static_cast<InnerBase *>(base);
            size_t index = has_bound ? inner->child_index_for(bound) : inner->num_children() - 1;
            path.push_back({.page = inner, .child_index = index});
            page_id = inner->child_at(index);
        }

        while (!path.empty()) {
            ParentStep step = path.back();
            path.pop_back();
            if (step.child_index == 0) {
                continue;
            }
            page_id = step.page->child_at(step.child_index - 1);
            while (page_id != kInvalidPageId) {
                PageBase *head = resident(page_id);
                PageBase *base = resolve_base(head);
                if (base == nullptr) {
                    return false;
                }
                if (base->type == page_type::kLeafBase) {
                    LeafChainCursor cursor(head, gc_floor);
                    cursor.seek_last();
                    if (cursor.valid()) {
                        *key  = cursor.key();
                        *cell = cursor.cell();
                        return true;
                    }
                    break;
                }
                auto  *inner = static_cast<InnerBase *>(base);
                size_t index = inner->num_children() - 1;
                path.push_back({.page = inner, .child_index = index});
                page_id = inner->child_at(index);
            }
        }
        return false;
    };

    std::string bound         = start_key.to_string();
    bool        has_bound     = has_start_bound;
    bool        include_bound = inclusive;
    while (true) {
        Slice              winner_key;
        const CellVersion *winner_l0 = nullptr;
        Slice              winner_l1;
        bool               have_winner = false;
        for (const auto &memtable : memtables) {
            auto cursor = memtable->cursor_reverse(Slice(bound), has_bound, include_bound);
            if (!cursor.valid()) {
                continue;
            }
            uint64_t slot        = cursor.cell_version() != nullptr ? cursor.cell_version()->slot : 0;
            uint64_t winner_slot = 0;
            if (winner_l0 != nullptr) {
                winner_slot = winner_l0->slot;
            }
            else if (!winner_l1.empty()) {
                winner_slot = CellView{winner_l1}.slot();
            }
            int comparison = have_winner ? cursor.key().compare(winner_key) : 1;
            if (comparison > 0 || (comparison == 0 && slot > winner_slot)) {
                winner_key  = cursor.key();
                winner_l0   = cursor.cell_version();
                winner_l1   = {};
                have_winner = true;
            }
        }

        Slice l1_key;
        Slice l1_cell;
        if (l1_predecessor(Slice(bound), has_bound, include_bound, &l1_key, &l1_cell)) {
            uint64_t l1_slot     = CellView{l1_cell}.slot();
            uint64_t winner_slot = 0;
            if (winner_l0 != nullptr) {
                winner_slot = winner_l0->slot;
            }
            else if (!winner_l1.empty()) {
                winner_slot = CellView{winner_l1}.slot();
            }
            int comparison = have_winner ? l1_key.compare(winner_key) : 1;
            if (comparison > 0 || (comparison == 0 && l1_slot > winner_slot)) {
                winner_key  = l1_key;
                winner_l0   = nullptr;
                winner_l1   = l1_cell;
                have_winner = true;
            }
        }
        if (!have_winner || (!begin_key.empty() && winner_key.compare(begin_key) < 0) ||
            opt_.key_range.before(winner_key)) {
            return false;
        }
        if (opt_.key_range.at_or_after_end(winner_key)) {
            bound         = winner_key.to_string();
            has_bound     = true;
            include_bound = false;
            continue;
        }

        buffer materialized;
        Slice  winner_cell = winner_l1;
        if (winner_l0 != nullptr) {
            if (winner_l0->cell.ownership() != buffer::mode::kExternal) {
                winner_cell = winner_l0->cell.slice();
            }
            else {
                size_t value_len = winner_l0->cell.size();
                materialized     = buffer::alloc(value_len, kCellHeaderSize);
                uint8_t *data    = materialized.data();
                for (int i = 0; i < 8; ++i) {
                    data[i] = static_cast<uint8_t>((winner_l0->slot >> (8 * i)) & 0xff);
                }
                data[8] = winner_l0->flags;
                if (value_len > 0) {
                    std::memcpy(data + kCellHeaderSize, winner_l0->cell.data(), value_len);
                }
                winner_cell = materialized.slice();
            }
        }
        CellView value{winner_cell};
        if (value.is_tombstone()) {
            bound         = winner_key.to_string();
            has_bound     = true;
            include_bound = false;
            continue;
        }
        out->key       = winner_key.to_string();
        out->slot      = value.slot();
        out->value     = value.is_overflow() ? assemble_overflow_value(value.overflow_head(), value.overflow_len())
                                             : value.value().to_string();
        out->tombstone = false;
        return true;
    }
}

Status Crowdbtree::scan_reverse(Slice start_key, bool has_start_bound, bool start_inclusive, Slice begin_key,
                                size_t limit, size_t byte_budget, std::vector<scan_entry> *out, bool *truncated) const
{
    auto generation = generation_.enter();
    if (out == nullptr || truncated == nullptr) {
        return Status::invalid_argument("scan_reverse output is null");
    }
    out->clear();
    *truncated                            = false;
    EpochManager::Guard guard             = epoch_.enter();
    auto                memtables         = all_memtables();
    const uint64_t      root_page_id      = root_page_id_.load();
    const uint64_t      gc_floor          = gc_floor_.load();
    std::string         bound             = start_key.to_string();
    bool                has_bound         = has_start_bound;
    bool                inclusive         = start_inclusive;
    size_t              accumulated_bytes = 0;
    while (true) {
        scan_entry entry;
        if (!seek_reverse_guarded(Slice(bound), has_bound, inclusive, begin_key, memtables, root_page_id, gc_floor,
                                  &entry)) {
            break;
        }
        size_t entry_bytes = entry.key.size() + entry.value.size();
        if ((limit != 0 && out->size() >= limit) ||
            (byte_budget != 0 && !out->empty() && accumulated_bytes + entry_bytes > byte_budget)) {
            *truncated = true;
            break;
        }
        bound     = entry.key;
        has_bound = true;
        inclusive = false;
        accumulated_bytes += entry_bytes;
        out->push_back(std::move(entry));
    }
    return Status::Ok();
}

bool Crowdbtree::try_scan_reverse_no_load(Slice prefix, Slice start_after, Slice end_key, size_t limit,
                                          size_t byte_budget, bool keys_only, uint64_t deadline_ms,
                                          ScanPackedBuf *out_packed, size_t *out_count, bool *truncated,
                                          uint64_t *out_pending_page_id) const
{
    auto generation      = generation_.enter();
    *out_packed          = ScanPackedBuf{};
    *out_count           = 0;
    *truncated           = false;
    *out_pending_page_id = kInvalidPageId;

    EpochManager::Guard guard        = epoch_.enter();
    auto                memtables    = all_memtables();
    const uint64_t      root_page_id = root_page_id_.load();
    const uint64_t      gc_floor     = gc_floor_.load();
    uint64_t            blocked      = kInvalidPageId;

    auto probe = [this, &blocked](uint64_t page_id) -> PageBase * {
        uint64_t word = mapping_.get_word(page_id);
        if (slot_word::is_empty(word)) {
            return nullptr;
        }
        if (slot_word::is_unloaded(word)) {
            blocked = page_id;
            return nullptr;
        }
        PageBase *page = slot_word::resident_ptr(word);
        page->last_touch_tick.store(touch_tick_.fetch_add(1, std::memory_order_relaxed), std::memory_order_relaxed);
        return page;
    };
    auto resolve_base = [](PageBase *head) {
        while (head != nullptr && head->type == page_type::kBatchDelta) {
            head = head->next;
        }
        return head;
    };

    struct ParentStep
    {
        InnerBase *page;
        size_t     child_index;
    };

    auto l1_predecessor = [&](Slice bound, bool has_bound, Slice *key, Slice *cell) -> bool {
        std::vector<ParentStep> path;
        uint64_t                page_id = root_page_id;
        while (page_id != kInvalidPageId) {
            PageBase *head = probe(page_id);
            if (blocked != kInvalidPageId) {
                return false;
            }
            PageBase *base = resolve_base(head);
            if (base == nullptr) {
                return false;
            }
            if (base->type == page_type::kLeafBase) {
                LeafChainCursor cursor(head, gc_floor);
                if (has_bound) {
                    cursor.seek_reverse(bound, false);
                }
                else {
                    cursor.seek_last();
                }
                if (cursor.valid()) {
                    *key  = cursor.key();
                    *cell = cursor.cell();
                    return true;
                }
                break;
            }
            auto  *inner = static_cast<InnerBase *>(base);
            size_t index = has_bound ? inner->child_index_for(bound) : inner->num_children() - 1;
            path.push_back({.page = inner, .child_index = index});
            page_id = inner->child_at(index);
        }
        while (!path.empty()) {
            ParentStep step = path.back();
            path.pop_back();
            if (step.child_index == 0) {
                continue;
            }
            page_id = step.page->child_at(step.child_index - 1);
            while (page_id != kInvalidPageId) {
                PageBase *head = probe(page_id);
                if (blocked != kInvalidPageId) {
                    return false;
                }
                PageBase *base = resolve_base(head);
                if (base == nullptr) {
                    return false;
                }
                if (base->type == page_type::kLeafBase) {
                    LeafChainCursor cursor(head, gc_floor);
                    cursor.seek_last();
                    if (cursor.valid()) {
                        *key  = cursor.key();
                        *cell = cursor.cell();
                        return true;
                    }
                    break;
                }
                auto  *inner = static_cast<InnerBase *>(base);
                size_t index = inner->num_children() - 1;
                path.push_back({.page = inner, .child_index = index});
                page_id = inner->child_at(index);
            }
        }
        return false;
    };

    std::string bound;
    bool        has_bound = false;
    if (!start_after.empty()) {
        bound     = start_after.to_string();
        has_bound = true;
    }
    else if (!end_key.empty()) {
        bound     = end_key.to_string();
        has_bound = true;
    }
    else if (!prefix.empty()) {
        bound = prefix.to_string();
        for (size_t i = bound.size(); i > 0; --i) {
            auto byte = static_cast<uint8_t>(bound[i - 1]);
            if (byte != 0xff) {
                bound[i - 1] = static_cast<char>(byte + 1);
                bound.resize(i);
                has_bound = true;
                break;
            }
        }
    }

    size_t accumulated_bytes = 0;
    size_t deadline_counter  = 0;
    while (true) {
        if (deadline_ms != 0 && ++deadline_counter >= 1024) {
            deadline_counter = 0;
            auto now_ms      = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                         std::chrono::system_clock::now().time_since_epoch())
                                                         .count());
            if (now_ms >= deadline_ms) {
                *truncated = true;
                return true;
            }
        }

        Slice              winner_key;
        const CellVersion *winner_l0 = nullptr;
        Slice              winner_l1;
        bool               have_winner = false;
        for (const auto &memtable : memtables) {
            auto cursor = memtable->cursor_reverse(Slice(bound), has_bound, false);
            if (!cursor.valid()) {
                continue;
            }
            uint64_t slot        = cursor.cell_version() != nullptr ? cursor.cell_version()->slot : 0;
            uint64_t winner_slot = 0;
            if (winner_l0 != nullptr) {
                winner_slot = winner_l0->slot;
            }
            else if (!winner_l1.empty()) {
                winner_slot = CellView{winner_l1}.slot();
            }
            int comparison = have_winner ? cursor.key().compare(winner_key) : 1;
            if (comparison > 0 || (comparison == 0 && slot > winner_slot)) {
                winner_key  = cursor.key();
                winner_l0   = cursor.cell_version();
                winner_l1   = {};
                have_winner = true;
            }
        }
        Slice l1_key;
        Slice l1_cell;
        if (l1_predecessor(Slice(bound), has_bound, &l1_key, &l1_cell)) {
            uint64_t l1_slot     = CellView{l1_cell}.slot();
            uint64_t winner_slot = 0;
            if (winner_l0 != nullptr) {
                winner_slot = winner_l0->slot;
            }
            else if (!winner_l1.empty()) {
                winner_slot = CellView{winner_l1}.slot();
            }
            int comparison = have_winner ? l1_key.compare(winner_key) : 1;
            if (comparison > 0 || (comparison == 0 && l1_slot > winner_slot)) {
                winner_key  = l1_key;
                winner_l0   = nullptr;
                winner_l1   = l1_cell;
                have_winner = true;
            }
        }
        if (blocked != kInvalidPageId) {
            *out_pending_page_id = blocked;
            return false;
        }
        if (!have_winner || opt_.key_range.before(winner_key)) {
            return true;
        }
        bound     = winner_key.to_string();
        has_bound = true;

        if ((!end_key.empty() && winner_key.compare(end_key) >= 0) || opt_.key_range.at_or_after_end(winner_key)) {
            continue;
        }
        if (!prefix.empty() && !winner_key.starts_with(prefix)) {
            if (winner_key.compare(prefix) < 0) {
                return true;
            }
            continue;
        }

        buffer materialized;
        Slice  winner_cell = winner_l1;
        if (winner_l0 != nullptr) {
            if (winner_l0->cell.ownership() != buffer::mode::kExternal) {
                winner_cell = winner_l0->cell.slice();
            }
            else {
                size_t value_len = winner_l0->cell.size();
                materialized     = buffer::alloc(value_len, kCellHeaderSize);
                uint8_t *data    = materialized.data();
                for (int i = 0; i < 8; ++i) {
                    data[i] = static_cast<uint8_t>((winner_l0->slot >> (8 * i)) & 0xff);
                }
                data[8] = winner_l0->flags;
                if (value_len > 0) {
                    std::memcpy(data + kCellHeaderSize, winner_l0->cell.data(), value_len);
                }
                winner_cell = materialized.slice();
            }
        }
        CellView value{winner_cell};
        if (value.is_tombstone()) {
            continue;
        }
        if (limit != 0 && *out_count >= limit) {
            *truncated = true;
            return true;
        }
        std::string val;
        if (!keys_only) {
            val = value.is_overflow() ? assemble_overflow_value(value.overflow_head(), value.overflow_len())
                                      : value.value().to_string();
        }
        size_t entry_bytes = winner_key.size() + val.size();
        if (byte_budget != 0 && *out_count > 0 && accumulated_bytes + entry_bytes > byte_budget) {
            *truncated = true;
            return true;
        }
        out_packed->pack_u32(static_cast<uint32_t>(winner_key.size()));
        out_packed->append(winner_key);
        out_packed->pack_u64(value.slot());
        out_packed->push_back(0);
        out_packed->pack_u32(static_cast<uint32_t>(val.size()));
        out_packed->append(val);
        ++*out_count;
        accumulated_bytes += entry_bytes;
    }
}

bool Crowdbtree::try_scan_no_load(
    Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget, bool keys_only,
    uint64_t                 deadline_ms,
    std::vector<scan_entry> *out, // NOLINT(readability-non-const-parameter) written to via push_back
    bool *truncated, uint64_t *out_pending_page_id, ScanPackedBuf *out_packed,
    size_t *out_count) // NOLINT(readability-non-const-parameter) written to via *out_count
    const
{
    auto generation = generation_.enter();
    if (out == nullptr && out_packed == nullptr) {
        return false;
    }
    if (out != nullptr) {
        out->clear();
    }
    if (out_packed != nullptr) {
        *out_packed = ScanPackedBuf{};
    }
    size_t packed_count = 0;
    if (truncated != nullptr) {
        *truncated = false;
    }
    EpochManager::Guard guard = epoch_.enter();

    auto dur_ns = [](std::chrono::steady_clock::time_point from) {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - from).count());
    };
    auto t_total = std::chrono::steady_clock::now();

    // L0: identical to scan() -- never touches the page store. R50: lock-free
    // cursor, no snapshot copy.
    struct L0Cursor
    {
        ConcurrentSkipList::Cursor cur;
    };

    auto                  t0 = std::chrono::steady_clock::now();
    std::vector<L0Cursor> l0;
    auto                  sources_owner = all_memtables();
    l0.reserve(sources_owner.size());
    for (auto &mt : sources_owner) {
        l0.push_back({.cur = mt->cursor(start_after)});
    }
    uint64_t l0_ns = dur_ns(t0);

    // Non-blocking probe (mirrors try_get_view_no_load's `probe`): bails out
    // on an unloaded slot instead of demand-loading it, recording which
    // page_id via `blocked_page_id`.
    uint64_t blocked_page_id = kInvalidPageId;
    auto     probe           = [this, &blocked_page_id](uint64_t p) -> PageBase               *{
        uint64_t w = mapping_.get_word(p);
        if (slot_word::is_empty(w)) {
            return nullptr;
        }
        if (slot_word::is_unloaded(w)) {
            blocked_page_id = p;
            return nullptr;
        }
        PageBase *v = slot_word::resident_ptr(w);
        v->last_touch_tick.store(touch_tick_.fetch_add(1, std::memory_order_relaxed), std::memory_order_relaxed);
        return v;
    };

    uint64_t l1_ns   = 0;
    uint64_t gc      = gc_floor_.load();
    uint64_t page_id = root_page_id_.load();
    // Descend at the cursor when present, else at the prefix start -- see
    // scan()'s own comment.
    Slice descend_key = !start_after.empty() ? start_after : prefix;
    if (page_id != kInvalidPageId) {
        t0      = std::chrono::steady_clock::now();
        page_id = find_leaf_page_id(probe, page_id, descend_key);
        if (blocked_page_id != kInvalidPageId) {
            *out_pending_page_id = blocked_page_id;
            return false; // genuine miss on the initial descent
        }
        l1_ns += page_id != kInvalidPageId ? dur_ns(t0) : 0;
    }
    // Lazy leaf cursor -- see scan()'s own comment.
    LeafChainCursor l1;
    bool            first_leaf = true;

    auto refill_l1 = [&]() -> bool {
        while (!l1.valid() && page_id != kInvalidPageId) {
            PageBase *head = probe(page_id);
            if (blocked_page_id != kInvalidPageId) {
                return false; // caller checks blocked_page_id, distinct from "chain exhausted"
            }
            if (head == nullptr) {
                page_id = kInvalidPageId;
                break;
            }
            auto rt = std::chrono::steady_clock::now();
            l1.reset(head, gc);
            if (first_leaf && !descend_key.empty()) {
                l1.seek(descend_key, /*exclusive=*/!start_after.empty());
            }
            first_leaf = false;
            l1_ns += dur_ns(rt);
            LeafBase *base = chain_leaf_base(head);
            page_id        = base != nullptr ? base->right_sibling() : kInvalidPageId;
            // R58: prefetch the right-sibling leaf (see scan()'s refill_l1).
            if (page_id != kInvalidPageId) {
                uint64_t w = mapping_.get_word(page_id);
                if (slot_word::is_resident(w)) {
                    __builtin_prefetch(slot_word::resident_ptr(w), 0, 2);
                }
            }
        }
        return l1.valid();
    };

    size_t accumulated_bytes = 0;
    auto   consider          = [&](Slice key, Slice cell) -> bool {
        if (opt_.key_range.before(key)) {
            return true;
        }
        if (opt_.key_range.at_or_after_end(key)) {
            return false;
        }
        if (!start_after.empty() && key.compare(start_after) <= 0) {
            return true; // cursor: skip keys <= start_after (exclusive lower bound)
        }
        if (!key.starts_with(prefix)) {
            return true;
        }
        CellView v{cell};
        if (v.is_tombstone()) {
            return true;
        }
        assert(out_packed != nullptr || out != nullptr);
        size_t cur_count = out_packed != nullptr ? packed_count : out->size();
        if (limit != 0 && cur_count >= limit) {
            if (truncated != nullptr) {
                *truncated = true;
            }
            return false;
        }
        std::string val;
        if (!keys_only) {
            val =
                v.is_overflow() ? assemble_overflow_value(v.overflow_head(), v.overflow_len()) : v.value().to_string();
        }
        size_t key_size    = key.size();
        size_t value_size  = val.size();
        size_t entry_bytes = key_size + value_size;
        if (byte_budget != 0 && cur_count > 0 && accumulated_bytes + entry_bytes > byte_budget) {
            if (truncated != nullptr) {
                *truncated = true;
            }
            return false; // byte budget would be exceeded; keep what we have
        }
        if (out_packed != nullptr) {
            out_packed->pack_u32(static_cast<uint32_t>(key_size));
            out_packed->append(key);
            out_packed->pack_u64(v.slot());
            out_packed->push_back(0);
            out_packed->pack_u32(static_cast<uint32_t>(value_size));
            out_packed->append(val);
        }
        else {
            out->push_back({.key = key.to_string(), .slot = v.slot(), .value = std::move(val)});
        }
        ++packed_count;
        accumulated_bytes += entry_bytes;
        if (byte_budget != 0 && entry_bytes > byte_budget) {
            CRB_LOG_WARN("[{}] scan: oversized entry key_size={} value_size={} exceeds byte_budget={}", name_, key_size,
                         value_size, byte_budget);
        }
        return true;
    };

    // R58: merge loop with 2-source fast path + loser tree (same structure as
    // scan()'s — see the comment there). The only difference is the
    // blocked_page_id early-return on a cold leaf.
    size_t n_valid_l0 = 0;
    for (const auto &c : l0) {
        if (c.cur.valid()) {
            ++n_valid_l0;
        }
    }

    LoserTree                lt;
    std::vector<MergeSource> lt_sources;
    bool                     lt_built = false;

    auto   t_loop           = std::chrono::steady_clock::now();
    size_t deadline_counter = 0; // check deadline every kDeadlineCheckInterval entries
    while (true) {
        // Periodic deadline check (same as scan()'s).
        if (deadline_ms != 0 && ++deadline_counter >= 1024) {
            deadline_counter = 0;
            auto now_ms      = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                         std::chrono::system_clock::now().time_since_epoch())
                                                         .count());
            if (now_ms >= deadline_ms) {
                if (truncated != nullptr) {
                    *truncated = true;
                }
                return true; // resolved with partial result
            }
        }
        bool l1_was_valid_before = l1.valid();
        bool have_l1             = refill_l1();
        if (blocked_page_id != kInvalidPageId) {
            *out_pending_page_id = blocked_page_id;
            return false; // genuine miss mid-walk
        }
        bool l1_refilled = have_l1 && !l1_was_valid_before;

        size_t n_sources = n_valid_l0 + (have_l1 ? 1 : 0);
        if (n_sources == 0) {
            break;
        }

        Slice              winner_key;
        const CellVersion *l0_winner = nullptr;
        Slice              l1_winner_cell;
        buffer             l0_materialized;

        if (n_sources == 1) {
            // Single source: no merge compare needed.
            if (have_l1) {
                winner_key     = l1.key();
                l1_winner_cell = l1.cell();
                l1.next();
            }
            else {
                for (auto &c : l0) {
                    if (!c.cur.valid()) {
                        continue;
                    }
                    winner_key = c.cur.key();
                    l0_winner  = c.cur.cell_version();
                    c.cur.prefetch_next();
                    c.cur.advance();
                    if (!c.cur.valid()) {
                        --n_valid_l0;
                    }
                    break;
                }
            }
        }
        else if (n_sources == 2) {
            // 2-source fast path: 1 compare instead of 2×2.
            ConcurrentSkipList::Cursor *c0 = nullptr;
            ConcurrentSkipList::Cursor *c1 = nullptr;
            for (auto &c : l0) {
                if (!c.cur.valid()) {
                    continue;
                }
                if (c0 == nullptr) {
                    c0 = &c.cur;
                }
                else {
                    c1 = &c.cur;
                    break;
                }
            }
            if (n_valid_l0 == 2) {
                int cmp = c0->key().compare(c1->key());
                if (cmp < 0) {
                    winner_key = c0->key();
                    l0_winner  = c0->cell_version();
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                }
                else if (cmp > 0) {
                    winner_key = c1->key();
                    l0_winner  = c1->cell_version();
                    c1->prefetch_next();
                    c1->advance();
                    if (!c1->valid()) {
                        --n_valid_l0;
                    }
                }
                else {
                    const CellVersion *cv0 = c0->cell_version();
                    const CellVersion *cv1 = c1->cell_version();
                    uint64_t           s0  = cv0 != nullptr ? cv0->slot : 0;
                    uint64_t           s1  = cv1 != nullptr ? cv1->slot : 0;
                    if (s0 >= s1) {
                        winner_key = c0->key();
                        l0_winner  = cv0;
                    }
                    else {
                        winner_key = c1->key();
                        l0_winner  = cv1;
                    }
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                    c1->prefetch_next();
                    c1->advance();
                    if (!c1->valid()) {
                        --n_valid_l0;
                    }
                }
            }
            else {
                int cmp = c0->key().compare(l1.key());
                if (cmp < 0) {
                    winner_key = c0->key();
                    l0_winner  = c0->cell_version();
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                }
                else if (cmp > 0) {
                    winner_key     = l1.key();
                    l1_winner_cell = l1.cell();
                    l1.next();
                }
                else {
                    const CellVersion *cv = c0->cell_version();
                    uint64_t           s0 = cv != nullptr ? cv->slot : 0;
                    uint64_t           s1 = CellView{l1.cell()}.slot();
                    if (s0 >= s1) {
                        winner_key = c0->key();
                        l0_winner  = cv;
                    }
                    else {
                        winner_key     = l1.key();
                        l1_winner_cell = l1.cell();
                    }
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                    l1.next();
                }
            }
        }
        else {
            // Loser tree (k > 2): O(log k) per merge step.
            if (!lt_built) {
                lt_sources.clear();
                lt_sources.reserve(l0.size() + 1);
                for (auto &c : l0) {
                    lt_sources.push_back({.kind = MergeSource::kL0, .l0 = &c.cur, .l1 = nullptr});
                }
                lt_sources.push_back({.kind = MergeSource::kL1, .l0 = nullptr, .l1 = &l1});
                lt.init(lt_sources.data(), static_cast<int>(lt_sources.size()));
                lt_built = true;
            }
            else if (l1_refilled) {
                lt.replay_source(static_cast<int>(lt_sources.size() - 1));
            }
            if (!lt.winner_valid()) {
                break;
            }
            int w      = lt.winner();
            winner_key = lt_sources[w].key();
            if (lt_sources[w].kind == MergeSource::kL0) {
                l0_winner = lt_sources[w].l0->cell_version();
            }
            else {
                l1_winner_cell = lt_sources[w].l1->cell();
            }
            lt_sources[w].prefetch_next();
            lt.advance_winner();
            if (lt_sources[w].kind == MergeSource::kL0 && !lt_sources[w].valid()) {
                --n_valid_l0;
            }
            while (lt.winner_valid() && lt_sources[lt.winner()].key().compare(winner_key) == 0) {
                int cw = lt.winner();
                lt_sources[cw].prefetch_next();
                lt.drain_winner();
                if (lt_sources[cw].kind == MergeSource::kL0 && !lt_sources[cw].valid()) {
                    --n_valid_l0;
                }
            }
        }

        Slice winner_cell;
        if (l0_winner != nullptr) {
            if (l0_winner->cell.ownership() != buffer::mode::kExternal) {
                winner_cell = l0_winner->cell.slice();
            }
            else {
                size_t vlen     = l0_winner->cell.size();
                l0_materialized = buffer::alloc(vlen, kCellHeaderSize);
                uint8_t *p      = l0_materialized.data();
                for (int i = 0; i < 8; ++i) {
                    p[i] = static_cast<uint8_t>((l0_winner->slot >> (8 * i)) & 0xff);
                }
                p[8] = l0_winner->flags;
                if (vlen > 0) {
                    std::memcpy(p + kCellHeaderSize, l0_winner->cell.data(), vlen);
                }
                winner_cell = l0_materialized.slice();
            }
        }
        else {
            winner_cell = l1_winner_cell;
        }

        if (!prefix.empty() && !winner_key.starts_with(prefix) && winner_key.compare(prefix) > 0) {
            break;
        }
        if (!end_key.empty() && winner_key.compare(end_key) >= 0) {
            break;
        }
        if (!consider(winner_key, winner_cell)) {
            break;
        }
    }
    auto     loop_ns  = dur_ns(t_loop);
    uint64_t merge_ns = (loop_ns > l1_ns) ? loop_ns - l1_ns : 0;
    uint64_t total_ns = dur_ns(t_total);
    if (metrics_.scan_l != nullptr) {
        metrics_.scan_entries_c->inc_by(packed_count);
        metrics_.scan_l->observe(total_ns);
        metrics_.scan_l0_l->observe(l0_ns);
        metrics_.scan_l1_l->observe(l1_ns);
        metrics_.scan_merge_l->observe(merge_ns);
    }
    if (out_count != nullptr) {
        *out_count = packed_count;
    }
    return true; // fully resolved
}

// Build the resume `start_after` for scan_async_attempt: if entries have
// been accumulated across prior cold-leaf retries, resume from the last
// resolved key; otherwise use the original start_after. This avoids
// re-traversing already-resolved leaves after a demand-load completes.
static std::shared_ptr<std::string> make_resume_after(const std::shared_ptr<std::string> &start_after_owned,
                                                      const std::shared_ptr<std::string> &last_key)
{
    if (last_key != nullptr && !last_key->empty()) {
        return last_key;
    }
    return start_after_owned;
}

void Crowdbtree::scan_async(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget,
                            bool keys_only, uint64_t deadline_ms,
                            std::function<void(Status, ScanPackedBuf, bool)> on_done) const
{
    // Copy the keys upfront: unlike scan()'s Slice (borrowed, valid only
    // for this one synchronous call), scan_async's keys must survive across
    // an arbitrary number of async round trips. `accumulated` collects
    // packed entries resolved before each cold leaf so retries resume from
    // the last resolved key instead of re-traversing already-resolved leaves.
    scan_async_attempt(std::make_shared<std::string>(prefix.to_string()),
                       std::make_shared<std::string>(start_after.to_string()),
                       std::make_shared<std::string>(end_key.to_string()), limit, byte_budget, keys_only, deadline_ms,
                       std::make_shared<ScanPackedBuf>(), nullptr, 0, std::move(on_done));
}

void Crowdbtree::scan_directional_async(Slice prefix, Slice start_after, Slice end_key, size_t limit,
                                        size_t byte_budget, bool keys_only, uint64_t deadline_ms, bool reverse,
                                        std::function<void(Status, ScanPackedBuf, bool)> on_done) const
{
    if (!reverse) {
        scan_async(prefix, start_after, end_key, limit, byte_budget, keys_only, deadline_ms, std::move(on_done));
        return;
    }
    scan_reverse_async_attempt(std::make_shared<std::string>(prefix.to_string()),
                               std::make_shared<std::string>(start_after.to_string()),
                               std::make_shared<std::string>(end_key.to_string()), limit, byte_budget, keys_only,
                               deadline_ms, std::make_shared<ScanPackedBuf>(), nullptr, 0, std::move(on_done));
}

// Extract the last key from a packed scan buffer (wire format:
// [u32 klen][key][u64 slot][u8 tombstone][u32 vlen][value] per entry).
// Used by scan_async_attempt to resume from the last resolved key.
static std::string last_key_from_packed(const uint8_t *data, size_t len)
{
    size_t      pos = 0;
    std::string last;
    while (pos + 4 <= len) {
        uint32_t klen = 0;
        for (int i = 0; i < 4; ++i) {
            klen |= static_cast<uint32_t>(data[pos + i]) << (8 * i);
        }
        pos += 4;
        if (pos + klen > len) {
            break;
        }
        last.assign(reinterpret_cast<const char *>(data + pos), klen);
        pos += klen;
        // skip slot (8) + tombstone (1)
        if (pos + 9 > len) {
            break;
        }
        pos += 9;
        if (pos + 4 > len) {
            break;
        }
        uint32_t vlen = 0;
        for (int i = 0; i < 4; ++i) {
            vlen |= static_cast<uint32_t>(data[pos + i]) << (8 * i);
        }
        pos += 4;
        pos += vlen;
    }
    return last;
}

static size_t payload_bytes_from_packed(const uint8_t *data, size_t len)
{
    size_t pos   = 0;
    size_t total = 0;
    while (pos + 4 <= len) {
        uint32_t key_len = 0;
        for (int i = 0; i < 4; ++i) {
            key_len |= static_cast<uint32_t>(data[pos + i]) << (8 * i);
        }
        pos += 4;
        if (pos + key_len + 13 > len) {
            break;
        }
        pos += key_len + 9;
        uint32_t value_len = 0;
        for (int i = 0; i < 4; ++i) {
            value_len |= static_cast<uint32_t>(data[pos + i]) << (8 * i);
        }
        pos += 4;
        if (pos + value_len > len) {
            break;
        }
        pos += value_len;
        total += key_len + value_len;
    }
    return total;
}

void Crowdbtree::scan_async_attempt(std::shared_ptr<std::string>        prefix_owned,
                                    const std::shared_ptr<std::string> &start_after_owned,
                                    const std::shared_ptr<std::string> &end_key_owned, size_t limit, size_t byte_budget,
                                    bool keys_only, uint64_t deadline_ms, std::shared_ptr<ScanPackedBuf> accumulated,
                                    std::shared_ptr<std::string> last_key, size_t accumulated_count,
                                    std::function<void(Status, ScanPackedBuf, bool)> on_done) const
{
    // Adjust the byte budget by entries already accumulated across prior
    // cold-leaf retries, mirroring the remaining_limit adjustment below.
    size_t accumulated_bytes = payload_bytes_from_packed(accumulated->data(), accumulated->size());
    if (byte_budget != 0 && accumulated_bytes >= byte_budget && accumulated_count > 0) {
        on_done(Status::Ok(), std::move(*accumulated), true);
        return;
    }
    size_t remaining_byte_budget = (byte_budget != 0) ? byte_budget - accumulated_bytes : 0;

    // Deadline check before each retry: if exceeded, deliver the accumulated
    // partial result with truncated = true instead of starting another attempt.
    if (deadline_ms != 0) {
        auto now_ms = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count());
        if (now_ms >= deadline_ms) {
            on_done(Status::Ok(), std::move(*accumulated), true);
            return;
        }
    }

    ScanPackedBuf out_packed;
    size_t        out_count       = 0;
    bool          truncated       = false;
    uint64_t      pending_page_id = kInvalidPageId;
    if (try_scan_no_load(Slice(*prefix_owned), Slice(*start_after_owned), Slice(*end_key_owned), limit,
                         remaining_byte_budget, keys_only, deadline_ms, nullptr, &truncated, &pending_page_id,
                         &out_packed, &out_count)) {
        // Append this attempt's packed entries to the accumulated buffer.
        if (out_count > 0) {
            accumulated->append(out_packed.data(), out_packed.size());
        }
        on_done(Status::Ok(), std::move(*accumulated), truncated);
        return;
    }

    // Cold leaf hit: append the entries resolved so far (those before the
    // cold leaf) to `accumulated`, then resume from the last resolved key
    // after the demand-load completes — no re-traversal of already-resolved
    // leaves. `out_packed` contains only entries before the cold page because
    // try_scan_no_load bails immediately on the first cold page.
    if (out_count > 0) {
        accumulated->append(out_packed.data(), out_packed.size());
        accumulated_count += out_count;
        last_key = std::make_shared<std::string>(last_key_from_packed(out_packed.data(), out_packed.size()));
    }
    // Adjust limit by the number of entries already accumulated so the
    // final result respects the caller's limit.
    size_t remaining_limit = (limit > accumulated_count) ? (limit - accumulated_count) : 0;

    if (opt_.async_page_store != nullptr) {
        uint64_t addr           = 0;
        uint32_t plen           = 0;
        bool     still_unloaded = false;
        uint64_t requested_word = 0;
        Status   location_status;
        {
            auto             generation = generation_.enter();
            std::scoped_lock lk(load_mutex_);
            uint64_t         w = mapping_.get_word(pending_page_id);
            if (slot_word::is_unloaded(w)) {
                requested_word  = w;
                location_status = opt_.page_store->decode_mapping_location(w, &addr, &plen);
                still_unloaded  = location_status.ok();
            }
        }
        if (!location_status.ok()) {
            io_failed_.store(true);
            on_done(location_status, ScanPackedBuf{}, false);
            return;
        }
        if (!still_unloaded) {
            // Another loader already resolved this page_id between the
            // lock-free probe above and this re-check -- retry, still here.
            // Resume from the last accumulated key (if any) to avoid
            // re-traversing already-resolved leaves.
            auto resume_after = make_resume_after(start_after_owned, last_key);
            if (metrics_.scan_retry_c != nullptr) {
                metrics_.scan_retry_c->inc();
            }
            scan_async_attempt(std::move(prefix_owned), resume_after, end_key_owned, remaining_limit, byte_budget,
                               keys_only, deadline_ms, std::move(accumulated), std::move(last_key), accumulated_count,
                               std::move(on_done));
            return;
        }
        uint32_t iu   = opt_.page_store->iu_size();
        auto     blob = std::make_shared<std::vector<uint8_t>>(round_up_to_iu(plen, iu));
        demand_load_total_.fetch_add(1, std::memory_order_relaxed);
        opt_.async_page_store->submit_read(
            addr, blob->data(), blob->size(),
            detail::own_async_completion([this, page_id = pending_page_id, requested_word, addr, plen, blob,
                                          prefix_owned, start_after_owned, end_key_owned, remaining_limit, byte_budget,
                                          keys_only, deadline_ms, accumulated, last_key, accumulated_count,
                                          on_done](Status st) mutable {
                if (!st.ok()) {
                    CRB_LOG_ERROR("[{}] scan_async: demand-load I/O fault: pid={} addr={} len={} status={}", name_,
                                  page_id, addr, plen, st.to_string());
                    io_failed_.store(true);
                    on_done(st, ScanPackedBuf{}, false);
                    return;
                }
                bool installed_ok = true;
                {
                    auto             generation = generation_.enter();
                    std::scoped_lock lk(load_mutex_);
                    uint64_t         w = mapping_.get_word(page_id);
                    if (w == requested_word) {
                        installed_ok = install_loaded_page(page_id, addr, plen, *blob) != nullptr;
                    }
                }
                if (!installed_ok) {
                    on_done(Status::io_error("scan_async: demand-load decode/CRC failure"), ScanPackedBuf{}, false);
                    return;
                }
                // Resume from the last accumulated key to avoid
                // re-traversing already-resolved leaves.
                auto resume_after = make_resume_after(start_after_owned, last_key);
                if (metrics_.scan_retry_c != nullptr) {
                    metrics_.scan_retry_c->inc();
                }
                scan_async_attempt(std::move(prefix_owned), resume_after, end_key_owned, remaining_limit, byte_budget,
                                   keys_only, deadline_ms, std::move(accumulated), std::move(last_key),
                                   accumulated_count, std::move(on_done));
            }));
        return;
    }
    // No async backend wired -- fall back to the existing synchronous
    // demand-load and retry, still on this same thread.
    if (metrics_.scan_retry_c != nullptr) {
        metrics_.scan_retry_c->inc();
    }
    {
        auto generation = generation_.enter();
        (void)resident(pending_page_id);
    }
    auto resume_after = make_resume_after(start_after_owned, last_key);
    scan_async_attempt(std::move(prefix_owned), resume_after, end_key_owned, remaining_limit, byte_budget, keys_only,
                       deadline_ms, std::move(accumulated), std::move(last_key), accumulated_count, std::move(on_done));
}

void Crowdbtree::scan_reverse_async_attempt(std::shared_ptr<std::string>        prefix_owned,
                                            const std::shared_ptr<std::string> &start_after_owned,
                                            const std::shared_ptr<std::string> &end_key_owned, size_t limit,
                                            size_t byte_budget, bool keys_only, uint64_t deadline_ms,
                                            std::shared_ptr<ScanPackedBuf> accumulated,
                                            std::shared_ptr<std::string> last_key, size_t accumulated_count,
                                            std::function<void(Status, ScanPackedBuf, bool)> on_done) const
{
    size_t accumulated_bytes = payload_bytes_from_packed(accumulated->data(), accumulated->size());
    if (byte_budget != 0 && accumulated_bytes >= byte_budget && accumulated_count > 0) {
        on_done(Status::Ok(), std::move(*accumulated), true);
        return;
    }
    if (limit != 0 && accumulated_count >= limit) {
        on_done(Status::Ok(), std::move(*accumulated), true);
        return;
    }
    size_t remaining_byte_budget = byte_budget != 0 ? byte_budget - accumulated_bytes : 0;
    if (deadline_ms != 0) {
        auto now_ms = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count());
        if (now_ms >= deadline_ms) {
            on_done(Status::Ok(), std::move(*accumulated), true);
            return;
        }
    }

    auto          continuation = make_resume_after(start_after_owned, last_key);
    ScanPackedBuf out_packed;
    size_t        out_count       = 0;
    bool          truncated       = false;
    uint64_t      pending_page_id = kInvalidPageId;
    size_t        attempt_limit   = limit > accumulated_count ? limit - accumulated_count : 0;
    if (try_scan_reverse_no_load(Slice(*prefix_owned), Slice(*continuation), Slice(*end_key_owned), attempt_limit,
                                 remaining_byte_budget, keys_only, deadline_ms, &out_packed, &out_count, &truncated,
                                 &pending_page_id)) {
        if (out_count > 0) {
            accumulated->append(out_packed.data(), out_packed.size());
        }
        on_done(Status::Ok(), std::move(*accumulated), truncated);
        return;
    }

    if (out_count > 0) {
        accumulated->append(out_packed.data(), out_packed.size());
        accumulated_count += out_count;
        last_key = std::make_shared<std::string>(last_key_from_packed(out_packed.data(), out_packed.size()));
    }
    if (opt_.async_page_store != nullptr) {
        uint64_t addr           = 0;
        uint32_t plen           = 0;
        bool     still_unloaded = false;
        uint64_t requested_word = 0;
        Status   location_status;
        {
            auto             generation = generation_.enter();
            std::scoped_lock lk(load_mutex_);
            uint64_t         word = mapping_.get_word(pending_page_id);
            if (slot_word::is_unloaded(word)) {
                requested_word  = word;
                location_status = opt_.page_store->decode_mapping_location(word, &addr, &plen);
                still_unloaded  = location_status.ok();
            }
        }
        if (!location_status.ok()) {
            io_failed_.store(true);
            on_done(location_status, ScanPackedBuf{}, false);
            return;
        }
        if (!still_unloaded) {
            if (metrics_.scan_retry_c != nullptr) {
                metrics_.scan_retry_c->inc();
            }
            scan_reverse_async_attempt(std::move(prefix_owned), start_after_owned, end_key_owned, limit, byte_budget,
                                       keys_only, deadline_ms, std::move(accumulated), std::move(last_key),
                                       accumulated_count, std::move(on_done));
            return;
        }
        uint32_t iu   = opt_.page_store->iu_size();
        auto     blob = std::make_shared<std::vector<uint8_t>>(round_up_to_iu(plen, iu));
        demand_load_total_.fetch_add(1, std::memory_order_relaxed);
        opt_.async_page_store->submit_read(
            addr, blob->data(), blob->size(),
            detail::own_async_completion([this, page_id = pending_page_id, requested_word, addr, plen, blob,
                                          prefix_owned = std::move(prefix_owned), start_after_owned, end_key_owned,
                                          limit, byte_budget, keys_only, deadline_ms,
                                          accumulated = std::move(accumulated), last_key = std::move(last_key),
                                          accumulated_count, on_done = std::move(on_done)](Status status) mutable {
                if (!status.ok()) {
                    CRB_LOG_ERROR("[{}] scan_reverse_async: demand-load I/O fault: pid={} addr={} len={} status={}",
                                  name_, page_id, addr, plen, status.to_string());
                    io_failed_.store(true);
                    on_done(status, ScanPackedBuf{}, false);
                    return;
                }
                bool installed_ok = true;
                {
                    auto             generation = generation_.enter();
                    std::scoped_lock lk(load_mutex_);
                    uint64_t         word = mapping_.get_word(page_id);
                    if (word == requested_word) {
                        installed_ok = install_loaded_page(page_id, addr, plen, *blob) != nullptr;
                    }
                }
                if (!installed_ok) {
                    io_failed_.store(true);
                    on_done(Status::io_error("scan_reverse_async: demand-load decode/CRC failure"), ScanPackedBuf{},
                            false);
                    return;
                }
                if (metrics_.scan_retry_c != nullptr) {
                    metrics_.scan_retry_c->inc();
                }
                scan_reverse_async_attempt(std::move(prefix_owned), start_after_owned, end_key_owned, limit,
                                           byte_budget, keys_only, deadline_ms, std::move(accumulated),
                                           std::move(last_key), accumulated_count, std::move(on_done));
            }));
        return;
    }
    {
        auto generation = generation_.enter();
        (void)resident(pending_page_id);
    }
    if (metrics_.scan_retry_c != nullptr) {
        metrics_.scan_retry_c->inc();
    }
    scan_reverse_async_attempt(std::move(prefix_owned), start_after_owned, end_key_owned, limit, byte_budget, keys_only,
                               deadline_ms, std::move(accumulated), std::move(last_key), accumulated_count,
                               std::move(on_done));
}

int Crowdbtree::height() const
{
    auto     generation = generation_.enter();
    auto     guard      = epoch_.enter();
    int      h          = 0;
    uint64_t page_id    = root_page_id_.load();
    for (int d = 0; d < 64; ++d) {
        PageBase *head = resident(page_id);
        if (head == nullptr) {
            break;
        }
        PageBase *base = head;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        ++h;
        if (base == nullptr || base->type == page_type::kLeafBase) {
            break;
        }
        page_id = static_cast<InnerBase *>(base)->child_at(0);
    }
    return h;
}

size_t Crowdbtree::leaf_count() const
{
    auto                            generation = generation_.enter();
    auto                            guard      = epoch_.enter();
    std::function<size_t(uint64_t)> rec        = [&](uint64_t page_id) -> size_t {
        PageBase *head = resident(page_id);
        if (head == nullptr) {
            return 0;
        }
        PageBase *base = head;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        if (base == nullptr) {
            return 0;
        }
        if (base->type == page_type::kLeafBase) {
            return 1;
        }
        size_t n = 0;
        for (uint64_t c : static_cast<InnerBase *>(base)->children()) {
            n += rec(c);
        }
        return n;
    };
    return rec(root_page_id_.load());
}

Status Crowdbtree::install_snapshot(std::vector<leaf_entry> sorted_entries, uint64_t at_slot)
try {
    // Construct everything privately. Failure cannot expose an empty or
    // partially imported generation to callers of this tree.
    Crowdbtree staged(opt_);
    auto       active = staged.current_active();
    active->set_allow_old_slots(true);
    for (auto &entry : sorted_entries) {
        const CellView cell{entry.cell.slice()};
        if (!cell.valid()) {
            return Status::invalid_argument("snapshot contains an invalid cell");
        }
        const auto slot = cell.slot();
        if (slot > at_slot || !opt_.key_range.contains(Slice(entry.key))) {
            return Status::invalid_argument("snapshot entry is outside its prefix or key range");
        }
        active->upsert(Slice(entry.key), slot, std::move(entry.cell));
    }
    staged.force_advance_slot(at_slot);
    auto status = staged.flush();
    if (!status.ok()) {
        return status;
    }
    std::vector<NativeFrame> frames;
    uint64_t                 root = 0;
    uint64_t                 next = 0;
    status                        = staged.collect_native_frames(&frames, &root, nullptr, &next);
    if (!status.ok()) {
        return status;
    }
    return install_snapshot_native(std::move(frames), root, at_slot, next);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("operation allocation failed");
}
catch (const std::exception &error) {
    return Status::internal_error(error.what());
}

Status Crowdbtree::collect_native_frames(std::vector<NativeFrame> *out, uint64_t *out_root_page_id,
                                         uint64_t *out_at_slot, uint64_t *out_next_page_id, const KeyRange *filter,
                                         uint64_t *out_subtrees_skipped)
{
    std::scoped_lock lk(write_mutex_);
    if (publication_incomplete_) {
        return Status::unavailable("flush publication requires repair");
    }
    uint64_t        gc               = gc_floor_.load();
    const bool      can_prune        = filter != nullptr && routing_fences_trusted_.load(std::memory_order_acquire);
    const KeyRange *effective_filter = can_prune ? filter : nullptr;

    // Same DFS shape as the pre-#14c manifest walk: fold any delta chain
    // into a fresh consolidated base first (a real side effect on the live
    // tree, same as snapshot()'s prepare phase -- unlike the read-only
    // snapshot_view()), then dump the resolved base's frame bytes verbatim,
    // recursing into inner children / leaf overflow chains.
    auto intersects = [effective_filter](const std::optional<std::string> &lower,
                                         const std::optional<std::string> &upper) {
        if (effective_filter == nullptr || !effective_filter->is_bounded()) {
            return true;
        }
        if (effective_filter->start().has_value() && effective_filter->end().has_value() &&
            Slice(*effective_filter->start()).compare(Slice(*effective_filter->end())) == 0) {
            return false;
        }
        const bool below_end   = !effective_filter->end().has_value() || !lower.has_value() ||
                                 Slice(*lower).compare(Slice(*effective_filter->end())) < 0;
        const bool above_start = !effective_filter->start().has_value() || !upper.has_value() ||
                                 Slice(*upper).compare(Slice(*effective_filter->start())) > 0;
        return below_end && above_start;
    };

    uint64_t skipped = 0;
    std::function<Status(uint64_t, std::optional<std::string>, std::optional<std::string>, bool, NativeBounds *)> walk =
        [&](uint64_t page_id, std::optional<std::string> lower, std::optional<std::string> upper, bool is_root,
            NativeBounds *bounds) -> Status {
        if (!is_root && !intersects(lower, upper)) {
            ++skipped;
            return Status::Ok();
        }
        PageBase *head = resident(page_id);
        if (head == nullptr) {
            return Status::internal_error("native snapshot: null page in walk");
        }
        const bool has_inframe_deltas =
            head->type == page_type::kLeafBase && static_cast<LeafBase *>(head)->view().delta_count() != 0;
        if (head->type == page_type::kBatchDelta || has_inframe_deltas) {
            PageBase *b = head;
            while (b != nullptr && b->type == page_type::kBatchDelta) {
                b = b->next;
            }
            if (b == nullptr || b->type != page_type::kLeafBase) {
                return Status::internal_error("native snapshot: delta chain without leaf base");
            }
            uint64_t              right = static_cast<LeafBase *>(b)->right_sibling();
            std::vector<uint64_t> dead_overflow;
            LeafBase             *fresh =
                build_leaf_spilling_locked(resolve_leaf_chain_for_rebuild(head, gc, &dead_overflow), right);
            store_preserving_parent_locked(page_id, fresh);
            for (PageBase *n = head; n != nullptr;) {
                PageBase *nx = n->next;
                retire_page(n);
                n = nx;
            }
            for (uint64_t h : dead_overflow) {
                retire_overflow_chain_locked(h);
            }
            head = fresh;
        }
        PageBase *base = head; // now a single base (no deltas above it)

        const uint8_t *frame = nullptr;
        uint32_t       plen  = 0;
        if (base->type == page_type::kLeafBase) {
            frame = static_cast<LeafBase *>(base)->frame();
            plen  = static_cast<LeafBase *>(base)->page_bytes();
        }
        else if (base->type == page_type::kInnerBase) {
            frame = static_cast<InnerBase *>(base)->frame();
            plen  = static_cast<InnerBase *>(base)->page_bytes();
        }
        else {
            return Status::internal_error("native snapshot: unexpected base type");
        }
        const size_t output_index = out->size();
        out->push_back(NativeFrame{.page_id = page_id, .frame = std::vector<uint8_t>(frame, frame + plen)});

        if (base->type == page_type::kInnerBase) {
            InnerFrameView inner = static_cast<InnerBase *>(base)->view();
            for (uint32_t index = 0; index < inner.num_children(); ++index) {
                std::optional<std::string> child_lower =
                    index == 0 ? lower : std::optional<std::string>(inner.separator_at(index - 1).to_string());
                std::optional<std::string> child_upper =
                    index == inner.num_separators() ? upper
                                                    : std::optional<std::string>(inner.separator_at(index).to_string());
                NativeBounds child_bounds;
                Status       cs =
                    walk(inner.child_at(index), std::move(child_lower), std::move(child_upper), false, &child_bounds);
                if (!cs.ok()) {
                    return cs;
                }
                if (!bounds->lower.has_value() && child_bounds.lower.has_value()) {
                    bounds->lower              = child_bounds.lower;
                    bounds->lower_leaf_page_id = child_bounds.lower_leaf_page_id;
                }
                if (child_bounds.upper.has_value()) {
                    bounds->upper              = child_bounds.upper;
                    bounds->upper_leaf_page_id = child_bounds.upper_leaf_page_id;
                }
            }
            NativeFrame &output_frame = (*out)[output_index];
            if (!set_native_frame_fences(&output_frame, *bounds)) {
                return Status::resource_exhausted("native snapshot: cannot persist inner fences");
            }
        }
        else { // leaf: dump its overflow chains too (reachable via cells, PT11)
            LeafFrameView v = static_cast<LeafBase *>(base)->view();
            if (!v.empty()) {
                bounds->lower              = v.key(0).to_string();
                bounds->upper              = v.key(v.count() - 1).to_string();
                bounds->lower_leaf_page_id = page_id;
                bounds->upper_leaf_page_id = page_id;
            }
            NativeFrame &output_frame = (*out)[output_index];
            if (!set_native_frame_fences(&output_frame, *bounds)) {
                return Status::resource_exhausted("native snapshot: leaf fence keys do not fit frame");
            }
            for (uint32_t i = 0; i < v.count(); ++i) {
                CellView c{v.cell(i)};
                if (!c.is_overflow()) {
                    continue;
                }
                uint64_t opid = c.overflow_head();
                while (opid != kInvalidPageId) {
                    PageBase *op = resident(opid);
                    if (op == nullptr || op->type != page_type::kOverflowFrame) {
                        return Status::internal_error("native snapshot: bad overflow page");
                    }
                    auto *ov = static_cast<OverflowBase *>(op);
                    out->push_back(NativeFrame{
                        .page_id = opid,
                        .frame   = std::vector<uint8_t>(ov->frame(), ov->frame() + ov->page_bytes()),
                    });
                    opid = ov->next_page_id();
                }
            }
        }
        return Status::Ok();
    };

    out->clear();
    NativeBounds root_bounds;
    Status       ws = walk(root_page_id_.load(), std::nullopt, std::nullopt, true, &root_bounds);
    if (!ws.ok()) {
        return ws;
    }
    if (filter != nullptr && !can_prune) {
        Status graph_status = validate_native_snapshot_graph(out, root_page_id_.load());
        if (!graph_status.ok()) {
            return graph_status;
        }
        routing_fences_trusted_.store(true, std::memory_order_release);
    }
    if (out_root_page_id != nullptr) {
        *out_root_page_id = root_page_id_.load();
    }
    if (out_at_slot != nullptr) {
        *out_at_slot = last_applied_slot_.load();
    }
    if (out_next_page_id != nullptr) {
        *out_next_page_id = mapping_.next_page_id();
    }
    if (out_subtrees_skipped != nullptr) {
        *out_subtrees_skipped = skipped;
    }
    return Status::Ok();
}

EngineStats Crowdbtree::stats() const
{
    EngineStats s;
    s.last_applied_slot         = last_applied_slot_.load();
    s.contiguous_slot           = contiguous_slot_.load();
    s.gc_watermark              = gc_floor_.load();
    s.io_failed                 = io_failed_.load();
    s.snapshot_pages_written    = snapshot_pages_written_.load();
    s.snapshot_pages_total      = snapshot_pages_total_.load();
    s.snapshot_segments_written = snapshot_segments_written_.load();

    BufferPool::Stats bp     = pool_->stats();
    s.buffer_pool_hits       = bp.hits;
    s.buffer_pool_misses     = bp.misses;
    s.buffer_pool_evictions  = bp.evictions;
    s.buffer_pool_writebacks = bp.writebacks;
    s.buffer_pool_resident   = bp.resident;
    s.buffer_pool_dirty      = bp.dirty;
    s.buffer_pool_used       = bp.used;
    s.buffer_pool_num_frames = bp.num_frames;

    s.mt_upsert_total        = mt_upsert_total_.load(std::memory_order_relaxed);
    s.mt_overwrite_total     = memtable_counters_.overwrite.load();
    s.mt_history_keep_total  = memtable_counters_.keep.load();
    s.mt_history_merge_total = memtable_counters_.merged.load();
    s.mt_version_cas_retries = memtable_counters_.cas_retries.load();
    s.mt_resident_bytes      = epoch_.memtable_allocation()->load();
    s.mt_get_total           = mt_get_total_.load(std::memory_order_relaxed);
    s.mt_get_hit_total       = mt_get_hit_total_.load(std::memory_order_relaxed);
    s.flush_drain_total      = flush_drain_total_.load(std::memory_order_relaxed);
    s.flush_entries_total    = flush_entries_total_.load(std::memory_order_relaxed);
    s.snapshot_total         = snapshot_total_.load(std::memory_order_relaxed);
    s.l1_get_total           = l1_get_total_.load(std::memory_order_relaxed);
    s.l1_get_hit_total       = l1_get_hit_total_.load(std::memory_order_relaxed);
    s.map_lookup_total       = map_lookup_total_.load(std::memory_order_relaxed);
    s.demand_load_total      = demand_load_total_.load(std::memory_order_relaxed);
    s.leaf_count             = leaf_count_.load(std::memory_order_relaxed);
    s.inner_count            = inner_count_.load(std::memory_order_relaxed);
    return s;
}

ScanProfile Crowdbtree::scan_profile() const
{
    ScanProfile p;
    if (metrics_registry_ == nullptr) {
        return p;
    }
    auto fill = [](LatencySummary *h, ScanProfile::Step &s, uint64_t count) {
        if (h == nullptr || count == 0) {
            return;
        }
        auto snap = h->flush();
        s.sum_ns  = snap.sum;
        s.max_ns  = snap.max;
        s.avg_ns  = snap.sum / count;
    };
    // Count from scan_l (scan_c removed — count derivable from latency).
    uint64_t scan_count = 0;
    if (metrics_.scan_l != nullptr) {
        auto snap      = metrics_.scan_l->flush();
        scan_count     = snap.count;
        p.total.sum_ns = snap.sum;
        p.total.max_ns = snap.max;
        if (snap.count > 0) {
            p.total.avg_ns = snap.sum / snap.count;
        }
    }
    p.count   = scan_count;
    p.entries = metrics_.scan_entries_c != nullptr ? metrics_.scan_entries_c->flush().count : 0;
    fill(metrics_.scan_l0_l, p.l0, p.count);
    fill(metrics_.scan_l1_l, p.l1, p.count);
    fill(metrics_.scan_merge_l, p.merge, p.count);
    return p;
}

void Crowdbtree::init_metrics(const std::string &prefix, const std::string &backend_label)
{
    metrics_registry_ = std::make_unique<MetricsRegistry>();
    auto *r           = metrics_registry_.get();
    // Backend-specific I/O prefix (empty for mem backend → no suffix).
    std::string io = backend_label.empty() ? prefix : prefix + "." + backend_label;

    // ── Logical metrics (backend-independent) ──
    metrics_.flush_l              = r->register_summary(prefix + ".flush.l");
    metrics_.flush_drain_c        = r->register_counter(prefix + ".flush.drain.c");
    metrics_.flush_entries_c      = r->register_counter(prefix + ".flush.entries.c");
    metrics_.split_view_begin_l   = r->register_summary(prefix + ".split.view.begin.l");
    metrics_.split_view_publish_l = r->register_summary(prefix + ".split.view.publish.l");
    metrics_.split_view_release_l = r->register_summary(prefix + ".split.view.release.l");
    metrics_.mt_apply_l           = r->register_summary(prefix + ".mt.apply.l");
    metrics_.mt_get_c             = r->register_counter(prefix + ".mt.get.c");
    metrics_.mt_get_hit_c         = r->register_counter(prefix + ".mt.get.hit.c");
    metrics_.mt_get_l             = r->register_summary(prefix + ".mt.get.l");
    metrics_.mt_frozen_g  = r->register_callback_gauge(prefix + ".mt.frozen.g",
                                                       [this] { return static_cast<uint64_t>(frozen_table_count()); });
    metrics_.mt_records_g = r->register_callback_gauge(prefix + ".mt.records.g", [this] {
        size_t total = 0;
        for (const auto &mt : all_memtables()) {
            total += mt->count();
        }
        return static_cast<uint64_t>(total);
    });
    r->register_callback_gauge(prefix + ".mt.overwrite.total", [this] { return memtable_counters_.overwrite.load(); });
    metrics_.mt_version_copy_l = r->register_summary(prefix + ".mt.version.copy.l");
    r->register_callback_gauge(prefix + ".mt.history.keep.total", [this] { return memtable_counters_.keep.load(); });
    r->register_callback_gauge(prefix + ".mt.history.merge.total", [this] { return memtable_counters_.merged.load(); });
    r->register_callback_gauge(prefix + ".mt.history.keep.gap.total",
                               [this] { return memtable_counters_.keep_gap.load(); });
    r->register_callback_gauge(prefix + ".mt.history.keep.pending.total",
                               [this] { return memtable_counters_.keep_pending.load(); });
    r->register_callback_gauge(prefix + ".mt.history.keep.boundary.total",
                               [this] { return memtable_counters_.keep_boundary.load(); });
    r->register_callback_gauge(prefix + ".mt.batch.failed.total",
                               [this] { return memtable_counters_.failed_batches.load(); });
    auto memory_gauge = [this, r, &prefix](const char *suffix, uint64_t VersionMemory::*member) {
        r->register_callback_gauge(prefix + suffix, [this, member] {
            uint64_t total = 0;
            for (const auto &source : all_memtables()) {
                total += source.owner->memory().*member;
            }
            return total;
        });
    };
    memory_gauge(".mt.history.live.g", &VersionMemory::history_count);
    memory_gauge(".mt.history.bytes.g", &VersionMemory::history_bytes);
    memory_gauge(".mt.descriptor.bytes.g", &VersionMemory::descriptor_bytes);
    memory_gauge(".mt.node.bytes.g", &VersionMemory::node_bytes);
    memory_gauge(".mt.payload.bytes.g", &VersionMemory::payload_bytes);
    r->register_callback_gauge(prefix + ".mt.freezing.writers.g", [this] {
        uint64_t writers = 0;
        for (const auto &source : local_memtables()) {
            if (source.owner->closed()) {
                writers += source.owner->writers();
            }
        }
        return writers;
    });
    r->register_callback_gauge(prefix + ".mt.frozen.retained.bytes.g", [this] {
        uint64_t bytes = 0;
        for (const auto &source : local_memtables()) {
            if (source.owner->closed() && source.owner->writers() == 0) {
                bytes += source.owner->approx_bytes();
            }
        }
        return bytes;
    });
    r->register_callback_gauge(prefix + ".mt.resident.bytes.g",
                               [this] { return epoch_.memtable_allocation()->load(); });
    r->register_callback_gauge(prefix + ".mt.retired.bytes.g", [this] {
        uint64_t live = 0;
        for (const auto &source : local_memtables()) {
            const auto memory = source.owner->memory();
            live += memory.node_bytes + memory.descriptor_bytes + memory.payload_bytes;
        }
        const auto resident = epoch_.memtable_allocation()->load();
        return resident > live ? resident - live : 0;
    });
    metrics_.mt_freeze_c        = r->register_counter(prefix + ".mt.freeze.c");
    metrics_.l1_get_c           = r->register_counter(prefix + ".l1.get.c");
    metrics_.l1_get_hit_c       = r->register_counter(prefix + ".l1.get.hit.c");
    metrics_.l1_get_l           = r->register_summary(prefix + ".l1.get.l");
    metrics_.page_write_l       = r->register_summary(prefix + ".page.write.l");
    metrics_.page_split_c       = r->register_counter(prefix + ".page.split.c");
    metrics_.page_merge_c       = r->register_counter(prefix + ".page.merge.c");
    metrics_.page_consolidate_c = r->register_counter(prefix + ".page.consolidate.c");
    metrics_.tree_height_g =
        r->register_callback_gauge(prefix + ".tree.height.g", [this] { return static_cast<uint64_t>(height()); });
    metrics_.tree_leaf_count_g  = r->register_gauge(prefix + ".tree.leaf.count.g");
    metrics_.tree_inner_count_g = r->register_gauge(prefix + ".tree.inner.count.g");
    metrics_.tree_retired_count_g =
        r->register_callback_gauge(prefix + ".tree.retired.count.g", [this] { return epoch_.pending_retired(); });
    // Seed the leaf/inner gauges with the current counter values (so the first
    // metrics flush before any SMO shows the right counts).
    metrics_.tree_leaf_count_g->set(leaf_count_.load(std::memory_order_relaxed));
    metrics_.tree_inner_count_g->set(inner_count_.load(std::memory_order_relaxed));
    metrics_.page_map_alloc_c      = r->register_counter(prefix + ".page.map.alloc.c");
    metrics_.page_map_total_pids_g = r->register_callback_gauge(prefix + ".page.map.total_pids.g", [this] {
        auto generation = generation_.enter();
        return mapping_.next_page_id();
    });
    metrics_.page_map_segments_g   = r->register_callback_gauge(prefix + ".page.map.segments.g", [this] {
        auto generation = generation_.enter();
        return static_cast<uint64_t>(mapping_.segments_allocated());
    });
    metrics_.snapshot_l            = r->register_summary(prefix + ".snapshot.l");
    metrics_.snapshot_pages_c      = r->register_counter(prefix + ".snapshot.pages.c");
    metrics_.scan_entries_c        = r->register_counter(prefix + ".scan.entries.c");
    metrics_.scan_l                = r->register_summary(prefix + ".scan.l");
    metrics_.scan_l0_l             = r->register_summary(prefix + ".scan.l0.l");
    metrics_.scan_l1_l             = r->register_summary(prefix + ".scan.l1.l");
    metrics_.scan_merge_l          = r->register_summary(prefix + ".scan.merge.l");
    metrics_.scan_retry_c          = r->register_counter(prefix + ".scan.retry.c");
    metrics_.gc_tombstones_c       = r->register_counter(prefix + ".gc.tombstones.c");
    metrics_.merge_gc_blocks_c     = r->register_counter(prefix + ".merge_gc.blocks.c");
    metrics_.merge_gc_relocated_c  = r->register_counter(prefix + ".merge_gc.relocated.c");
    metrics_.merge_gc_deleted_c    = r->register_counter(prefix + ".merge_gc.deleted.c");
    metrics_.merge_gc_l            = r->register_summary(prefix + ".merge_gc.l");

    // ── Backend I/O metrics ──
    metrics_.page_find_c                 = r->register_counter(io + ".page.find.c");
    metrics_.buf_evictions               = r->register_counter(io + ".buf.evictions.c");
    metrics_.buf_writebacks              = r->register_counter(io + ".buf.writebacks.c");
    metrics_.buf_resident                = r->register_gauge(io + ".buf.resident.g");
    metrics_.buf_dirty                   = r->register_gauge(io + ".buf.dirty.g");
    metrics_.page_load_l                 = r->register_summary(io + ".page.load.l");
    metrics_.page_writeback_l            = r->register_summary(io + ".page.writeback.l");
    metrics_.page_writeback_bw           = r->register_bandwidth(io + ".page.writeback.bw");
    metrics_.fsync_l                     = r->register_summary(io + ".fsync.l");
    metrics_.snapshot_apply_l            = r->register_summary(io + ".snapshot.apply.l");
    metrics_.snapshot_page_write_l       = r->register_summary(io + ".snapshot.page.write.io.l");
    metrics_.snapshot_page_write_cache_c = r->register_counter(io + ".snapshot.page.write.cache.c");
    metrics_.snapshot_page_write_bw      = r->register_bandwidth(io + ".snapshot.page.write.bw");
    metrics_.snapshot_meta_write_bw      = r->register_bandwidth(io + ".snapshot.meta.write.bw");
    metrics_.page_read_bw                = r->register_bandwidth(io + ".page.read.bw");

    pool_->set_metrics(metrics_.buf_evictions, metrics_.buf_writebacks, metrics_.buf_resident, metrics_.buf_dirty,
                       metrics_.page_writeback_l, metrics_.page_writeback_bw);
    mapping_.set_alloc_counter(metrics_.page_map_alloc_c);
}

std::string Crowdbtree::flush_metrics_str(double window_secs, const char *timestamp, size_t width, size_t count_w,
                                          size_t tps_w)
{
    if (metrics_registry_ == nullptr) {
        return {};
    }
    char  *buf = nullptr;
    size_t len = 0;
    FILE  *fp  = open_memstream(&buf, &len);
    if (fp == nullptr) {
        return {};
    }
    metrics_registry_->flush_to(fp, window_secs, timestamp, "cpp-tree", width, count_w, tps_w);
    std::fflush(fp);
    std::fclose(fp);
    std::string result(buf, len);
    free(buf);
    return result;
}

size_t Crowdbtree::max_name_len() const
{
    if (metrics_registry_ == nullptr) {
        return 0;
    }
    return metrics_registry_->max_name_len();
}

std::shared_ptr<Snapshot> Crowdbtree::snapshot_view()
{
    std::scoped_lock writer(write_mutex_);
    if (publication_incomplete_) {
        throw std::runtime_error("flush publication requires repair");
    }

    // R6: zero-copy pinned snapshot. Walks the leaf chain under an epoch guard
    // (same safety argument as the old materialized version — see the comment
    // below on the walk's concurrency properties), captures every PageBase*
    // touched (leaf chain heads + overflow pages), pins each via pin_state_,
    // then releases the guard. Returns a PinnedSnapshot that holds the pins
    // and materializes entries lazily from the pinned frames on first call.
    // The pages stay alive via refcount until the PinnedSnapshot is dropped —
    // on any thread.
    //
    // Concurrency: the walk uses the same collect_in_order right_sibling
    // technique as scan() and the old snapshot_view(), under a single epoch
    // guard. A concurrent split/merge/flush is never blocked. The guard is
    // entered and released on this same thread (respecting Guard's
    // thread-bound contract); the PinnedSnapshot's pins are thread-independent.
    //
    // at_slot is captured *before* the walk (same rationale as before: flush
    // only bumps last_applied_slot_ after publishing, so a racing flush can
    // only make the walk see more, never less).
    EpochManager::Guard guard   = epoch_.enter();
    uint64_t            at_slot = last_applied_slot_.load();

    // Walk the leaf chain, capturing page pointers. We inline the
    // collect_in_order walk so we can capture both leaf chain pages (head →
    // ... → base for each leaf — the full delta chain, not just the head) and
    // overflow pages in a single pass. materialize() follows head->next, so
    // every node in every chain must be pinned.
    std::vector<PageBase *> leaf_chain_heads;
    std::vector<PageBase *> all_pinned_pages;
    std::vector<PageBase *> overflow_pages;
    uint64_t                root_pid = root_page_id_.load();
    if (root_pid != kInvalidPageId) {
        uint64_t page_id = find_leaf_page_id([this](uint64_t p) { return resident(p); }, root_pid, Slice());
        while (page_id != kInvalidPageId) {
            PageBase *head = resident(page_id);
            if (head == nullptr) {
                break;
            }
            leaf_chain_heads.push_back(head);
            // Capture the entire chain (head → ... → base), not just the head.
            // materialize() calls resolve_chain_sorted(head) which follows
            // head->next; every delta node must be pinned to survive the
            // epoch guard release.
            for (PageBase *node = head; node != nullptr; node = node->next) {
                all_pinned_pages.push_back(node);
                if (node->type == page_type::kLeafBase) {
                    LeafFrameView v = static_cast<LeafBase *>(node)->view();
                    for (uint32_t i = 0; i < v.count(); ++i) {
                        CellView c{v.cell(i)};
                        if (c.is_overflow()) {
                            capture_overflow_chain(c.overflow_head(), overflow_pages);
                        }
                    }
                    for (uint32_t i = 0; i < v.delta_count(); ++i) {
                        CellView c{v.delta_cell(i)};
                        if (c.is_overflow()) {
                            capture_overflow_chain(c.overflow_head(), overflow_pages);
                        }
                    }
                }
                else if (node->type == page_type::kBatchDelta) {
                    for (const leaf_entry &e : static_cast<BatchDelta *>(node)->entries()) {
                        CellView c{Slice(e.cell)};
                        if (c.is_overflow()) {
                            capture_overflow_chain(c.overflow_head(), overflow_pages);
                        }
                    }
                }
            }
            LeafBase *base = chain_leaf_base(head);
            page_id        = base != nullptr ? base->right_sibling() : kInvalidPageId;
        }
    }

    // Dedup overflow pages (a single overflow chain may be referenced by
    // multiple keys in the same leaf). Also add them to all_pinned_pages.
    std::ranges::sort(overflow_pages);
    auto [first_dup, last_dup] = std::ranges::unique(overflow_pages);
    overflow_pages.erase(first_dup, last_dup);
    for (PageBase *p : overflow_pages) {
        all_pinned_pages.push_back(p);
    }

    auto snap = std::make_shared<PinnedSnapshot>(at_slot, std::move(leaf_chain_heads), std::move(all_pinned_pages),
                                                 std::move(overflow_pages));
    // The PinnedSnapshot ctor pins the captured pages; release the epoch guard
    // now (the pins keep the pages resident across threads).
    guard = EpochManager::Guard();
    return snap;
}

// Capture all pages in an overflow chain starting at head_page_id. Used by
// snapshot_view() to pin overflow pages so PinnedSnapshot::materialize() can
// assemble overflow values from pinned frames without re-entering the mapping
// table.
void Crowdbtree::capture_overflow_chain(uint64_t head_page_id, std::vector<PageBase *> &out)
{
    uint64_t page_id = head_page_id;
    for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard) {
        PageBase *p = resident(page_id);
        if (p == nullptr || p->type != page_type::kOverflowFrame) {
            break;
        }
        out.push_back(p);
        page_id = static_cast<OverflowBase *>(p)->next_page_id();
    }
}

// R6: PinnedSnapshot::materialize() — walk the captured leaf chain heads (in
// order), resolve each chain via resolve_chain_sorted, and assemble overflow
// values from the captured overflow pages. Called lazily on first entries()
// access. Defined here (not in snapshot.h) because it needs resolve_chain_sorted
// and the OverflowBase/LeafBase page types from crowdb-tree.cpp's internal helpers.
void PinnedSnapshot::materialize() const
{
    // Build a lookup from page_id → PageBase* for the captured overflow pages
    // so we can assemble overflow values without re-entering the mapping table.
    std::unordered_map<uint64_t, PageBase *> overflow_by_id;
    for (PageBase *p : overflow_pages_) {
        overflow_by_id[p->page_id] = p;
    }

    for (PageBase *head : leaf_chain_heads_) {
        auto entries = resolve_chain_sorted(head, 0); // gc_floor=0: keep all (snapshot is point-in-time)
        for (auto &e : entries) {
            CellView v{Slice(e.cell)};
            if (v.is_overflow()) {
                // Assemble from pinned overflow pages.
                std::string assembled;
                assembled.reserve(v.overflow_len());
                uint64_t page_id = v.overflow_head();
                for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard) {
                    auto it = overflow_by_id.find(page_id);
                    if (it == overflow_by_id.end()) {
                        break;
                    }
                    auto *ov    = static_cast<OverflowBase *>(it->second);
                    Slice chunk = ov->payload();
                    assembled.append(chunk.data(), chunk.size());
                    page_id = ov->next_page_id();
                }
                if (assembled.size() > v.overflow_len()) {
                    assembled.resize(v.overflow_len());
                }
                e.cell = encode_cell_buf(v.slot(), OpKind::kPut, Slice(assembled));
            }
            entries_.push_back(std::move(e));
        }
    }
}

std::string Crowdbtree::assemble_overflow_value(uint64_t head_page_id, uint64_t total_len) const
{
    std::string out;
    out.reserve(total_len);
    uint64_t page_id = head_page_id;
    for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard) {
        PageBase *p = resident(page_id);
        if (p == nullptr || p->type != page_type::kOverflowFrame) {
            break; // corruption -> short value
        }
        auto *ov    = static_cast<OverflowBase *>(p);
        Slice chunk = ov->payload();
        out.append(chunk.data(), chunk.size());
        page_id = ov->next_page_id();
    }
    if (out.size() > total_len) {
        out.resize(total_len);
    }
    return out;
}

std::vector<leaf_entry> Crowdbtree::resolve_leaf_chain_for_rebuild(PageBase *head, uint64_t gc_floor,
                                                                   std::vector<uint64_t> *dead_overflow,
                                                                   size_t                *out_tombstones_dropped,
                                                                   size_t                *out_bytes_dropped)
{
    std::map<std::string, std::string> resolved; // key -> encoded storage cell
    auto                               consider = [&](Slice key, Slice cell) {
        CellView    incoming{cell};
        uint64_t    s  = incoming.slot();
        std::string k  = key.to_string();
        auto        it = resolved.find(k);
        if (it == resolved.end()) {
            resolved[k] = cell.to_string();
            return;
        }
        CellView current{Slice(it->second)};
        if (s > current.slot()) {
            if (dead_overflow && current.is_overflow()) {
                dead_overflow->push_back(current.overflow_head());
            }
            it->second = cell.to_string();
        }
        else if (dead_overflow && incoming.is_overflow()) {
            dead_overflow->push_back(incoming.overflow_head()); // incoming loses
        }
    };
    for (PageBase *node = head; node != nullptr; node = node->next) {
        if (node->type == page_type::kBatchDelta) {
            for (const leaf_entry &e : static_cast<BatchDelta *>(node)->entries()) {
                consider(Slice(e.key), Slice(e.cell));
            }
        }
        else if (node->type == page_type::kLeafBase) {
            LeafFrameView v = static_cast<LeafBase *>(node)->view();
            for (uint32_t i = 0; i < v.count(); ++i) {
                consider(v.key(i), v.cell(i));
            }
            for (uint32_t i = 0; i < v.delta_count(); ++i) {
                consider(v.delta_key(i), v.delta_cell(i));
            }
        }
    }
    std::vector<leaf_entry> out;
    out.reserve(resolved.size());
    size_t dropped       = 0;
    size_t dropped_bytes = 0;
    for (auto &kv : resolved) {
        CellView v{Slice(kv.second)};
        if (v.is_tombstone() && v.slot() <= gc_floor) {
            ++dropped;
            dropped_bytes += kv.first.size() + kv.second.size();
            continue; // GC drop
        }
        out.push_back({.key = kv.first, .cell = cell_of(kv.second)});
    }
    if (out_tombstones_dropped != nullptr) {
        *out_tombstones_dropped = dropped;
    }
    if (out_bytes_dropped != nullptr) {
        *out_bytes_dropped = dropped_bytes;
    }
    return out;
}

uint64_t Crowdbtree::spill_value_to_overflow_chain_locked(const std::string &value)
{
    const uint32_t cap = overflow_chunk_cap(opt_.frame_bytes);
    // Split into chunks; build the chain tail-first so each frame knows its next.
    size_t                n       = value.size();
    size_t                nchunks = n == 0 ? 1 : (n + cap - 1) / cap;
    std::vector<uint64_t> pids(nchunks);
    for (size_t i = 0; i < nchunks; ++i) {
        pids[i] = mapping_.allocate_page_id();
    }
    uint64_t next = kInvalidPageId;
    for (size_t i = nchunks; i-- > 0;) {
        size_t        off  = i * cap;
        uint32_t      len  = static_cast<uint32_t>(std::min<size_t>(cap, n - off));
        OverflowBase *page = OverflowBase::build(next, reinterpret_cast<const uint8_t *>(value.data() + off), len,
                                                 pool_, opt_.frame_bytes);
        mapping_.store(pids[i], page);
        next = pids[i];
    }
    return pids[0];
}

LeafBase *Crowdbtree::build_leaf_spilling_locked(std::vector<leaf_entry> entries, uint64_t right_sibling)
{
    const size_t threshold = max_inline_value();
    for (leaf_entry &e : entries) {
        CellView v{Slice(e.cell)};
        if (v.is_overflow() || v.is_tombstone()) {
            continue; // pointer / tombstone: keep
        }
        Slice val = v.value();
        if (val.size() > threshold) {
            std::string value = val.to_string();
            uint64_t    head  = spill_value_to_overflow_chain_locked(value);
            e.cell            = encode_overflow_cell_buf(v.slot(), head, value.size());
        }
    }
    return LeafBase::build(entries, right_sibling, pool_, opt_.frame_bytes);
}

void Crowdbtree::retire_overflow_chain_locked(uint64_t head_page_id)
{
    uint64_t page_id = head_page_id;
    for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard) {
        // Demand-load unloaded links so we can read their next_page_id and retire the
        // whole chain (no descriptor/extent leak when a tail link was evicted). Lock
        // order write_mutex_ -> load_mutex_ holds (caller holds write_mutex_).
        PageBase *p = resident(page_id);
        if (p == nullptr || p->type != page_type::kOverflowFrame) {
            mapping_.clear(page_id); // clear a stray slot if any
            break;
        }
        uint64_t next = static_cast<OverflowBase *>(p)->next_page_id();
        mapping_.clear(page_id); // unlink before retiring
        retire_page(p);
        page_id = next;
    }
}

void Crowdbtree::evict_overflow_chain_locked(uint64_t head_page_id)
{
    uint64_t page_id = head_page_id;
    for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard) {
        uint64_t w = mapping_.get_word(page_id);
        // Stop at an already-unloaded link: chains evict whole, so the tail is
        // already unloaded (and not leaking). A dirty page (no durable addr) can't
        // be evicted; leave it resident.
        if (slot_word::is_empty(w) || slot_word::is_unloaded(w)) {
            break;
        }
        PageBase *p = slot_word::resident_ptr(w);
        if (p->type != page_type::kOverflowFrame || p->durable_addr == kNoAddr) {
            break;
        }
        uint64_t next     = static_cast<OverflowBase *>(p)->next_page_id();
        uint64_t unloaded = slot_word::kEmpty;
        if (!opt_.page_store->encode_mapping_location(p->durable_addr, p->durable_plen, &unloaded).ok()) {
            io_failed_.store(true);
            break;
        }
        mapping_.store_word(page_id, unloaded);
        retire_page(p);
        page_id = next;
    }
}

void Crowdbtree::free_overflow_chain(uint64_t head_page_id)
{
    uint64_t page_id = head_page_id;
    for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard) {
        uint64_t w = mapping_.get_word(page_id);
        if (slot_word::is_empty(w) || slot_word::is_unloaded(w)) {
            mapping_.clear(page_id); // clear any unloaded descriptor
            break;
        }
        PageBase *p = slot_word::resident_ptr(w);
        if (p->type != page_type::kOverflowFrame) {
            break;
        }
        uint64_t next = static_cast<OverflowBase *>(p)->next_page_id();
        mapping_.clear(page_id);
        delete p; // teardown / clear: no concurrent readers
        page_id = next;
    }
}

} // namespace crowdb::tree
