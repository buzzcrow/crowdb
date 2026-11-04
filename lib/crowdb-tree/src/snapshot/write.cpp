// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "async_completion_adapter.h"
#include "crowdb-common/log.h"
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/crowdb-tree.h"

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <utility>
#include <vector>

namespace crowdb::tree
{
Status Crowdbtree::snapshot(uint64_t *out_last_applied, uint64_t *out_snapshot_seq)
{
    if (opt_.page_store == nullptr) {
        return Status::invalid_argument("snapshot: no page_store");
    }
    auto snap_t0 = std::chrono::steady_clock::now();
    acquire_snapshot_slot();
    Status pending = retry_snapshot_anchor();
    if (!pending.ok()) {
        release_snapshot_slot();
        return pending;
    }
    PreparedSnapshot prepared;
    Status           ps;
    {
        auto             apply_t0 = std::chrono::steady_clock::now();
        std::scoped_lock lk(write_mutex_);
        ps = prepare_snapshot_locked(&prepared);
        if (metrics_.snapshot_apply_l != nullptr) {
            auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - apply_t0)
                          .count();
            metrics_.snapshot_apply_l->observe(static_cast<uint64_t>(ns));
        }
    }
    if (!ps.ok()) {
        CRB_LOG_ERROR("[{}] snapshot: prepare_snapshot_locked failed: {}", name_, ps.to_string());
        release_snapshot_slot();
        return ps;
    }
    for (auto &w : prepared.page_writes) {
        w.blob.resize(round_up_to_iu(w.logical_len, opt_.page_store->iu_size()), 0);
        auto   write_t0 = std::chrono::steady_clock::now();
        Status s        = opt_.page_store->write_at(w.addr, w.blob.data(), w.blob.size());
        if (metrics_.snapshot_page_write_l != nullptr) {
            auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - write_t0)
                          .count();
            metrics_.snapshot_page_write_l->observe(static_cast<uint64_t>(ns));
        }
        if (metrics_.snapshot_page_write_bw != nullptr) {
            metrics_.snapshot_page_write_bw->observe(w.blob.size());
        }
        if (!s.ok()) {
            CRB_LOG_ERROR("[{}] snapshot page write failed: addr={} status={}", name_, w.addr, s.to_string());
            release_snapshot_slot();
            return s;
        }
        std::vector<uint8_t>().swap(w.blob);
    }
    for (auto &sw : prepared.segment_writes) {
        Status s = opt_.page_store->write_typed_at(PagePurpose::kPageIndex, sw.addr, sw.blob.data(), sw.blob.size());
        if (!s.ok()) {
            CRB_LOG_ERROR("[{}] snapshot segment write failed: addr={} status={}", name_, sw.addr, s.to_string());
            release_snapshot_slot();
            return s;
        }
    }
    Status dw =
        opt_.page_store->write_typed_at(PagePurpose::kPageIndex, prepared.directory_write.addr,
                                        prepared.directory_write.blob.data(), prepared.directory_write.blob.size());
    if (!dw.ok()) {
        CRB_LOG_ERROR("[{}] snapshot directory write failed: addr={} status={}", name_, prepared.directory_write.addr,
                      dw.to_string());
        release_snapshot_slot();
        return dw;
    }
    // Barrier: pages + segment images + directory durable before the anchor
    // that references them.
    auto   fsync_t0 = std::chrono::steady_clock::now();
    Status sync1    = opt_.page_store->sync();
    if (metrics_.fsync_l != nullptr) {
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - fsync_t0).count();
        metrics_.fsync_l->observe(static_cast<uint64_t>(ns));
    }
    if (!sync1.ok()) {
        CRB_LOG_ERROR("[{}] snapshot data sync failed: {}", name_, sync1.to_string());
        release_snapshot_slot();
        return sync1;
    }
    Status aw = opt_.page_store->write_typed_at(PagePurpose::kPageIndex, prepared.anchor_write.addr,
                                                prepared.anchor_write.blob.data(), prepared.anchor_write.blob.size());
    if (!aw.ok()) {
        CRB_LOG_ERROR("[{}] snapshot anchor write failed: addr={} status={}", name_, prepared.anchor_write.addr,
                      aw.to_string());
        release_snapshot_slot();
        return aw;
    }
    auto fsync2_t0           = std::chrono::steady_clock::now();
    snapshot_anchor_pending_ = true;
    Status sync2             = opt_.page_store->sync();
    if (metrics_.fsync_l != nullptr) {
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - fsync2_t0).count();
        metrics_.fsync_l->observe(static_cast<uint64_t>(ns));
    }
    if (!sync2.ok()) {
        CRB_LOG_ERROR("[{}] snapshot anchor sync failed: {}", name_, sync2.to_string());
        commit_prepared_snapshot(prepared);
        release_snapshot_slot();
        return sync2;
    }
    snapshot_anchor_pending_ = false;

    // Observe metadata write bytes: segments + directory + anchor.
    if (metrics_.snapshot_meta_write_bw != nullptr) {
        uint64_t meta_bytes = 0;
        for (const auto &sw : prepared.segment_writes) {
            meta_bytes += sw.blob.size();
        }
        meta_bytes += prepared.directory_write.blob.size();
        meta_bytes += prepared.anchor_write.blob.size();
        metrics_.snapshot_meta_write_bw->observe(meta_bytes);
    }

    finalize_prepared_snapshot(prepared);

    // Publish the exact logical summary only after the root anchor is durable.
    if (PageBase *root = resident(root_page_id_.load(std::memory_order_acquire)); root != nullptr) {
        PageSummary summary;
        if (root->type == page_type::kLeafBase) {
            summary = static_cast<LeafBase *>(root)->summary();
        }
        else if (root->type == page_type::kInnerBase) {
            summary = static_cast<InnerBase *>(root)->summary();
        }
        if (summary.exact) {
            summary_live_kv_.store(summary.live_kv, std::memory_order_release);
            summary_live_key_bytes_.store(summary.live_key_bytes, std::memory_order_release);
            summary_live_value_bytes_.store(summary.live_value_bytes, std::memory_order_release);
            summary_live_logical_bytes_.store(summary.live_logical_bytes, std::memory_order_release);
            summary_reachable_overflow_pages_.store(summary.reachable_overflow_pages, std::memory_order_release);
            summary_reachable_page_capacity_bytes_.store(summary.reachable_page_capacity_bytes,
                                                         std::memory_order_release);
            summary_covered_slot_.store(prepared.last_applied_slot, std::memory_order_release);
            summary_root_version_.store(prepared.seq, std::memory_order_release);
            summary_available_.store(true, std::memory_order_release);
        }
    }

    release_snapshot_slot();
    if (out_last_applied != nullptr) {
        *out_last_applied = prepared.last_applied_slot;
    }
    if (out_snapshot_seq != nullptr) {
        *out_snapshot_seq = prepared.seq;
    }
    if (metrics_.snapshot_l != nullptr) {
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - snap_t0).count();
        metrics_.snapshot_l->observe(static_cast<uint64_t>(ns));
    }
    return Status::Ok();
}

void Crowdbtree::snapshot_async(
    std::function<void(Status, uint64_t)> on_done) // NOLINT(performance-unnecessary-value-param)
{
    if (opt_.page_store == nullptr) {
        on_done(Status::invalid_argument("snapshot: no page_store"), 0);
        return;
    }
    if (opt_.async_page_store != nullptr) {
        acquire_snapshot_slot();
        snapshot_prepare_async(std::move(on_done));
        return;
    }
    // No async backend wired -- run the synchronous path in
    // this stack frame; still correct, just not genuinely async.
    uint64_t last_applied = 0;
    Status   st           = snapshot(&last_applied);
    on_done(st, last_applied);
}

void Crowdbtree::snapshot_write_next_async(                    // NOLINT(readability-convert-member-functions-to-static)
    std::shared_ptr<PreparedSnapshot> prepared,                // NOLINT(performance-unnecessary-value-param)
    size_t idx, std::function<void(Status, uint64_t)> on_done) // NOLINT(performance-unnecessary-value-param)
{
    // snapshot_inflight_ (acquired by snapshot_async() before
    // prepare_snapshot_locked()) stays held across this entire async chain
    // -- see snapshot_async's doc comment on crowdb-tree.h for why an atomic
    // spin-gate is used here instead of write_mutex_ (which cannot be
    // unlocked from a different thread than the one that locked it, and
    // this chain's completions run on the Reactor thread). `prepared` is a
    // shared_ptr so each hop's lambda can carry it to the next hop after
    // this call's own stack frame returns.
    if (idx < prepared->page_writes.size()) {
        PreparedPageWrite &w = prepared->page_writes[idx];
        w.blob.resize(round_up_to_iu(w.logical_len, opt_.page_store->iu_size()), 0);
        opt_.async_page_store->submit_write(
            w.addr, w.blob.data(), w.blob.size(),
            detail::own_async_completion([this, prepared, idx, on_done](const Status &st) mutable {
                if (!st.ok()) {
                    release_snapshot_slot();
                    on_done(st, 0);
                    return;
                }
                std::vector<uint8_t>().swap(prepared->page_writes[idx].blob);
                snapshot_write_next_async(std::move(prepared), idx + 1, std::move(on_done));
            }));
        return;
    }
    size_t seg_idx_in_list = idx - prepared->page_writes.size();
    if (seg_idx_in_list < prepared->segment_writes.size()) {
        const PreparedSegmentWrite &sw = prepared->segment_writes[seg_idx_in_list];
        opt_.async_page_store->submit_typed_write(
            PagePurpose::kPageIndex, sw.addr, sw.blob.data(), sw.blob.size(),
            detail::own_async_completion([this, prepared, idx, on_done](const Status &st) mutable {
                if (!st.ok()) {
                    release_snapshot_slot();
                    on_done(st, 0);
                    return;
                }
                snapshot_write_next_async(std::move(prepared), idx + 1, std::move(on_done));
            }));
        return;
    }

    const PreparedSnapshotWrite &dw = prepared->directory_write;
    opt_.async_page_store->submit_typed_write(
        PagePurpose::kPageIndex, dw.addr, dw.blob.data(), dw.blob.size(),
        detail::own_async_completion([this, prepared, on_done](const Status &st) mutable {
            if (!st.ok()) {
                release_snapshot_slot();
                on_done(st, 0);
                return;
            }
            // Barrier: pages + segment images + directory durable before the anchor
            // that references them.
            Status fs1 = opt_.async_page_store->submit_fsync(
                detail::own_async_completion([this, prepared, on_done](const Status &st2) mutable {
                    if (!st2.ok()) {
                        release_snapshot_slot();
                        on_done(st2, 0);
                        return;
                    }
                    const PreparedSnapshotWrite &aw = prepared->anchor_write;
                    opt_.async_page_store->submit_typed_write(
                        PagePurpose::kPageIndex, aw.addr, aw.blob.data(), aw.blob.size(),
                        detail::own_async_completion([this, prepared, on_done](const Status &st3) mutable {
                            if (!st3.ok()) {
                                release_snapshot_slot();
                                on_done(st3, 0);
                                return;
                            }
                            snapshot_anchor_pending_ = true;
                            Status fs2               = opt_.async_page_store->submit_fsync(
                                detail::own_async_completion([this, prepared, on_done](const Status &st4) mutable {
                                    if (!st4.ok()) {
                                        CRB_LOG_ERROR("[{}] snapshot anchor sync failed: {}", name_, st4.to_string());
                                        commit_prepared_snapshot(*prepared);
                                        release_snapshot_slot();
                                        on_done(st4, 0);
                                        return;
                                    }
                                    snapshot_anchor_pending_ = false;
                                    finalize_prepared_snapshot(*prepared);
                                    uint64_t last_applied = prepared->last_applied_slot;
                                    release_snapshot_slot();
                                    on_done(Status::Ok(), last_applied);
                                }));
                            if (!fs2.ok()) {
                                commit_prepared_snapshot(*prepared);
                                release_snapshot_slot();
                                on_done(fs2, 0);
                            }
                        }));
                }));
            if (!fs1.ok()) {
                release_snapshot_slot();
                on_done(fs1, 0);
            }
        }));
}

Status Crowdbtree::persist_compaction_snapshot(std::vector<PrefetchedPage> prefetched,
                                               std::set<uint32_t> selected_blocks, PreparedSnapshot *prepared)
{
    acquire_snapshot_slot();
    Status pending = retry_snapshot_anchor();
    if (!pending.ok()) {
        release_snapshot_slot();
        return pending;
    }
    Status prepare;
    {
        std::scoped_lock lk(write_mutex_);
        prepare = prepare_snapshot_locked(prepared, std::move(prefetched), std::move(selected_blocks));
    }
    if (!prepare.ok()) {
        CRB_LOG_ERROR("[{}] compact_sparse_blocks: prepare failed: {}", name_, prepare.to_string());
        release_snapshot_slot();
        return prepare;
    }

    for (auto &write : prepared->page_writes) {
        write.blob.resize(round_up_to_iu(write.logical_len, opt_.page_store->iu_size()), 0);
        Status status = opt_.page_store->write_at(write.addr, write.blob.data(), write.blob.size());
        if (!status.ok()) {
            release_snapshot_slot();
            return status;
        }
        std::vector<uint8_t>().swap(write.blob);
    }
    for (auto &write : prepared->segment_writes) {
        Status status =
            opt_.page_store->write_typed_at(PagePurpose::kPageIndex, write.addr, write.blob.data(), write.blob.size());
        if (!status.ok()) {
            release_snapshot_slot();
            return status;
        }
    }
    Status directory =
        opt_.page_store->write_typed_at(PagePurpose::kPageIndex, prepared->directory_write.addr,
                                        prepared->directory_write.blob.data(), prepared->directory_write.blob.size());
    if (!directory.ok()) {
        release_snapshot_slot();
        return directory;
    }
    Status durable_contents = opt_.page_store->sync();
    if (!durable_contents.ok()) {
        release_snapshot_slot();
        return durable_contents;
    }
    Status anchor =
        opt_.page_store->write_typed_at(PagePurpose::kPageIndex, prepared->anchor_write.addr,
                                        prepared->anchor_write.blob.data(), prepared->anchor_write.blob.size());
    if (!anchor.ok()) {
        release_snapshot_slot();
        return anchor;
    }
    Status committed = opt_.page_store->sync();
    if (!committed.ok()) {
        std::vector<uint8_t> invalid_anchor(prepared->anchor_write.blob.size(), 0);
        Status rollback = opt_.page_store->write_typed_at(PagePurpose::kPageIndex, prepared->anchor_write.addr,
                                                          invalid_anchor.data(), invalid_anchor.size());
        if (rollback.ok()) {
            (void)opt_.page_store->sync();
        }
        release_snapshot_slot();
        return committed;
    }
    finalize_prepared_snapshot(*prepared);
    release_snapshot_slot();
    return Status::Ok();
}

} // namespace crowdb::tree
