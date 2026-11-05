// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Independent immutable read owners survive asynchronous delivery and
// generation replacement; their final release retires the old publication.
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/snapshot/snapshot.h"
#include "runtime_tree_access.h"
#include "snapshot/published_snapshot_view.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace crowdb::tree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

std::string make_key(int i)
{
    std::array<char, 16> buf{};
    snprintf(buf.data(), buf.size(), "k%04d", i);
    return buf.data();
}
} // namespace

// A cold asynchronous read hands an independent immutable value owner to the
// callback, without carrying a caller-thread epoch guard.
TEST(R6, GetAsyncSlowPathReturnsBorrowedSlice)
{
    MemPageStore store(1);
    Config       opt;
    opt.page_store       = &store;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowdbtree t(opt);

    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "val" + std::to_string(i))).ok());
    }
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot(nullptr).ok()); // clean + evictable

    GetView warm;
    ASSERT_TRUE(RuntimeTreeAccess_for_tests::core(t).get(Slice(make_key(0)), &warm).ok());
    ASSERT_TRUE(warm.found());
    warm = {};

    // Evict all evictable leaves so the next get_async demand-loads (slow path).
    size_t evicted = t.evict_clean_leaves(0);
    ASSERT_GT(evicted, 0U) << "test requires at least one evictable leaf";

    std::string       k = make_key(0);
    std::atomic<bool> done{false};
    std::atomic<bool> borrowed{false};
    std::atomic<bool> found{false};

    t.get_async(Slice(k), [&](Status status, GetView v) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        found.store(v.found(), std::memory_order_relaxed);
        // frame_base() != nullptr iff the value is borrowed from a frame
        // (not an owned copy). The slow path must return a borrowed value.
        borrowed.store(v.frame_base() != nullptr, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });

    // Wait for the callback (synchronous on macOS, async on Linux).
    for (int i = 0; i < 2000 && !done.load(std::memory_order_acquire); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(done.load()) << "get_async callback never fired";
    EXPECT_TRUE(found.load()) << "key should be found";
    EXPECT_TRUE(borrowed.load()) << "slow path must return a borrowed Slice (frame_base != nullptr), not an owned copy";
}

// An immutable snapshot keeps its fixed mapping through generation install.
TEST(R6, PublishedSnapshotStaysConsistentAcrossInstallSnapshot)
{
    auto   store = std::make_shared<MemPageStore>(1);
    Config opt;
    opt.page_store       = store.get();
    opt.backend_owner    = store;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowdbtree t(opt);

    for (int i = 0; i < 200; ++i) {
        ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "val" + std::to_string(i))).ok());
    }
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    std::weak_ptr<const PublishedPage> old_root = RuntimeTreeAccess_for_tests::root(t);
    auto                               snap     = t.snapshot_view();
    ASSERT_NE(dynamic_cast<PublishedSnapshotView *>(snap.get()), nullptr);
    ASSERT_EQ(snap->size(), 200U);

    // The old mapping remains owned by the independent snapshot.
    ASSERT_TRUE(t.install_snapshot({}, 0).ok());

    // The pinned snapshot must still see all 200 keys (leaf chain not
    // truncated/mixed by the slot clears in install_snapshot).
    EXPECT_EQ(snap->size(), 200U) << "Published snapshot must stay consistent across install_snapshot";
    for (int i = 0; i < 200; ++i) {
        uint64_t    s;
        std::string v;
        EXPECT_TRUE(snap->get(Slice(make_key(i)), &s, &v)) << "key " << i << " missing after install_snapshot";
        EXPECT_EQ(v, "val" + std::to_string(i));
    }

    RuntimeTreeAccess_for_tests::reclaim(t);
    EXPECT_FALSE(old_root.expired());
    snap.reset();
    RuntimeTreeAccess_for_tests::reclaim(t);
    EXPECT_TRUE(old_root.expired());
}

// R6 scenario 3: concurrent readers + install_snapshot churn, no UAF.
// Threads do snapshot_view() and get_async() while install_snapshot()
// repeatedly replaces the tree. The refcount + EBR composition must keep
// every borrowed value alive until its handle drops.
TEST(R6, ConcurrentReadersAndInstallSnapshotNoUAF)
{
    MemPageStore store(1);
    Config       opt;
    opt.page_store       = &store;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    // Raw-backend snapshots materialize independently. Four concurrent
    // cursors must each fund a full supported value and its output owner.
    TreeRuntimeOptions runtime;
    runtime.limits.routing_bytes             = 1024ULL * 1024 * 1024;
    opt.runtime_binding.limits.routing_bytes = 768ULL * 1024 * 1024;
    ASSERT_TRUE(TreeRuntime::create(runtime, &opt.runtime).ok());
    Crowdbtree t(opt);

    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "val" + std::to_string(i))).ok());
    }
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    std::atomic<bool>   stop{false};
    std::atomic<bool>   bad{false};
    std::atomic<size_t> ready{0};

    // Reader threads: snapshot_view in a loop. Each snapshot is a
    // materialized copy (today) or a PinnedSnapshot (after T8-T11). The
    // entries() walk must not crash or read freed memory even as
    // install_snapshot churns the live tree underneath.
    std::vector<std::thread> readers;
    readers.reserve(4);
    for (int r = 0; r < 4; ++r) {
        readers.emplace_back([&] {
            bool reported_ready = false;
            while (!stop.load(std::memory_order_relaxed) && !bad.load(std::memory_order_relaxed)) {
                std::shared_ptr<Snapshot> snap;
                const auto status = create_published_snapshot(TreeCoreAccess::get(t)->shared_from_this(), &snap);
                if (status.code() == Code::kUnavailable) {
                    // Replacement closes new admission while draining the
                    // preceding generation; already accepted owners remain valid.
                    continue;
                }
                if (!status.ok()) {
                    ADD_FAILURE() << status.to_string();
                    bad.store(true, std::memory_order_relaxed);
                    return;
                }
                if (!reported_ready) {
                    reported_ready = true;
                    ready.fetch_add(1, std::memory_order_release);
                }
                if (snap == nullptr || snap->size() == 0) {
                    continue;
                }
                // Walk the snapshot's entries (reads from the materialized
                // copy / pinned frames). Verify every entry's value is
                // readable (no UAF) — the exact key set depends on which
                // install_snapshot iteration won the race.
                const auto &entries = snap->entries();
                for (const auto &e : entries) {
                    CellView cv{Slice(e.cell)};
                    if (cv.is_tombstone()) {
                        continue;
                    }
                    // Touch the value bytes — would crash on UAF.
                    (void)cv.value().to_string();
                }
            }
        });
    }

    for (size_t attempt = 0; attempt < 2000 && ready.load(std::memory_order_acquire) < 4 && !bad.load(); ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(ready.load(), 4U);
    if (ready.load() != 4) {
        bad.store(true);
    }

    // Writer thread: repeatedly install_snapshot to churn the tree.
    std::thread writer([&] {
        for (int i = 0; i < 50 && !bad.load(std::memory_order_relaxed); ++i) {
            std::vector<leaf_entry> entries;
            for (int j = 0; j < 20; ++j) {
                std::string k = make_key(j);
                std::string v = "v" + std::to_string(i) + "_" + std::to_string(j);
                entries.push_back({.key = k, .cell = encode_cell_buf((i * 100) + j, OpKind::kPut, Slice(v))});
            }
            const auto installed = t.install_snapshot(std::move(entries), (i * 100) + 20);
            if (!installed.ok()) {
                ADD_FAILURE() << "install iteration " << i << ": " << installed.to_string();
                bad.store(true, std::memory_order_relaxed);
            }
        }
    });

    writer.join();
    stop.store(true, std::memory_order_relaxed);
    for (auto &r : readers) {
        r.join();
    }
    EXPECT_FALSE(bad.load()) << "concurrent readers + install_snapshot churn must not corrupt or UAF";
}
