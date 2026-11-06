// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Crash / torn-write recovery across the full feature set (compression, overflow,
// IU alignment). Verifies two-generation fallback (a corrupted newest snapshot
// falls back intact to the previous committed image) and that demand-load
// corruption of the committed image is surfaced via the latched io_failed flag.
#include "crowdb-tree/backend/block_page_store.h"
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/maptable/mapping_persist.h"
#include "runtime_tree_access.h"
#include "snapshot/commit_anchor.h"
#include "test_tmp.h"

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <cstdio>
#include <map>
#include <memory>
#include <string>
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
    std::array<char, 16> b{};
    snprintf(b.data(), b.size(), "k%05d", i);
    return b.data();
}

std::string big_val(size_t n, char c)
{
    return std::string(n, c); // NOLINT(modernize-return-braced-init-list)
}

constexpr uint64_t kSbBytes = 4096;
} // namespace

// plan-tree #14e: FaultyPageStore-driven crash injection (declarative, in
// place of hand-computed byte offsets). A write failing mid-snapshot must
// leave the previous generation fully recoverable -- snapshot() itself
// reports the failure, and no anchor for the failed generation is ever
// written.
TEST(CrashRecovery, FaultInjectedWriteFailureLeavesPreviousGenerationIntact)
{
    MemPageStore    mem(1);
    FaultyPageStore store(&mem);
    Config          opt;
    opt.page_store = &store;

    std::unique_ptr<Crowdbtree> t;
    ASSERT_TRUE(Crowdbtree::open(opt, &t).ok());
    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(t->apply(i + 1, put_one(make_key(i), "gen1-" + std::to_string(i))).ok());
    }
    ASSERT_TRUE(t->flush().ok());
    ASSERT_TRUE(t->snapshot(nullptr).ok()); // gen1 commits

    int gen1_writes = store.write_count();

    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(t->apply(100 + i, put_one(make_key(i), "gen2-" + std::to_string(i))).ok());
    }
    t->force_advance_slot(119); // slots 100..119 are non-contiguous with gen1's 1..20
    ASSERT_TRUE(t->flush().ok());
    // Fail the very first byte this snapshot generation tries to write.
    store.arm_write_fault(gen1_writes, FaultyPageStore::Fault::kFail);
    EXPECT_FALSE(t->snapshot(nullptr).ok()); // gen2 must fail, not silently succeed

    // gen2's anchor was never written -- gen1 is still the last committed
    // generation. Reopen (fault already disarmed -- one-shot) and confirm.
    t.reset();
    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    for (int i = 0; i < 20; ++i) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(make_key(i)), &s, &v));
        EXPECT_EQ(v, "gen1-" + std::to_string(i));
    }
}

// A committed anchor whose segment image fails CRC is media corruption;
// recovery must not silently fall back to an older logical image.
TEST(CrashRecovery, CorruptSegmentImageFailsReopenEvenThoughAnchorCommitted)
{
    MemPageStore store(1);
    Config       options;
    options.page_store = &store;
    std::unique_ptr<Crowdbtree> tree;
    ASSERT_TRUE(Crowdbtree::open(options, &tree).ok());
    for (int index = 0; index < 5; ++index) {
        ASSERT_TRUE(tree->apply(index + 1, put_one(make_key(index), "first")).ok());
    }
    ASSERT_TRUE(tree->snapshot().ok());
    ASSERT_TRUE(tree->put(make_key(0), "second").ok());
    ASSERT_TRUE(tree->snapshot().ok());
    std::vector<snapshot::CommitAnchor> anchors;
    ASSERT_TRUE(snapshot::read_valid_anchors(store, store.iu_size(), &anchors).ok());
    ASSERT_EQ(anchors.size(), 2U);
    const auto anchor = *std::max_element(anchors.begin(), anchors.end(), [](const auto &left, const auto &right) {
        return left.snapshot_seq < right.snapshot_seq;
    });
    std::vector<uint8_t> directory(anchor.segdir_len);
    ASSERT_TRUE(store.read_at(anchor.segdir_addr, directory.data(), directory.size()).ok());
    std::vector<DirEntry> entries;
    ASSERT_TRUE(decode_segment_directory(directory.data(), directory.size(), &entries).ok());
    ASSERT_FALSE(entries.empty());
    const auto damaged = entries.front();
    tree.reset();
    std::vector<uint8_t> zeros(damaged.image_len, 0);
    ASSERT_TRUE(store.write_at(damaged.image_addr, zeros.data(), zeros.size()).ok());
    EXPECT_FALSE(Crowdbtree::open(options, &tree).ok());
    EXPECT_FALSE(tree);
}

// ckpt1 (large overflow values + compression) commits; a second snapshot then
// commits new values; corrupting the newest superblock must fall back to ckpt1's
// fully-intact image, including the multi-frame overflow values.
TEST(CrashRecovery, TwoGenerationFallbackWithOverflowAndCompression)
{
    MemPageStore store(1);
    Config       opt;
    opt.page_store       = &store;
    opt.compression      = compress_algo::kLz4;
    opt.frame_bytes      = 4096;
    opt.max_inline_value = 64; // force overflow chains
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 512;

    std::map<std::string, std::string> gen1;
    {
        Crowdbtree t(opt);
        uint64_t   slot = 0;
        for (int i = 0; i < 20; ++i) {
            ++slot;
            std::string v = big_val(5000 + i, static_cast<char>('A' + (i % 26))); // ~2 frames
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), v)).ok());
            ASSERT_TRUE(t.flush().ok());
            gen1[make_key(i)] = v;
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok()); // seq 1 -> slot 0

        // Second generation: overwrite a few keys, then snapshot (seq 2 -> slot B).
        for (int i = 0; i < 20; i += 5) {
            ++slot;
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), big_val(7000, 'Z'))).ok());
            ASSERT_TRUE(t.flush().ok());
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok()); // seq 2 -> slot kSbBytes
    }

    // Simulate a crash that left the newest superblock unreadable.
    std::vector<uint8_t> garbage(kSbBytes, 0x5a);
    ASSERT_TRUE(store.write_at(kSbBytes, garbage.data(), garbage.size()).ok());

    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    // Falls back to gen1: every key (incl. overflow values) matches the first ckpt.
    for (const auto &kv : gen1) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second) << "value mismatch " << kv.first;
    }
    EXPECT_FALSE(t2->io_failed()); // fallback image is intact, no media fault
}

// Same two-generation fallback on an aligned (iu=4096) block device.
TEST(CrashRecovery, AlignedTwoGenerationFallback)
{
    MemPageStore store(4096);
    Config       opt;
    opt.page_store       = &store;
    opt.frame_bytes      = 4096;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 256;

    std::map<std::string, std::string> gen1;
    {
        Crowdbtree t(opt);
        uint64_t   slot = 0;
        for (int i = 0; i < 60; ++i) {
            ++slot;
            std::string v = "v" + std::to_string(i);
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), v)).ok());
            ASSERT_TRUE(t.flush().ok());
            gen1[make_key(i)] = v;
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok());
        for (int i = 0; i < 60; i += 7) {
            ++slot;
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), "new")).ok());
            ASSERT_TRUE(t.flush().ok());
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok());
    }
    std::vector<uint8_t> garbage(kSbBytes, 0xcd);
    ASSERT_TRUE(store.write_at(kSbBytes, garbage.data(), garbage.size()).ok());

    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    for (const auto &kv : gen1) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
}

// Corrupting a page body in the *committed* image (valid superblock) is a media
// fault: the read degrades to a miss but io_failed() latches true.
TEST(CrashRecovery, DemandLoadCorruptionLatched)
{
    MemPageStore store(1);
    Config       opt;
    uint64_t     off     = 0;
    opt.page_store       = &store;
    opt.frame_bytes      = 4096;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 256; // multi-page so a single corruption is localized
    {
        Crowdbtree t(opt);
        for (int i = 0; i < 60; ++i) {
            ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "v" + std::to_string(i))).ok());
            ASSERT_TRUE(t.flush().ok());
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok());
        RuntimeTreeAccess_for_tests::capture(t)->mapping.visit(
            &off, [](void *raw, uint64_t, const std::shared_ptr<const PublishedPage> &page) {
                auto &offset = *static_cast<uint64_t *>(raw);
                if (offset == 0 && page->leaf && !page->leaf->bases.empty()) {
                    const auto &backing = page->leaf->bases.front().backing;
                    if (backing && backing->length > 1) {
                        offset = backing->address + (backing->length / 2);
                    }
                }
            });
    }
    ASSERT_GT(off, 2 * kSbBytes);
    uint8_t b = 0;
    ASSERT_TRUE(store.read_at(off, &b, 1).ok());
    const uint8_t corrupt = b ^ 0xff;
    ASSERT_TRUE(store.write_at(off, &corrupt, 1).ok());

    std::unique_ptr<Crowdbtree> t2;
    // Recovery verifies the persisted graph before exposing its cold mapping.
    EXPECT_EQ(Crowdbtree::open(opt, &t2).code(), Code::kCorruption);
    ASSERT_FALSE(t2);
    ASSERT_TRUE(store.write_at(off, &b, 1).ok());
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    EXPECT_FALSE(t2->io_failed()); // nothing loaded yet (lazy recovery)
    ASSERT_TRUE(store.write_at(off, &corrupt, 1).ok());

    // Read every key: the corrupted page demand-load fails CRC and latches.
    for (int i = 0; i < 60; ++i) {
        std::string v;
        uint64_t    s;
        (void)t2->get(Slice(make_key(i)), &s, &v); // some reads succeed, the corrupt one misses
    }
    EXPECT_TRUE(t2->io_failed());

    t2->clear_io_error();
    EXPECT_FALSE(t2->io_failed());
}

// File-backed (real fsync) crash: gen2's snapshot commits, but a crash tears
// its final superblock write (the engine writes pages+manifest, syncs, THEN the
// superblock — so a torn superblock is the realistic mid-commit crash). Reopen
// must fall back to gen1's committed image, fully intact, on a real file.
TEST(CrashRecovery, FileTornSuperblockFallsBack)
{
    crowdb::tree_test::TempDir tmp("crash_");
    ASSERT_FALSE(tmp.path.empty());
    std::string path = tmp.path;

    std::map<std::string, std::string> gen1;
    {
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open_blocks(path, 0, 0, 8 * 1024 * 1024, 1, &store).ok());
        Config opt;
        opt.page_store       = store.get();
        opt.frame_bytes      = 4096;
        opt.compression      = compress_algo::kLz4;
        opt.max_delta_len    = 1;
        opt.leaf_split_bytes = 256;
        Crowdbtree t(opt);
        for (int i = 0; i < 80; ++i) {
            ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "v" + std::to_string(i))).ok());
            ASSERT_TRUE(t.flush().ok());
            gen1[make_key(i)] = "v" + std::to_string(i);
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok()); // gen1: seq 1 -> superblock slot 0

        for (int i = 0; i < 80; i += 4) {
            ASSERT_TRUE(t.apply(1000 + i, put_one(make_key(i), "GEN2")).ok());
            ASSERT_TRUE(t.flush().ok());
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok()); // gen2: seq 2 -> superblock slot 4096
    }

    // Tear the gen2 superblock (slot B at offset 4096) as a crash would.
    {
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open_blocks(path, 0, 0, 8 * 1024 * 1024, 1, &store).ok());
        std::vector<uint8_t> garbage(4096, 0x77);
        ASSERT_TRUE(store->write_at(4096, garbage.data(), garbage.size()).ok());
        ASSERT_TRUE(store->sync().ok());
    }

    {
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open_blocks(path, 0, 0, 8 * 1024 * 1024, 1, &store).ok());
        Config opt;
        opt.page_store  = store.get();
        opt.frame_bytes = 4096;
        opt.compression = compress_algo::kLz4;
        std::unique_ptr<Crowdbtree> t;
        ASSERT_TRUE(Crowdbtree::open(opt, &t).ok());
        for (const auto &kv : gen1) {
            std::string v;
            uint64_t    s;
            ASSERT_TRUE(t->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
            EXPECT_EQ(v, kv.second); // gen1 value, not GEN2 (gen2 never committed)
        }
        EXPECT_FALSE(t->io_failed());
    }
}
