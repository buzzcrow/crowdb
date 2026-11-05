// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/maptable/compressor.h"
#include "local_checkpoint_test_support.h"
#include "snapshot/published_native.h"

#include <set>

namespace crowdb::tree
{
namespace
{
struct TestNativeBackend
{
    std::shared_ptr<MemPageStore>   device = std::make_shared<MemPageStore>(4096);
    test_write::TestCheckpointStore store;

    TestNativeBackend()
    {
        store.device = device;
    }
};

struct TestNativeExport
{
    std::shared_ptr<TestNativeBackend> backend = std::make_shared<TestNativeBackend>();
    Config                             config;
    std::unique_ptr<Crowdbtree>        tree;

    explicit TestNativeExport(bool owned = true)
    {
        EXPECT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
        config.page_store                           = backend->device.get();
        config.async_page_store                     = &backend->store;
        config.frame_bytes                          = 4096;
        config.max_inline_value                     = 64;
        config.runtime_binding.limits.io_operations = 1;
        if (owned) {
            config.backend_owner = backend;
        }
    }

    Status open()
    {
        return Crowdbtree::open(config, &tree);
    }

    [[nodiscard]] Status put(uint64_t slot, std::string key, std::string value) const
    {
        return tree->apply(slot,
                           Batch{.ops = {{.key = std::move(key), .kind = OpKind::kPut, .value = std::move(value)}}});
    }

    static void append(const NativeFrameView &frame, std::vector<NativeFrame> *out)
    {
        ASSERT_TRUE(frame.owner);
        ASSERT_FALSE(frame.bytes.empty());
        EXPECT_TRUE(frame_validate(frame.bytes.data(), frame.bytes.size()));
        EXPECT_EQ(frame_u64(frame.bytes.data(), fh::kSelfpage_id), frame.page_id);
        EXPECT_EQ(frame.durable_addr, kNoAddr);
        EXPECT_EQ(frame.durable_plen, 0U);
        EXPECT_FALSE(frame.inherited);
        out->push_back({.page_id      = frame.page_id,
                        .frame        = std::vector<uint8_t>(frame.bytes.begin(), frame.bytes.end()),
                        .durable_addr = kNoAddr,
                        .durable_plen = 0,
                        .inherited    = false});
    }

    static void import_and_read(std::vector<NativeFrame> frames, NativeStreamInfo info, const std::string &expected)
    {
        Config destination;
        destination.frame_bytes = 4096;
        Crowdbtree copy(destination);
        auto       status =
            copy.install_snapshot_native(std::move(frames), info.root_page_id, info.at_slot, info.next_page_id);
        ASSERT_TRUE(status.ok()) << status.to_string();
        std::string value;
        uint64_t    slot = 0;
        ASSERT_TRUE(copy.get("a", &slot, &value));
        EXPECT_EQ(value, expected);
        EXPECT_EQ(slot, 1U);
    }
};

TEST(PublishedNative, FreshOverflowHasExactFrameCountAndStableOwnedWindows)
{
    TestNativeExport test;
    auto             status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string old_value(14000, 'v');
    ASSERT_TRUE(test.put(1, "a", old_value).ok());
    std::unique_ptr<NativeFrameIterator> iterator;
    status = test.tree->open_native_frame_iterator(nullptr, &iterator);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto info = iterator->stream_info();
    ASSERT_TRUE(info.frame_count_known);
    EXPECT_EQ(info.at_slot, 1U);
    EXPECT_GT(info.frame_count, 2U);
    const auto leased_next = TreeCoreAccess::get(*test.tree)->capture()->next_page_id;
    EXPECT_EQ(info.next_page_id, leased_next);
    ASSERT_TRUE(test.put(2, "a", "newer").ok());
    ASSERT_TRUE(test.tree->flush().ok());

    std::vector<NativeFrameView> retained;
    std::vector<NativeFrame>     frames;
    std::set<uint64_t>           pids;
    bool                         done = false;
    while (!done) {
        std::optional<NativeFrameView> frame;
        status = iterator->next_owned(&frame, &done);
        ASSERT_TRUE(status.ok()) << status.to_string();
        if (frame) {
            EXPECT_TRUE(pids.insert(frame->page_id).second);
            TestNativeExport::append(*frame, &frames);
            retained.push_back(std::move(*frame));
        }
        EXPECT_EQ(test.config.runtime->snapshot().reserved.io_operations, 0U);
    }
    ASSERT_EQ(frames.size(), info.frame_count);
    for (size_t i = 0; i < retained.size(); ++i) {
        EXPECT_EQ(std::vector<uint8_t>(retained[i].bytes.begin(), retained[i].bytes.end()), frames[i].frame);
    }
    TestNativeExport::import_and_read(std::move(frames), info, old_value);
}

TEST(PublishedNative, RecoveredOverflowRemainsReadableAfterOwnedBackendTreeCloses)
{
    TestNativeExport test;
    ASSERT_TRUE(test.open().ok());
    const std::string value(12000, 'c');
    ASSERT_TRUE(test.put(1, "a", value).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    std::unique_ptr<NativeFrameIterator> iterator;
    auto                                 status = test.tree->open_native_frame_iterator(nullptr, &iterator);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto info = iterator->stream_info();
    test.tree.reset();
    EXPECT_EQ(test.open().code(), Code::kUnavailable);
    std::vector<NativeFrame> frames;
    bool                     done = false;
    while (!done) {
        std::optional<NativeFrameView> frame;
        status = iterator->next_owned(&frame, &done);
        ASSERT_TRUE(status.ok()) << status.to_string();
        if (frame) {
            TestNativeExport::append(*frame, &frames);
        }
    }
    EXPECT_EQ(frames.size(), info.frame_count);
    TestNativeExport::import_and_read(std::move(frames), info, value);
    iterator.reset();
    status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
}

TEST(PublishedNative, RawBackendIdleCursorDoesNotBlockCloseAndKeepsReturnedBytes)
{
    TestNativeExport test(false);
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, "a", std::string(12000, 'r')).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    std::unique_ptr<NativeFrameIterator> iterator;
    auto                                 status = test.tree->open_native_frame_iterator(nullptr, &iterator);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::optional<NativeFrameView> first;
    bool                           done = false;
    status                              = iterator->next_owned(&first, &done);
    ASSERT_TRUE(status.ok()) << status.to_string();
    if (!first.has_value()) {
        FAIL() << "native cursor returned no owned frame";
    }
    ASSERT_FALSE(done);
    const auto                &retained = first.value();
    const std::vector<uint8_t> copy(retained.bytes.begin(), retained.bytes.end());
    test.tree.reset();
    std::optional<NativeFrameView> after;
    status = iterator->next_owned(&after, &done);
    EXPECT_EQ(status.code(), Code::kUnavailable) << status.to_string();
    EXPECT_FALSE(after);
    EXPECT_EQ(std::vector<uint8_t>(retained.bytes.begin(), retained.bytes.end()), copy);
    status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
}

TEST(PublishedNative, FrameCountLimitRejectsBeforeReservingOverflowIds)
{
    TestNativeExport test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, "a", std::string(12000, 'b')).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    auto                                   core   = TreeCoreAccess::get(*test.tree)->shared_from_this();
    const auto                             before = core->capture()->next_page_id;
    std::shared_ptr<PublishedNativeCursor> cursor;
    auto status = PublishedNativeCursor::create(core, {.max_frames = 1, .max_dependency_steps = 16}, &cursor);
    EXPECT_EQ(status.code(), Code::kResourceExhausted) << status.to_string();
    EXPECT_FALSE(cursor);
    EXPECT_EQ(core->capture()->next_page_id, before);
    EXPECT_EQ(test.config.runtime->snapshot().reserved.io_operations, 0U);
}

TEST(PublishedNative, CorruptColdOverflowCannotBeReportedAsCompleteOutput)
{
    TestNativeExport test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, "a", std::string(12000, 'x')).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    auto      *core = TreeCoreAccess::get(*test.tree);
    const auto view = core->capture();
    const auto page = view->mapping.get(view->root_page_id);
    ASSERT_TRUE(page && page->leaf && !page->leaf->bases.empty());
    const auto primary = page->leaf->bases.front().backing;
    ASSERT_TRUE(primary && primary->overflow && !primary->overflow->pages.empty());
    const auto                           broken = primary->overflow->pages.begin()->second.backing;
    std::unique_ptr<NativeFrameIterator> iterator;
    auto                                 status = test.tree->open_native_frame_iterator(nullptr, &iterator);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::vector<uint8_t> blob(broken->length);
    ASSERT_TRUE(test.backend->device->read_at(broken->address, blob.data(), blob.size()).ok());
    blob.back() ^= 0xff;
    // Corrupt inside the logical blob rather than IU alignment padding.
    blob[kDurableBlobHeader + 32] ^= 0xff;
    ASSERT_TRUE(test.backend->device->write_at(broken->address, blob.data(), blob.size()).ok());
    bool     complete = false;
    uint64_t emitted  = 0;
    do {
        std::optional<NativeFrameView> frame;
        status = iterator->next_owned(&frame, &complete);
        if (frame) {
            ++emitted;
        }
    } while (status.ok() && !complete);
    EXPECT_EQ(status.code(), Code::kCorruption) << status.to_string();
    EXPECT_FALSE(complete);
    EXPECT_LT(emitted, iterator->stream_info().frame_count);
}
} // namespace
} // namespace crowdb::tree
