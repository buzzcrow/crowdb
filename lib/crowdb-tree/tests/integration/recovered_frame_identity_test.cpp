// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "local_recovery_test_support.h"
#include "native_import_test_support.h"
#include "recovered_state_test_support.h"

namespace crowdb::tree
{
namespace
{
TEST(RecoveredFrameIdentity, ConcreteLeafAndOverflowPidMismatchesRemainCorruption)
{
    for (const uint64_t pid : {0U, 1U}) {
        test_write::TestRecovery test;
        test.checkpoint(std::string(8000, 'v'));
        const auto [address, length] = test.locations.at(pid);
        std::vector<uint8_t> blob(length);
        ASSERT_TRUE(test.store->device->read_at(address, blob.data(), blob.size()).ok());
        std::vector<uint8_t> raw(durable_blob_raw_len(blob.data(), blob.size()));
        ASSERT_TRUE(decode_durable_page(blob.data(), blob.size(), raw.data(), raw.size()).ok());
        frame_put_u64(raw.data(), fh::kSelfpage_id, pid + 99);
        frame_restamp_crc(raw.data(), raw.size());
        encode_durable_page(raw.data(), raw.size(), compress_algo::kLz4, &blob);
        ASSERT_LE(blob.size(), length);
        blob.resize(length);
        ASSERT_TRUE(test.store->device->write_at(address, blob.data(), blob.size()).ok());
        ASSERT_TRUE(test.prepare().ok());
        test_write::Completion done;
        test.run(&done);
        EXPECT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(done.status.code(), Code::kCorruption);
        EXPECT_FALSE(test.operation->result().latest);
    }
}

TEST(RecoveredFrameIdentity, ConcreteInnerPidMismatchIsRejectedBeforePublication)
{
    test_read::TestRecoveredGraph test;
    test.balanced();
    std::vector<uint8_t> raw(4096);
    ASSERT_TRUE(inner_frame_build(raw.data(), raw.size(), 99, {1, 2}, {"m"}));
    test.add(0, raw);
    ASSERT_TRUE(test.prepare().ok());
    test_read::Completion done;
    test.run(&done);
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(done.status.code(), Code::kCorruption);
    EXPECT_FALSE(test.operation->result().state);
}

TEST(RecoveredFrameIdentity, NativeImportStillRejectsEveryConcreteFrameIdentityMismatch)
{
    for (const auto type : {page_type::kLeafBase, page_type::kInnerBase, page_type::kOverflowFrame}) {
        Config config;
        ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
        config.frame_bytes = 4096;
        Crowdbtree tree(config);
        auto       frames = test_import::overflow_tree(std::string(8000, 'v'));
        if (type == page_type::kInnerBase) {
            frames = {
                NativeFrame{.page_id = 0, .frame = std::vector<uint8_t>(4096)},
                test_import::inline_leaf(1)
            };
            ASSERT_TRUE(inner_frame_build(frames[0].frame.data(), 4096, 0, {1}, {}));
        }
        const auto count  = frames.size();
        auto       status = tree.install_snapshot_native(frames, 0, 7, count);
        ASSERT_TRUE(status.ok()) << status.to_string();
        const auto original = TreeCoreAccess::get(tree)->capture();
        size_t     modified = 0;
        for (auto &frame : frames) {
            if (frame_page_type(frame.frame.data()) == type) {
                frame_put_u64(frame.frame.data(), fh::kSelfpage_id, frame.page_id + 99);
                frame_restamp_crc(frame.frame.data(), frame.frame.size());
                ++modified;
            }
        }
        ASSERT_GT(modified, 0U);
        status = tree.install_snapshot_native(std::move(frames), 0, 7, count);
        EXPECT_EQ(status.code(), Code::kCorruption);
        EXPECT_EQ(TreeCoreAccess::get(tree)->capture(), original);
    }
}
} // namespace
} // namespace crowdb::tree
