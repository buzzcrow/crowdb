// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-common/crc32c.h"
#include "crowdb-tree/snapshot/snapshot_io.h"
#include "native_import_test_support.h"

namespace crowdb::tree
{
namespace
{
void test_u32(std::string &wire, uint32_t value)
{
    for (unsigned i = 0; i != 4; ++i) {
        wire.push_back(static_cast<char>(value >> (8 * i)));
    }
}

void test_u64(std::string &wire, uint64_t value)
{
    for (unsigned i = 0; i != 8; ++i) {
        wire.push_back(static_cast<char>(value >> (8 * i)));
    }
}

void test_crc(std::string &wire)
{
    test_u32(wire, crowdb::common::crc32c(reinterpret_cast<const uint8_t *>(wire.data()), wire.size()));
}

struct TestTuple
{
    std::string key;
    uint64_t    slot;
    uint8_t     kind;
    std::string value;
};

std::string test_portable(const std::vector<TestTuple> &entries, uint64_t coverage = 17)
{
    std::string wire;
    test_u32(wire, 0x4E535443);
    test_u32(wire, 1);
    wire.push_back(0);
    test_u64(wire, coverage);
    test_u64(wire, entries.size());
    for (const auto &e : entries) {
        test_u32(wire, e.key.size());
        wire += e.key;
        test_u64(wire, e.slot);
        wire.push_back(static_cast<char>(e.kind));
        test_u32(wire, e.value.size());
        wire += e.value;
    }
    test_crc(wire);
    return wire;
}

std::string test_native(const std::vector<NativeFrame> &frames, uint64_t coverage = 17)
{
    std::string wire;
    test_u32(wire, 0x4E535443);
    test_u32(wire, 1);
    wire.push_back(1);
    test_u64(wire, coverage);
    test_u64(wire, frames.empty() ? kInvalidPageId : 0);
    test_u64(wire, frames.size());
    for (const auto &f : frames) {
        test_u64(wire, f.page_id);
        test_u32(wire, f.frame.size());
        wire.append(reinterpret_cast<const char *>(f.frame.data()), f.frame.size());
    }
    test_crc(wire);
    return wire;
}

class RuntimeSnapshotImportTest : public testing::Test
{
  protected:
    void SetUp() override
    {
        ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
        config.frame_bytes                          = 4096;
        config.max_inline_value                     = 32;
        config.runtime_binding.limits.io_operations = 1;
        tree                                        = std::make_unique<Crowdbtree>(config);
        core                                        = TreeCoreAccess::get(*tree);
        ASSERT_TRUE(tree->put("old", "retained").ok());
    }

    static Status feed_chunks(SnapshotImport &importer, const std::string &wire, size_t chunk)
    {
        for (size_t offset = 0; offset < wire.size(); offset += chunk) {
            auto status = importer.feed(Slice(wire.data() + offset, std::min(chunk, wire.size() - offset)));
            if (!status.ok()) {
                return status;
            }
        }
        return Status::Ok();
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
    TreeCore                   *core = nullptr;
};

TEST_F(RuntimeSnapshotImportTest, PublicPortableInstallReplacesGenerationWithBalancedOverflowGraph)
{
    const auto              old = tree->get_view("old");
    std::vector<leaf_entry> entries;
    for (size_t i = 0; i != 200; ++i) {
        const auto key = std::string(3 - std::to_string(i).size(), '0') + std::to_string(i);
        entries.push_back({.key = key, .cell = encode_cell_buf(i + 1, OpKind::kPut, std::string(9000, 'v'))});
    }
    const auto status = tree->install_snapshot(std::move(entries), 250);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(core->capture()->stats.kv_known_count, 200U);
    EXPECT_GT(core->capture()->stats.height, 1U);
    EXPECT_EQ(tree->get_view("199").value().size(), 9000U);
    EXPECT_EQ(old.value().to_string(), "retained");
    ASSERT_TRUE(tree->put("next", "value").ok());
    EXPECT_EQ(tree->get_view("next").slot(), 251U);
}

TEST_F(RuntimeSnapshotImportTest, PortableEveryByteBoundaryPreservesEmptyKeyTombstoneAndLargeValue)
{
    const std::string value(9000, 'v');
    const auto        wire = test_portable({
        {.key = "",     .slot = 3, .kind = 0, .value = "empty"},
        {.key = "gone", .slot = 5, .kind = 1, .value = ""     },
        {.key = "key",  .slot = 7, .kind = 0, .value = value  }
    });
    const auto        old  = tree->get_view("old");
    SnapshotImport    importer(*tree);
    auto              status = feed_chunks(importer, wire, 1);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(tree->get_view("old").value().to_string(), "retained");
    uint64_t coverage = 0;
    status            = importer.finish(&coverage);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(coverage, 17U);
    EXPECT_EQ(tree->get_view("").value().to_string(), "empty");
    EXPECT_FALSE(tree->get_view("gone").found());
    EXPECT_EQ(tree->get_view("key").value().to_string(), value);
    EXPECT_EQ(old.value().to_string(), "retained");
    ASSERT_TRUE(tree->put("next", "value").ok());
    EXPECT_EQ(tree->get_view("next").slot(), 18U);
}

TEST_F(RuntimeSnapshotImportTest, CrcTruncationOrderAndFieldFailuresNeverReplaceTheOldRoot)
{
    const auto               original = core->capture();
    std::vector<std::string> invalid;
    auto                     crc = test_portable({
        {.key = "key", .slot = 7, .kind = 0, .value = "value"}
    });
    crc.back() ^= 1;
    invalid.push_back(std::move(crc));
    auto truncated = test_portable({
        {.key = "key", .slot = 7, .kind = 0, .value = "value"}
    });
    truncated.pop_back();
    invalid.push_back(std::move(truncated));
    auto trailing = test_portable({});
    trailing.push_back('x');
    invalid.push_back(std::move(trailing));
    invalid.push_back(test_portable({
        {.key = "z", .slot = 7, .kind = 0, .value = "value"},
        {.key = "a", .slot = 7, .kind = 0, .value = "value"}
    }));
    invalid.push_back(test_portable({
        {.key = "future", .slot = 18, .kind = 0, .value = "value"}
    }));
    invalid.push_back(test_portable({
        {.key = "bad", .slot = 7, .kind = 2, .value = "value"}
    }));
    invalid.push_back(test_portable({
        {.key = "bad", .slot = 7, .kind = 1, .value = "value"}
    }));
    auto       oversized     = test_portable({
        {.key = "key", .slot = 7, .kind = 0, .value = "value"}
    });
    const auto length_offset = 25 + 4 + 3 + 8 + 1;
    for (size_t i = 0; i != 4; ++i) {
        oversized[length_offset + i] = static_cast<char>(0xff);
    }
    invalid.push_back(std::move(oversized));
    for (const auto &wire : invalid) {
        SnapshotImport importer(*tree);
        auto           status = feed_chunks(importer, wire, 13);
        if (status.ok()) {
            status = importer.finish(nullptr);
        }
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(core->capture(), original);
        EXPECT_EQ(tree->get_view("old").value().to_string(), "retained");
    }
    ASSERT_TRUE(tree->put("after", "works").ok());
    EXPECT_EQ(tree->get_view("after").slot(), 2U);
}

TEST_F(RuntimeSnapshotImportTest, NativeStreamsRetainOnlyChargedFramesBeforeAtomicInstall)
{
    const std::string value(9000, 'n');
    const auto        wire    = test_native(test_import::overflow_tree(value));
    const auto        account = core->binding()->account();
    const auto        reclaim = static_cast<size_t>(detail::BudgetClass::kReclaim);
    SnapshotImport    importer(*tree);
    auto              status = feed_chunks(importer, wire, 17);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_GT(account->snapshot().allocated[reclaim], wire.size());
    EXPECT_EQ(config.runtime->snapshot().reserved.io_operations, 0U);
    EXPECT_EQ(core->capture()->coverage, 0U);
    status = importer.finish(nullptr);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(tree->get_view("key").value().to_string(), value);
    EXPECT_EQ(core->capture()->coverage, 17U);
}

TEST_F(RuntimeSnapshotImportTest, IdlePartialImportHoldsNoIoTicketAndCloseDoesNotWaitForItsOwner)
{
    const auto account = core->binding()->account();
    const auto wire    = test_portable({
        {.key = "key", .slot = 7, .kind = 0, .value = "value"}
    });
    {
        SnapshotImport importer(*tree);
        ASSERT_TRUE(importer.feed(Slice(wire.data(), 9)).ok());
        auto io = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kIo, 1);
        ASSERT_TRUE(io);
        io.reset();
        core->close_and_drain();
        EXPECT_EQ(importer.feed(Slice(wire.data() + 9, wire.size() - 9)).code(), Code::kUnavailable);
    }
    const auto reclaim = static_cast<size_t>(detail::BudgetClass::kReclaim);
    EXPECT_EQ(account->snapshot().allocated[reclaim], 0U);
    EXPECT_EQ(account->snapshot().reserved[reclaim], 0U);
}

TEST_F(RuntimeSnapshotImportTest, EmptyStreamsKeepCoverageAndAdmissionFailureLeavesSourceUsable)
{
    for (bool native : {false, true}) {
        SnapshotImport importer(*tree);
        const auto     wire = native ? test_native({}, 23) : test_portable({}, 23);
        ASSERT_TRUE(feed_chunks(importer, wire, 1).ok());
        auto status = importer.finish(nullptr);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(tree->contiguous_slot(), 23U);
        ASSERT_TRUE(tree->put("after", "value").ok());
        EXPECT_EQ(tree->get_view("after").slot(), 24U);
    }
    const auto original = core->capture();
    const auto account  = core->binding()->account();
    const auto index    = static_cast<size_t>(detail::BudgetClass::kReclaim);
    const auto sample   = account->snapshot();
    auto       pressure = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kReclaim,
                                                              config.runtime_binding.limits.reclaim_bytes -
                                                                  sample.allocated[index] - sample.reserved[index]);
    ASSERT_TRUE(pressure);
    SnapshotImport importer(*tree);
    EXPECT_EQ(importer.feed(test_portable({})).code(), Code::kResourceExhausted);
    EXPECT_EQ(core->capture(), original);
}
} // namespace
} // namespace crowdb::tree
