// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "local_checkpoint_test_support.h"
#include "snapshot/published_snapshot_view.h"

#include <barrier>
#include <stdexcept>
#include <thread>

namespace crowdb::tree
{
namespace
{
struct TestViewBackend
{
    std::shared_ptr<MemPageStore>   device = std::make_shared<MemPageStore>(4096);
    test_write::TestCheckpointStore store;

    TestViewBackend()
    {
        store.device = device;
    }
};

struct TestSnapshotView
{
    Config                           config;
    std::shared_ptr<TestViewBackend> backend;
    std::unique_ptr<Crowdbtree>      tree;

    explicit TestSnapshotView(bool storage = true, bool owned = true)
    {
        EXPECT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
        config.frame_bytes      = 4096;
        config.max_inline_value = 64;
        if (storage) {
            backend                 = std::make_shared<TestViewBackend>();
            config.page_store       = backend->device.get();
            config.async_page_store = &backend->store;
            if (owned) {
                config.backend_owner = backend;
            }
        }
    }

    [[nodiscard]] Status open()
    {
        if (config.page_store != nullptr) {
            return Crowdbtree::open(config, &tree);
        }
        tree = std::make_unique<Crowdbtree>(config);
        return Status::Ok();
    }

    [[nodiscard]] Status capture(std::shared_ptr<Snapshot> *out) const
    {
        try {
            *out = tree->snapshot_view();
            return Status::Ok();
        }
        catch (const std::exception &error) {
            return Status::internal_error(error.what());
        }
    }
};

TEST(PublishedSnapshotView, FixedL1GetAndEntriesSurviveSharedBackendTreeClose)
{
    TestSnapshotView test;
    auto             status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string value(12000, 'v');
    ASSERT_TRUE(test.tree
                    ->apply(1,
                            Batch{
                                .ops = {{.key = "a", .kind = OpKind::kPut, .value = value},
                                        {.key = "gone", .kind = OpKind::kDelete, .value = ""}}
    })
                    .ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    ASSERT_TRUE(test.tree->put("a", "pending").ok());
    std::shared_ptr<Snapshot> snapshot;
    status = test.capture(&snapshot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(snapshot->at_slot(), 1U);
    EXPECT_EQ(TreeCoreAccess::get(*test.tree)->capture()->coverage, 1U);
    test.tree.reset();
    uint64_t    slot = 0;
    std::string actual;
    ASSERT_TRUE(snapshot->get("a", &slot, &actual));
    EXPECT_EQ(slot, 1U);
    EXPECT_EQ(actual, value);
    EXPECT_FALSE(snapshot->get("gone", nullptr, nullptr));
    const auto &entries = snapshot->entries();
    ASSERT_EQ(entries.size(), 2U);
    EXPECT_EQ(entries[0].key, "a");
    EXPECT_EQ(CellView(Slice(entries[0].cell)).value().to_string(), value);
    EXPECT_TRUE(CellView(Slice(entries[1].cell)).is_tombstone());
    EXPECT_EQ(&snapshot->entries(), &entries);
}

TEST(PublishedSnapshotView, RawBackendMaterializesBeforeReturningAndReleasesDomain)
{
    TestSnapshotView test(true, false);
    auto             status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string value(12000, 'r');
    ASSERT_TRUE(test.tree->put("a", value).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    std::shared_ptr<Snapshot> snapshot;
    status = test.capture(&snapshot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    test.tree.reset();
    status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::string actual;
    ASSERT_TRUE(snapshot->get("a", nullptr, &actual));
    EXPECT_EQ(actual, value);
    EXPECT_EQ(snapshot->size(), 1U);
    test.tree.reset();
    test.backend.reset();
    EXPECT_EQ(CellView(Slice(snapshot->entries()[0].cell)).value().to_string(), value);
}

TEST(PublishedSnapshotView, ConcurrentWholeResultCallsPublishOneStableVector)
{
    TestSnapshotView test(false);
    test.config.runtime_binding.limits.routing_bytes = test.config.runtime->options().limits.routing_bytes;
    auto status                                      = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(test.tree->put("a", std::string(16000, 'x')).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    std::shared_ptr<Snapshot> snapshot;
    status = test.capture(&snapshot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::barrier                   start(3);
    const std::vector<leaf_entry> *first  = nullptr;
    const std::vector<leaf_entry> *second = nullptr;
    std::exception_ptr             first_error;
    std::exception_ptr             second_error;
    std::jthread                   left([&] {
        start.arrive_and_wait();
        try {
            first = &snapshot->entries();
        }
        catch (...) {
            first_error = std::current_exception();
        }
    });
    std::jthread                   right([&] {
        start.arrive_and_wait();
        try {
            second = &snapshot->entries();
        }
        catch (...) {
            second_error = std::current_exception();
        }
    });
    start.arrive_and_wait();
    left.join();
    right.join();
    EXPECT_FALSE(first_error);
    EXPECT_FALSE(second_error);
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first, second);
    EXPECT_EQ(first->size(), 1U);
}

TEST(PublishedSnapshotView, WholeResultLimitFailsExplicitlyWhilePointReadsStayUsable)
{
    TestSnapshotView test;
    auto             status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(test.tree->put("a", std::string(12000, 'l')).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    std::shared_ptr<PublishedSnapshotContext> context;
    ASSERT_TRUE(TreeCoreAccess::get(*test.tree)->capture_snapshot(&context).ok());
    std::shared_ptr<Snapshot> snapshot;
    status = PublishedSnapshotView::create(context, 1024, &snapshot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_THROW(static_cast<void>(snapshot->entries()), std::runtime_error);
    std::string actual;
    ASSERT_TRUE(snapshot->get("a", nullptr, &actual));
    EXPECT_EQ(actual, std::string(12000, 'l'));
    detail::ReservedUnique<SnapshotMaterialized> materialized;
    status = materialize_snapshot(context, 1024, &materialized);
    EXPECT_EQ(status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(materialized);
}
} // namespace
} // namespace crowdb::tree
