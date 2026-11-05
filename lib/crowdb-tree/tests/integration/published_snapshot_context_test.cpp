// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "local_checkpoint_test_support.h"
#include "snapshot/published_snapshot.h"

namespace crowdb::tree
{
namespace
{
struct TestSnapshotBackend
{
    std::shared_ptr<MemPageStore>   device = std::make_shared<MemPageStore>(4096);
    test_write::TestCheckpointStore store;

    TestSnapshotBackend()
    {
        store.device = device;
    }
};

struct TestSnapshotContext
{
    std::shared_ptr<TreeRuntime>         runtime;
    std::shared_ptr<TestSnapshotBackend> backend = std::make_shared<TestSnapshotBackend>();
    Config                               config;
    std::shared_ptr<TreeCore>            core;

    explicit TestSnapshotContext(bool owned)
    {
        EXPECT_TRUE(TreeRuntime::create({}, &runtime).ok());
        config.runtime                              = runtime;
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
        return TreeCore::open(config, &core);
    }

    Status put(uint64_t slot, const std::string &value)
    {
        return core->apply(slot, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = value}}});
    }
};

TEST(PublishedSnapshotContext, ExcludesLiveSourcesAndKeepsOneImmutableL1Coverage)
{
    TestSnapshotContext test(true);
    auto                status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(test.put(1, "old").ok());
    ASSERT_TRUE(test.core->flush().ok());
    ASSERT_TRUE(test.put(2, "new").ok());
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    status = PublishedSnapshotContext::capture(test.core, &snapshot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(snapshot->view()->coverage, 1U);
    EXPECT_TRUE(snapshot->view()->sources->tables.empty());
    EXPECT_EQ(snapshot->view()->sources->floor, 1U);
    ASSERT_TRUE(test.core->flush().ok());
    GetView result;
    status = snapshot->get("a", &result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(result.found());
    EXPECT_EQ(result.slot(), 1U);
    EXPECT_EQ(result.value().to_string(), "old");
}

TEST(PublishedSnapshotContext, SharedBackendAndDomainOutliveCoreWithoutPinningRegistry)
{
    TestSnapshotContext test(true);
    auto                status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string value(12000, 'x');
    ASSERT_TRUE(test.put(1, value).ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    status = PublishedSnapshotContext::capture(test.core, &snapshot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(snapshot->detached());
    std::weak_ptr<LocalBackendState> old_local = test.core->local_backend();
    test.core.reset();
    EXPECT_TRUE(old_local.expired());
    EXPECT_EQ(test.open().code(), Code::kUnavailable);
    GetView result;
    status = snapshot->get("a", &result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(result.found());
    EXPECT_EQ(result.value().to_string(), value);
    EXPECT_EQ(result.slot(), 1U);
    snapshot.reset();
    status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(result.value().to_string(), value);
}

TEST(PublishedSnapshotContext, RawBackendIdleHandleDoesNotBlockCloseAndOwnedReadSurvives)
{
    TestSnapshotContext test(false);
    auto                status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(test.put(1, "retained").ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    status = PublishedSnapshotContext::capture(test.core, &snapshot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_FALSE(snapshot->detached());
    GetView owned;
    status = snapshot->get("a", &owned);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(owned.found());
    test.core.reset();
    GetView after;
    status = snapshot->get("a", &after);
    EXPECT_EQ(status.code(), Code::kUnavailable) << status.to_string();
    EXPECT_FALSE(after.found());
    EXPECT_EQ(owned.value().to_string(), "retained");
    status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
}

TEST(PublishedSnapshotContext, OversizeWindowFailsBeforeIoOrAdmissionAndSingleIoGetsCanRepeat)
{
    TestSnapshotContext test(true);
    auto                status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(test.put(1, "one").ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    ASSERT_TRUE(PublishedSnapshotContext::capture(test.core, &snapshot).ok());
    SnapshotReadWindow window;
    EXPECT_EQ(snapshot->prepare_window(SIZE_MAX, &window).code(), Code::kResourceExhausted);
    EXPECT_FALSE(window.loader);
    EXPECT_FALSE(window.ticket);
    GetView first;
    status = snapshot->get("a", &first);
    ASSERT_TRUE(status.ok()) << status.to_string();
    GetView second;
    status = snapshot->get("a", &second);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(first.value().to_string(), "one");
    EXPECT_EQ(second.value().to_string(), "one");
}
} // namespace
} // namespace crowdb::tree
