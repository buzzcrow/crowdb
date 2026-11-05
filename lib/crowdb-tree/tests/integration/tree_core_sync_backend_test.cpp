// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "crowdb-tree/backend/page_store.h"
#include "snapshot/published_snapshot.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
TEST(TreeCoreSyncBackend, OwnedSynchronousBackendCheckpointsReopensAndOutlivesCore)
{
    Config             config;
    TreeRuntimeOptions runtime_options;
    runtime_options.workers         = 1;
    runtime_options.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(runtime_options, &config.runtime).ok());
    auto store                                  = std::make_shared<MemPageStore>(4096);
    config.page_store                           = store.get();
    config.backend_owner                        = store;
    config.frame_bytes                          = 4096;
    config.max_inline_value                     = 64;
    config.runtime_binding.limits.io_operations = 1;
    std::shared_ptr<TreeCore> core;
    auto                      status = TreeCore::open(config, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string large(12000, 'v');
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = large}}}).ok());
    status = core->checkpoint();
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    ASSERT_TRUE(core->capture_snapshot(&snapshot).ok());
    EXPECT_TRUE(snapshot->detached());
    core.reset();
    GetView old;
    status = snapshot->get("a", &old);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(old.found());
    EXPECT_EQ(old.value().to_string(), large);
    snapshot.reset();
    status = TreeCore::open(config, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
    GetView recovered;
    status = core->get("a", &recovered);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(recovered.found());
    EXPECT_EQ(recovered.value().to_string(), large);
    EXPECT_EQ(core->capture()->coverage, 1U);
}

TEST(TreeCoreSyncBackend, RawSynchronousSnapshotCannotOutliveItsBackendDomain)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    MemPageStore store(4096);
    config.page_store  = &store;
    config.frame_bytes = 4096;
    std::shared_ptr<TreeCore> core;
    auto                      status = TreeCore::open(config, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    ASSERT_TRUE(core->capture_snapshot(&snapshot).ok());
    EXPECT_FALSE(snapshot->detached());
    GetView value;
    ASSERT_TRUE(snapshot->get("a", &value).ok());
    core.reset();
    EXPECT_EQ(snapshot->get("a", &value).code(), Code::kUnavailable);
    status = TreeCore::open(config, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
}
} // namespace
} // namespace crowdb::tree
