// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/btree/tree.h"
#include "crowdb-tree/config.h"
#include "runtime/default_runtime.h"

#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <thread>
#include <vector>

namespace crowdb::tree
{
TEST(DefaultTreeRuntime, DefaultGeometryFlushesWithinTheBindingReclaimLimit)
{
    Crowdbtree tree;
    ASSERT_TRUE(tree.put("key", "value").ok());
    const auto flushed = tree.flush();
    ASSERT_TRUE(flushed.ok()) << flushed.to_string();
    EXPECT_EQ(tree.get_view("key").value().to_string(), "value");
}

TEST(DefaultTreeRuntime, ConcurrentStandaloneConfigurationsShareOneHost)
{
    std::array<Config, 8>    configs;
    std::array<Status, 8>    statuses;
    std::barrier             ready(8);
    std::vector<std::thread> workers;
    workers.reserve(configs.size());
    for (size_t index = 0; index < configs.size(); ++index) {
        workers.emplace_back([&, index] {
            ready.arrive_and_wait();
            statuses[index] = configure_default_runtime(&configs[index]);
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    ASSERT_NE(configs.front().runtime, nullptr);
    for (size_t index = 0; index < configs.size(); ++index) {
        ASSERT_TRUE(statuses[index].ok()) << statuses[index].to_string();
        EXPECT_EQ(configs[index].runtime, configs.front().runtime);
    }
    const auto shared = configs.front().runtime;
    for (auto &config : configs) {
        config.runtime.reset();
    }
    Config reopened;
    ASSERT_TRUE(configure_default_runtime(&reopened).ok());
    EXPECT_EQ(reopened.runtime, shared);
    EXPECT_FALSE(reopened.runtime->stopped());
}

TEST(DefaultTreeRuntime, ExplicitHostAndBindingLimitsArePreserved)
{
    Config             config;
    TreeRuntimeOptions options;
    options.workers = 1;
    ASSERT_TRUE(TreeRuntime::create(options, &config.runtime).ok());
    const auto explicit_host               = config.runtime;
    config.runtime_binding.cpu_queue_limit = 3;
    ASSERT_TRUE(configure_default_runtime(&config).ok());
    EXPECT_EQ(config.runtime, explicit_host);
    EXPECT_EQ(config.runtime->options().workers, 1U);
    EXPECT_EQ(config.runtime_binding.cpu_queue_limit, 3U);
    EXPECT_EQ(configure_default_runtime(nullptr).code(), Code::kInvalidArgument);
}
} // namespace crowdb::tree
