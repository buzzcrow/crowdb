// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "disk/mem_disk.h"
#include "disk/retired_disks.h"
#include "engine/blocking/blocking_engine.h"

#include <fcntl.h>
#include <gtest/gtest.h>

TEST(RetiredDisks, DetachesNewRequestsButPreservesDescriptorUntilLastReaderCompletes)
{
    using namespace crowdb::diskio;
    DiskSet      active;
    RetiredDisks retired;
    auto         engine = std::make_shared<BlockingEngine>(1);
    active.add(std::make_shared<MemDisk>(
        DiskId{
            .high = 1, .low = 2
    },
        engine, std::vector<Zone>{{.zone_index = 0, .base_offset = 0, .capacity = 4096}}));
    auto request = active.find_disk({.high = 1, .low = 2});
    ASSERT_NE(request, nullptr);
    const int fd = request->fd();
    ASSERT_GE(fd, 0);
    retired.retire(active, {.high = 1, .low = 2});
    EXPECT_EQ(active.find_disk({.high = 1, .low = 2}), nullptr);
    EXPECT_TRUE(retired.contains({.high = 1, .low = 2}));
    EXPECT_TRUE(retired.collect_quiescent().empty());
    EXPECT_GE(fcntl(fd, F_GETFD), 0);
    request.reset();
    auto ready = retired.collect_quiescent();
    ASSERT_EQ(ready.size(), 1);
    EXPECT_EQ(ready.front()->id(), (DiskId{.high = 1, .low = 2}));
    EXPECT_GE(fcntl(fd, F_GETFD), 0);
    EXPECT_FALSE(retired.contains({.high = 1, .low = 2}));
    ready.clear();
    EXPECT_EQ(fcntl(fd, F_GETFD), -1);
    EXPECT_TRUE(retired.collect_quiescent().empty());
}

TEST(RetiredDisks, ASlowDiskDoesNotDelayUnrelatedQuiescentDisks)
{
    using namespace crowdb::diskio;
    DiskSet      active;
    RetiredDisks retired;
    auto         engine = std::make_shared<BlockingEngine>(1);
    for (uint64_t low : {1, 2}) {
        active.add(std::make_shared<MemDisk>(
            DiskId{
                .high = 1, .low = low
        },
            engine, std::vector<Zone>{{.zone_index = 0, .base_offset = 0, .capacity = 4096}}));
    }
    auto request = active.find_disk({.high = 1, .low = 1});
    retired.retire(active, {.high = 1, .low = 1});
    retired.retire(active, {.high = 1, .low = 2});
    retired.retire(active, {.high = 9, .low = 9});
    auto ready = retired.collect_quiescent();
    ASSERT_EQ(ready.size(), 1);
    EXPECT_EQ(ready.front()->id(), (DiskId{.high = 1, .low = 2}));
    EXPECT_TRUE(retired.contains({.high = 1, .low = 1}));
    EXPECT_FALSE(retired.contains({.high = 9, .low = 9}));
    request.reset();
    ready = retired.collect_quiescent();
    ASSERT_EQ(ready.size(), 1);
    EXPECT_EQ(ready.front()->id(), (DiskId{.high = 1, .low = 1}));
}
