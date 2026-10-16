// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "disk/mem_disk.h"
#include "engine/blocking/blocking_engine.h"
#include "group0/disk_membership.h"

#include <folly/json.h>
#include <gtest/gtest.h>

TEST(DiskMembership, OfflineExcludesNewRequestsAndRetainsInflightHandles)
{
    using namespace crowdb::diskio;
    DiskSet      disks;
    RetiredDisks retired;
    const DiskId id{.high = 1, .low = 2};
    auto         engine = std::make_shared<BlockingEngine>(1);
    disks.add(std::make_shared<MemDisk>(id, engine,
                                        std::vector<Zone>{
                                            {.zone_index = 0, .base_offset = 0, .capacity = 4096}
    }));
    auto in_flight = disks.find_disk(id);
    auto snapshot  = DiskMembership::parse(
        folly::parseJson(R"([{"disk_id":{"high":"1","low":"2"},"disk_group_id":7,"value":{"status":6}}])"), 0);
    EXPECT_TRUE(snapshot.groups.contains(7));
    ASSERT_EQ(snapshot.exclude(disks, retired).size(), 1);
    EXPECT_EQ(disks.find_disk(id), nullptr);
    EXPECT_TRUE(retired.contains(id));
    EXPECT_TRUE(retired.collect_quiescent().empty());
    in_flight.reset();
    EXPECT_EQ(retired.collect_quiescent().size(), 1);
}

TEST(DiskMembership, OtherHealthStatesPreserveHandlesForRecoveryAndUnknownStatesReject)
{
    using namespace crowdb::diskio;
    auto entries = folly::parseJson(R"([{"disk_id":{"high":"1","low":"2"},"value":{"status":0}}])");
    for (int status = 0; status < 6; ++status) {
        entries[0]["value"]["status"] = status;
        auto snapshot                 = DiskMembership::parse(entries, 9);
        EXPECT_TRUE(snapshot.active.contains({.high = 1, .low = 2}));
        EXPECT_TRUE(snapshot.groups.contains(9));
    }
    for (int status : {-1, 7}) {
        entries[0]["value"]["status"] = status;
        EXPECT_THROW(DiskMembership::parse(entries, 9), std::exception);
    }
    entries[0]["value"].erase("status");
    EXPECT_THROW(DiskMembership::parse(entries, 9), std::exception);
}

TEST(DiskMembership, InvalidOrConflictingSnapshotCannotPartiallyExcludeDisks)
{
    using namespace crowdb::diskio;
    auto entries = folly::parseJson(R"([{"disk_id":{"high":"1","low":"2"},"value":{"status":6}}])");
    entries.push_back(entries[0]);
    EXPECT_THROW(DiskMembership::parse(entries, 1), std::exception);
    entries[1]["disk_id"]["high"] = "invalid";
    EXPECT_THROW(DiskMembership::parse(entries, 1), std::exception);
    EXPECT_THROW(DiskMembership::parse(folly::dynamic::object(), 1), std::exception);
    EXPECT_TRUE(DiskMembership::parse(folly::dynamic::array(), 1).active.empty());
}
