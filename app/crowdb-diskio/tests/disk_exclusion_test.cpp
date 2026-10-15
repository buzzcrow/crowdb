// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "disk/mem_disk.h"
#include "engine/blocking/blocking_engine.h"
#include "group0/disk_exclusion.h"

#include <folly/json.h>
#include <gtest/gtest.h>

#include <set>

TEST(DiskExclusion, WaitsForDetachedCallbacksAndConfirmsOnlyExactRevisions)
{
    using namespace crowdb::diskio;
    DiskSet       disks;
    RetiredDisks  retired;
    DiskExclusion exclusion;
    const DiskId  id{.high = 1, .low = 2};
    auto          engine = std::make_shared<BlockingEngine>(1);
    disks.add(std::make_shared<MemDisk>(id, engine,
                                        std::vector<Zone>{
                                            {.zone_index = 0, .base_offset = 0, .capacity = 4096}
    }));
    auto callback = disks.find_disk(id);
    auto entries  = folly::parseJson(
        R"([{"disk_id":{"high":"1","low":"2"},"disk_group_id":7,"revision":"9","value":{"status":6}}])");
    exclusion.observe(entries, 0);
    EXPECT_TRUE(exclusion.pending(disks, retired).empty());
    retired.retire(disks, id);
    EXPECT_TRUE(exclusion.pending(disks, retired).empty());
    callback.reset();
    auto ready = retired.collect_quiescent();
    ASSERT_EQ(ready.size(), 1);
    ready.clear();
    auto batch = exclusion.pending(disks, retired);
    ASSERT_EQ(batch.size(), 1);
    EXPECT_EQ(batch[0]["disk_revision"].asString(), "9");
    exclusion.confirm(batch);
    EXPECT_TRUE(exclusion.pending(disks, retired).empty());
    entries[0]["revision"] = "10";
    exclusion.observe(entries, 0);
    exclusion.confirm(batch);
    EXPECT_EQ(exclusion.pending(disks, retired).size(), 1);
    entries[0]["value"]["status"] = 1;
    exclusion.observe(entries, 0);
    EXPECT_TRUE(exclusion.pending(disks, retired).empty());
}

TEST(DiskExclusion, BatchesRotatePastUnconfirmedDisksAndKeepUnsignedWordsExact)
{
    using namespace crowdb::diskio;
    DiskSet       disks;
    RetiredDisks  retired;
    DiskExclusion exclusion;
    auto          entries = folly::dynamic::array();
    for (uint64_t id = 0; id < 33; ++id) {
        entries.push_back(folly::dynamic::object(
            "disk_id", folly::dynamic::object("high", "18446744073709551615")("low", std::to_string(id)))(
            "disk_group_id", 7)("revision", "18446744073709551615")("value", folly::dynamic::object("status", 6)));
    }
    exclusion.observe(entries, 0);
    std::set<std::string> observed;
    for (int index = 0; index < 3; ++index) {
        auto batch = exclusion.pending(disks, retired);
        ASSERT_EQ(batch.size(), 16);
        for (const auto &entry : batch) {
            EXPECT_EQ(entry.at("disk_id_high").asString(), "18446744073709551615");
            EXPECT_EQ(entry.at("disk_revision").asString(), "18446744073709551615");
            observed.insert(entry.at("disk_id_low").asString());
        }
    }
    EXPECT_EQ(observed.size(), 33);
    entries[0]["revision"] = "0";
    EXPECT_THROW(exclusion.observe(entries, 0), std::exception);
}
