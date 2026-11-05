// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_load_delta_test_support.h"

namespace crowdb::tree
{
using namespace test_read;

TEST(PublishedReadDelta, GetUsesFoldedSnapshotAndCoverageOnlyFiltersMemTables)
{
    TestDeltaLeaf fixture;
    auto          loader = fixture.loader();
    auto          leaf   = std::make_shared<PageState>();
    leaf->bases.push_back(fixture.reference);
    auto table = std::make_shared<MemTable>();
    ASSERT_TRUE(table->upsert("a", 7, encode_cell_buf(7, OpKind::kPut, "covered")));
    ASSERT_TRUE(table->upsert("b", 8, encode_cell_buf(8, OpKind::kPut, "active")));
    const auto captured = view(leaf, {table}, 7);
    for (const auto &[key, value] : std::array{
             std::pair{"a",   "delta"   },
             std::pair{"b",   "active"  },
             std::pair{"new", "first"   },
             std::pair{"tie", "base-tie"}
    }) {
        std::shared_ptr<PublishedGet> request;
        ASSERT_TRUE(PublishedGet::create(captured, loader, key, {}, &request).ok());
        fixture.complete(request->sender());
        ASSERT_TRUE(request->result().found);
        EXPECT_EQ(request->result().cell.value, Slice(value));
    }
    for (const auto *key : {"c", "zero"}) {
        std::shared_ptr<PublishedGet> request;
        ASSERT_TRUE(PublishedGet::create(captured, loader, key, {}, &request).ok());
        fixture.complete(request->sender());
        EXPECT_FALSE(request->result().found);
        EXPECT_TRUE(request->result().cell.tombstone());
    }
}

TEST(PublishedReadDelta, ForwardAndReverseScanMergeBaseDeltaAndDeletes)
{
    TestDeltaLeaf fixture;
    auto          loader = fixture.loader();
    auto          leaf   = std::make_shared<PageState>();
    leaf->bases.push_back(fixture.reference);
    for (bool reverse : {false, true}) {
        std::shared_ptr<PublishedScan> request;
        ASSERT_TRUE(PublishedScan::create(view(leaf, {}, 7), loader, {}, reverse, {.max_entries = 1}, &request).ok());
        std::vector<std::pair<std::string, std::string>> actual;
        for (size_t i = 0; i < 20; ++i) {
            fixture.complete(request->next_sender());
            for (const auto &entry : request->result().entries) {
                actual.emplace_back(Slice(entry.key).to_string(), entry.cell.value.to_string());
            }
            if (request->result().done) {
                break;
            }
        }
        ASSERT_TRUE(request->result().done);
        std::vector<std::pair<std::string, std::string>> expected{
            {"a",   "delta"     },
            {"b",   "base-newer"},
            {"new", "first"     },
            {"tie", "base-tie"  }
        };
        if (reverse) {
            std::reverse(expected.begin(), expected.end());
        }
        EXPECT_EQ(actual, expected);
    }
}
} // namespace crowdb::tree
