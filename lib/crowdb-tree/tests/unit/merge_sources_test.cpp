// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "btree/merge_sources.h"

#include <gtest/gtest.h>

#include <array>
#include <string>

using namespace crowdb::tree;

TEST(MergeSources, RefillingAnExhaustedNonWinnerPreservesOtherSources)
{
    ConcurrentSkipList first;
    ConcurrentSkipList second;
    ConcurrentSkipList refill;
    const auto         value = std::make_shared<CellVersion>(encode_cell_buf(1, OpKind::kPut, Slice("v")), 1, 0);
    for (const auto *key : {"a", "z"}) {
        first.upsert(key, value, 0);
    }
    for (const auto *key : {"c", "y"}) {
        second.upsert(key, value, 0);
    }
    refill.upsert("b", value, 0);
    auto                       first_cursor  = first.cursor({});
    auto                       second_cursor = second.cursor({});
    ConcurrentSkipList::Cursor refill_cursor;
    std::array<MergeSource, 3> sources{
        {
         {.kind = MergeSource::kL0, .l0 = &first_cursor, .l1 = nullptr},
         {.kind = MergeSource::kL0, .l0 = &second_cursor, .l1 = nullptr},
         {.kind = MergeSource::kL0, .l0 = &refill_cursor, .l1 = nullptr},
         }
    };
    LoserTree merge;
    merge.init(sources.data(), 3);
    ASSERT_EQ(sources[merge.winner()].key().to_string(), "a");
    merge.advance_winner();
    ASSERT_EQ(sources[merge.winner()].key().to_string(), "c");

    // A leaf cursor can be refilled after exhaustion while another source wins.
    refill_cursor = refill.cursor({});
    merge.replay_source(2);
    std::string keys;
    for (int i = 0; i < 5 && merge.winner_valid(); ++i) {
        keys += sources[merge.winner()].key().to_string();
        merge.advance_winner();
    }
    EXPECT_EQ(keys, "bcyz");
    EXPECT_FALSE(merge.winner_valid());
}
