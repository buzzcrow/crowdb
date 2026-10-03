// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "group0/disk_identity.h"

#include <folly/json.h>
#include <gtest/gtest.h>

#include <limits>

TEST(Group0Identity, PreservesUnsignedWordsAndLegacySmallIntegers)
{
    auto value = folly::parseJson(R"({"high":"18446744073709551615","low":"12334355714748114139"})");
    EXPECT_EQ(crowdb::diskio::decode_disk_id_word(value["high"]), std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(crowdb::diskio::decode_disk_id_word(value["low"]), 12334355714748114139ULL);
    EXPECT_EQ(crowdb::diskio::decode_disk_id_word(folly::dynamic(42)), 42);
    for (const auto &invalid : {"-1", "18446744073709551616", "1x", ""}) {
        EXPECT_FALSE(crowdb::diskio::decode_disk_id_word(folly::dynamic(invalid)));
    }
}
