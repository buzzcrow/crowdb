// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/crc32c.h"
#include "snapshot/snapshot_crc.h"

#include <gtest/gtest.h>

#include <string>

namespace crowdb::tree
{
TEST(SnapshotCrc, ConcatenationMatchesOnePassAcrossEmptyAndLongPieces)
{
    std::string bytes(70001, '\0');
    for (size_t i = 0; i < bytes.size(); ++i) {
        bytes[i] = static_cast<char>((i * 37) & 255U);
    }
    const auto *data  = reinterpret_cast<const uint8_t *>(bytes.data());
    const auto  whole = crowdb::common::crc32c(data, bytes.size());
    for (const size_t split :
         {size_t{0}, size_t{1}, size_t{25}, size_t{33}, size_t{4096}, size_t{65536}, bytes.size()}) {
        const auto prefix = crowdb::common::crc32c(data, split);
        const auto suffix = crowdb::common::crc32c(data + split, bytes.size() - split);
        EXPECT_EQ(snapshot_detail::crc_concatenate(prefix, suffix, bytes.size() - split), whole) << split;
    }
}
} // namespace crowdb::tree
