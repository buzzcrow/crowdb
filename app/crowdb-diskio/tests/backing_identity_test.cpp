// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/runtime_path.h"
#include "group0/backing_identity.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>

TEST(BackingIdentity, RetainedFileRejectsReplacementTruncationSymlinkAndWrongBackend)
{
    auto root = crowdb::common::test_runtime_path("diskio-backing-identity");
    std::filesystem::create_directories(root);
    std::string pattern = (root / "disk-XXXXXX").string();
    int         fd      = mkstemp(pattern.data());
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, 8192), 0);
    struct stat metadata{};
    ASSERT_EQ(fstat(fd, &metadata), 0);
    folly::dynamic storage = folly::dynamic::object("version", 1)("removed", false)("backend", "file")(
        "backing_identity", "file:" + std::to_string(metadata.st_dev) + ":" + std::to_string(metadata.st_ino))(
        "geometry", folly::dynamic::object("device_path", pattern)("capacity_units", 2)("unit_size_bytes", 4096));
    EXPECT_TRUE(crowdb::diskio::matches_backing_identity(fd, pattern, storage));
    storage["backend"] = "block-device";
    EXPECT_FALSE(crowdb::diskio::matches_backing_identity(fd, pattern, storage));
    storage["backend"] = "file";
    ASSERT_EQ(ftruncate(fd, 4096), 0);
    EXPECT_FALSE(crowdb::diskio::matches_backing_identity(fd, pattern, storage));
    ASSERT_EQ(ftruncate(fd, 8192), 0);
    std::string retained = pattern + ".retained";
    ASSERT_EQ(rename(pattern.c_str(), retained.c_str()), 0);
    int replacement = open(pattern.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    ASSERT_GE(replacement, 0);
    ASSERT_EQ(ftruncate(replacement, 8192), 0);
    EXPECT_FALSE(crowdb::diskio::matches_backing_identity(fd, pattern, storage));
    EXPECT_FALSE(crowdb::diskio::matches_backing_identity(replacement, pattern, storage));
    close(replacement);
    ASSERT_EQ(unlink(pattern.c_str()), 0);
    ASSERT_EQ(symlink(retained.c_str(), pattern.c_str()), 0);
    EXPECT_FALSE(crowdb::diskio::matches_backing_identity(fd, pattern, storage));
    unlink(pattern.c_str());
    unlink(retained.c_str());
    close(fd);
}
