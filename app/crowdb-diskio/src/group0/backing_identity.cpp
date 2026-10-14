// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "group0/backing_identity.h"

#include <sys/ioctl.h>
#include <sys/stat.h>

#ifdef __linux__
#    include <linux/fs.h>
#endif

#include <cstdint>
#include <limits>
#include <utility>

namespace crowdb::diskio
{

bool matches_backing_identity(int fd, const std::string &path, const folly::dynamic &storage)
{
    struct stat opened{};
    struct stat current{};
    if (fd < 0 || fstat(fd, &opened) != 0 || lstat(path.c_str(), &current) != 0 || S_ISLNK(current.st_mode) ||
        opened.st_dev != current.st_dev || opened.st_ino != current.st_ino || opened.st_rdev != current.st_rdev) {
        return false;
    }
    try {
        if (storage.at("version").asInt() != 1 || storage.at("removed").asBool()) {
            return false;
        }
        const auto &geometry  = storage.at("geometry");
        auto        units     = geometry.at("capacity_units").asInt();
        auto        unit_size = geometry.at("unit_size_bytes").asInt();
        if (geometry.at("device_path").asString() != path || units <= 0 || unit_size <= 0 ||
            static_cast<uint64_t>(units) > std::numeric_limits<uint64_t>::max() / static_cast<uint64_t>(unit_size)) {
            return false;
        }
        auto        capacity = static_cast<uint64_t>(units) * static_cast<uint64_t>(unit_size);
        std::string identity;
        if (storage.at("backend").asString() == "file" && S_ISREG(opened.st_mode)) {
            identity = "file:" + std::to_string(opened.st_dev) + ":" + std::to_string(opened.st_ino);
            if (std::cmp_not_equal(opened.st_size, capacity)) {
                return false;
            }
        }
        else if (storage.at("backend").asString() == "block-device" && S_ISBLK(opened.st_mode)) {
            identity = "device:" + std::to_string(opened.st_rdev);
#ifdef __linux__
            uint64_t actual = 0;
            if (ioctl(fd, BLKGETSIZE64, &actual) != 0 || actual != capacity) {
                return false;
            }
#else
            return false;
#endif
        }
        else {
            return false;
        }
        return storage.at("backing_identity").asString() == identity;
    }
    catch (const std::exception &) {
        return false;
    }
}

} // namespace crowdb::diskio
