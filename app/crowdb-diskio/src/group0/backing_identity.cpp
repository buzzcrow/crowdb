// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "group0/backing_identity.h"

#include <sys/ioctl.h>
#include <sys/stat.h>

#ifdef __linux__
#    include <linux/fs.h>
#    include <linux/loop.h>
#endif

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <utility>

namespace crowdb::diskio
{
namespace
{

bool matches_resource(int fd, const struct stat &opened, const std::string &expected)
{
    if (S_ISREG(opened.st_mode)) {
        return expected == "file:" + std::to_string(opened.st_dev) + ":" + std::to_string(opened.st_ino);
    }
#ifdef __linux__
    if (S_ISBLK(opened.st_mode)) {
        struct loop_info64 loop{};
        if (ioctl(fd, LOOP_GET_STATUS64, &loop) == 0) {
            return loop.lo_inode != 0 && loop.lo_rdevice == 0 && loop.lo_offset == 0 && loop.lo_sizelimit == 0 &&
                   expected == "file:" + std::to_string(loop.lo_device) + ":" + std::to_string(loop.lo_inode);
        }
        // ENXIO on an unbound loop is not evidence of an independent device.
        return errno == ENOTTY && expected == "device:" + std::to_string(opened.st_rdev);
    }
#endif
    return false;
}

} // namespace

bool matches_backing_identity(int fd, const std::string &path, const folly::dynamic &storage)
{
    struct stat opened{};
    struct stat current{};
    if (fd < 0 || fstat(fd, &opened) != 0 || lstat(path.c_str(), &current) != 0 || S_ISLNK(current.st_mode) ||
        opened.st_dev != current.st_dev || opened.st_ino != current.st_ino || opened.st_rdev != current.st_rdev) {
        return false;
    }
    try {
        auto version = storage.at("version").asInt();
        if ((version != 1 && version != 2) || storage.at("removed").asBool()) {
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
        const auto *physical = storage.get_ptr("physical_backing");
        if (version == 1) {
            if (physical != nullptr && !physical->isNull()) {
                return false;
            }
        }
        else {
            if (physical == nullptr || !physical->isObject()) {
                return false;
            }
            auto  physical_capacity = physical->at("capacity_bytes").asInt();
            auto *host              = std::getenv("CROWDB_PHYSICAL_HOST_ID");
            if (physical_capacity <= 0 || std::cmp_less(physical_capacity, capacity) || host == nullptr ||
                physical->at("physical_host_id").asString() != host ||
                !matches_resource(fd, opened, physical->at("resource_identity").asString())) {
                return false;
            }
            capacity = static_cast<uint64_t>(physical_capacity);
        }
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
