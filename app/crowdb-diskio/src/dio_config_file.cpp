// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "dio_config.h"

#include <toml++/toml.h>

#include <cstdlib>
#include <cstring>
#include <limits>

namespace crowdb::diskio
{
namespace
{

bool parse_disk_id(const char *text, DiskId &out)
{
    out.high          = 0;
    out.low           = 0;
    const char *colon = std::strchr(text, ':');
    char       *end   = nullptr;
    if (colon != nullptr) {
        out.high = std::strtoull(text, &end, 16);
        if (end != colon) {
            return false;
        }
        out.low = std::strtoull(colon + 1, &end, 16);
    }
    else {
        out.low = std::strtoull(text, &end, 16);
    }
    return *end == '\0';
}

template <typename T> bool read_value(const toml::table &table, std::string_view key, T &out, std::string &err)
{
    const auto node = table[key];
    if (!node) {
        return true;
    }
    const auto value = node.template value<T>();
    if (!value.has_value()) {
        err = "invalid type for " + std::string(key);
        return false;
    }
    out = *value;
    return true;
}

bool read_u32(const toml::table &table, std::string_view key, uint32_t &out, std::string &err)
{
    int64_t value = 0;
    if (!read_value(table, key, value, err)) {
        return false;
    }
    if (!table[key]) {
        return true;
    }
    if (value < 0 || value > std::numeric_limits<uint32_t>::max()) {
        err = "value out of range for " + std::string(key);
        return false;
    }
    out = static_cast<uint32_t>(value);
    return true;
}

bool read_u64(const toml::table &table, std::string_view key, uint64_t &out, std::string &err)
{
    int64_t value = 0;
    if (!read_value(table, key, value, err)) {
        return false;
    }
    if (!table[key]) {
        return true;
    }
    if (value < 0) {
        err = "value out of range for " + std::string(key);
        return false;
    }
    out = static_cast<uint64_t>(value);
    return true;
}

bool read_section(const toml::table &root, std::string_view name, const toml::table *&section, std::string &err)
{
    const auto node = root[name];
    if (!node) {
        section = nullptr;
        return true;
    }
    section = node.as_table();
    if (section == nullptr) {
        err = std::string(name) + " must be a table";
        return false;
    }
    return true;
}

bool read_server(const toml::table &root, DioConfig &config, std::string &err)
{
    const toml::table *server = nullptr;
    if (!read_section(root, "server", server, err) || server == nullptr) {
        return server == nullptr && err.empty();
    }

    int64_t     listen_port = config.listen_port;
    std::string dummy_disk_type;
    if (!read_value(*server, "bind_address", config.bind_address, err) ||
        !read_value(*server, "listen_port", listen_port, err) ||
        !read_u32(*server, "rpc_workers", config.rpc_workers, err) ||
        !read_u64(*server, "node_id", config.node_id, err) || !read_value(*server, "o_direct", config.o_direct, err) ||
        !read_value(*server, "dummy_disk_type", dummy_disk_type, err)) {
        return false;
    }
    if (listen_port < 0 || listen_port > 65535) {
        err = "value out of range for listen_port";
        return false;
    }
    config.listen_port = static_cast<int>(listen_port);
    if ((*server)["dummy_disk_type"] && !parse_dummy_disk_type(dummy_disk_type, config.dummy_disk_type)) {
        err = "invalid server.dummy_disk_type (null|mem)";
        return false;
    }

    uint32_t   latency_min = 0;
    uint32_t   latency_max = 0;
    double     error_rate  = 0.0;
    const bool has_min     = static_cast<bool>((*server)["fault_latency_min_ms"]);
    const bool has_max     = static_cast<bool>((*server)["fault_latency_max_ms"]);
    const bool has_rate    = static_cast<bool>((*server)["fault_error_rate"]);
    if (has_min != has_max) {
        err = "fault_latency_min_ms and fault_latency_max_ms must be set together";
        return false;
    }
    if (!read_u32(*server, "fault_latency_min_ms", latency_min, err) ||
        !read_u32(*server, "fault_latency_max_ms", latency_max, err) ||
        !read_value(*server, "fault_error_rate", error_rate, err)) {
        return false;
    }
    if (has_min && latency_min > latency_max) {
        err = "server.fault_latency_min_ms must be <= fault_latency_max_ms";
        return false;
    }
    if (has_rate && (error_rate < 0.0 || error_rate > 1.0)) {
        err = "server.fault_error_rate must be in 0.0..1.0";
        return false;
    }
    if (has_min || has_rate) {
        config.dummy_props                 = DiskProperties{};
        config.dummy_props->latency_min_ms = latency_min;
        config.dummy_props->latency_max_ms = latency_max;
        config.dummy_props->error_rate     = error_rate;
    }
    return true;
}

bool read_group0(const toml::table &root, DioConfig &config, std::string &err)
{
    const toml::table *group0 = nullptr;
    if (!read_section(root, "group0", group0, err) || group0 == nullptr) {
        return group0 == nullptr && err.empty();
    }
    if (!read_u64(*group0, "instance_id", config.instance_id, err) ||
        !read_u64(*group0, "rack_id", config.rack_id, err) || !read_u64(*group0, "disk_group_id", config.dg_id, err) ||
        !read_u32(*group0, "sync_interval_ms", config.sync_interval_ms, err) ||
        !read_value(*group0, "auto_discover_disks", config.auto_discover_disks, err)) {
        return false;
    }
    if (const auto seeds = (*group0)["kv_seeds"]; seeds) {
        const auto *array = seeds.as_array();
        if (array == nullptr) {
            err = "invalid type for kv_seeds";
            return false;
        }
        config.kv_seeds.clear();
        for (const auto &node : *array) {
            const auto value = node.value<std::string>();
            if (!value.has_value()) {
                err = "group0.kv_seeds entries must be strings";
                return false;
            }
            config.kv_seeds.push_back(*value);
        }
    }
    return true;
}

bool read_disks(const toml::table &root, DioConfig &config, std::string &err)
{
    const auto disks = root["disk"];
    if (!disks) {
        return true;
    }
    const auto *array = disks.as_array();
    if (array == nullptr) {
        err = "disk must be an array of tables";
        return false;
    }
    config.disks.clear();
    for (const auto &node : *array) {
        const auto *disk = node.as_table();
        if (disk == nullptr) {
            err = "disk entries must be tables";
            return false;
        }
        std::string id;
        std::string path;
        int64_t     capacity = 1LL << 40;
        if (!read_value(*disk, "id", id, err) || !(*disk)["id"] || !read_value(*disk, "path", path, err) ||
            !(*disk)["path"] || !read_value(*disk, "zone_capacity", capacity, err)) {
            if (err.empty()) {
                err = "disk.id and disk.path are required";
            }
            return false;
        }
        if (capacity <= 0) {
            err = "disk.zone_capacity must be > 0";
            return false;
        }
        DiskEntry entry;
        if (!parse_disk_id(id.c_str(), entry.id)) {
            err = "invalid disk.id";
            return false;
        }
        entry.path = std::move(path);
        entry.zones.push_back(Zone{.zone_index = 0, .base_offset = 0, .capacity = capacity});
        config.disks.push_back(std::move(entry));
    }
    return true;
}

} // namespace

bool DioConfig::load_file(const std::string &path, DioConfig &out, std::string &err)
{
    toml::table root;
    try {
        root = toml::parse_file(path);
    }
    catch (const toml::parse_error &e) {
        err = "failed to parse config file " + path + ": " + std::string(e.description());
        return false;
    }

    DioConfig candidate;
    if (!read_server(root, candidate, err)) {
        return false;
    }

    const toml::table *engine = nullptr;
    if (!read_section(root, "engine", engine, err)) {
        return false;
    }
    if (engine != nullptr && (!read_u32(*engine, "thread_pool_size", candidate.thread_pool_size, err) ||
                              !read_u32(*engine, "sq_entries", candidate.sq_entries, err))) {
        return false;
    }
    if (!read_group0(root, candidate, err)) {
        return false;
    }

    const toml::table *metrics = nullptr;
    if (!read_section(root, "metrics", metrics, err)) {
        return false;
    }
    if (metrics != nullptr && (!read_value(*metrics, "log_dir", candidate.metrics_log_dir, err) ||
                               !read_u32(*metrics, "interval_secs", candidate.metrics_interval_secs, err))) {
        return false;
    }
    if (!read_disks(root, candidate, err) || !candidate.validate(err)) {
        return false;
    }
    out = std::move(candidate);
    return true;
}

} // namespace crowdb::diskio
