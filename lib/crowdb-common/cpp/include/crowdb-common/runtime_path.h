// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <string>

namespace crowdb::common
{

inline std::filesystem::path runtime_root()
{
    if (const char *root = std::getenv("CROWDB_RUNTIME_ROOT"); root != nullptr && root[0] != '\0') {
        return root;
    }
    if (const char *root = std::getenv("PIXI_PROJECT_ROOT"); root != nullptr && root[0] != '\0') {
        return std::filesystem::path(root) / ".crowdb-runtime";
    }
    std::error_code ec;
    auto            dir = std::filesystem::current_path(ec);
    for (int i = 0; i < 20 && !ec; ++i) {
        if (std::filesystem::exists(dir / "pixi.toml", ec)) {
            return dir / ".crowdb-runtime";
        }
        const auto parent = dir.parent_path();
        if (parent == dir) {
            break;
        }
        dir = parent;
    }
    return std::filesystem::current_path() / ".crowdb-runtime";
}

inline std::filesystem::path test_runtime_path(const std::string &component)
{
    auto path = runtime_root() / "ephemeral" / "cpp" / (component + "-" + std::to_string(::getpid()));
    std::filesystem::create_directories(path);
    return path;
}

} // namespace crowdb::common
