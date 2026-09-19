// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Shared test utility under the workspace runtime namespace.
#pragma once

#include "crowdb-common/runtime_path.h"

#include <unistd.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

namespace crowdb::tree_test
{

inline std::string test_tmp_root()
{
    static const char *env = std::getenv("CROWDB_TREE_TEST_TMP");
    if ((env != nullptr) && env[0] != '\0') {
        return env;
    }
    return crowdb::common::test_runtime_path("crowdb-tree").string();
}

// RAII temp directory. Creates a unique subdirectory under test_tmp_root()
// on construction, recursively removes it on destruction.
struct TempDir
{
    std::string path;

    TempDir(const char *prefix = "ct_")
    {
        std::string root = test_tmp_root();
        std::filesystem::create_directories(root);
        std::array<char, 128> tmpl{};
        std::snprintf(tmpl.data(), tmpl.size(), "%s/%sXXXXXX", root.c_str(), prefix);
        // mkdtemp modifies in place; copy into a mutable buffer
        std::vector<char> buf(tmpl.begin(), tmpl.end());
        buf.push_back('\0');
        char *d = mkdtemp(buf.data());
        if (d != nullptr) {
            path = d;
        }
    }

    ~TempDir()
    {
        if (!path.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    }

    TempDir(const TempDir &)            = delete;
    TempDir &operator=(const TempDir &) = delete;
};

// RAII temp file (for tests that need a single file path, not a directory).
struct TempFile
{
    std::string path;

    TempFile(const char *prefix = "ct_")
    {
        std::string root = test_tmp_root();
        std::filesystem::create_directories(root);
        std::array<char, 128> tmpl{};
        std::snprintf(tmpl.data(), tmpl.size(), "%s/%sXXXXXX", root.c_str(), prefix);
        std::vector<char> buf(tmpl.begin(), tmpl.end());
        buf.push_back('\0');
        int fd = mkstemp(buf.data());
        if (fd >= 0) {
            close(fd);
            path = buf.data();
        }
    }

    ~TempFile()
    {
        if (!path.empty()) {
            std::remove(path.c_str());
        }
    }

    TempFile(const TempFile &)            = delete;
    TempFile &operator=(const TempFile &) = delete;
};

} // namespace crowdb::tree_test
