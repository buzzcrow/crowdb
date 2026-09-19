// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Shared gtest main for all C++ test binaries. Replaces gtest_main so
// logging is initialized before any test runs: info/debug logs go to
// files under the workspace runtime namespace, while error-level logs are
// mirrored to stderr for CI visibility. shutdown_logging is called
// after all tests complete.

#include "crowdb-common/log.h"
#include "crowdb-common/runtime_path.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <string>

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);

    // Initialize logging below the workspace runtime namespace.
    const auto *binary = std::getenv("CROWDB_TEST_BINARY_NAME");
    std::string name   = binary != nullptr ? binary : "crowdb-test";
    // Use the argv[0] basename if CROWDB_TEST_BINARY_NAME is not set.
    if (binary == nullptr && argc > 0) {
        std::string p   = argv[0];
        auto        pos = p.find_last_of("/\\");
        name            = pos == std::string::npos ? p : p.substr(pos + 1);
    }

    auto            log_dir = crowdb::common::test_runtime_path("logs") / name;
    std::error_code ec;
    std::filesystem::create_directories(log_dir, ec);
    crowdb::common::init_logging(log_dir.string(), "info", 30, 5, name);
    crowdb::common::add_log_stderr("error");

    int result = RUN_ALL_TESTS();

    crowdb::common::flush_logging();
    crowdb::common::shutdown_logging();
    return result;
}
