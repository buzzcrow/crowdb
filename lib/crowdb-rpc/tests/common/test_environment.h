// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdlib>
#include <optional>
#include <string>

struct TestEnvironment
{
    std::string                name;
    std::optional<std::string> previous;

    TestEnvironment(const char *key, const char *value) : name(key)
    {
        if (const char *old = std::getenv(key); old != nullptr) {
            previous = old;
        }
        if (value == nullptr) {
            ::unsetenv(key);
        }
        else {
            ::setenv(key, value, 1);
        }
    }

    ~TestEnvironment()
    {
        if (previous) {
            ::setenv(name.c_str(), previous->c_str(), 1);
        }
        else {
            ::unsetenv(name.c_str());
        }
    }
};
