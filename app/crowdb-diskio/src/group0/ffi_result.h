// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

namespace crowdb::diskio
{

struct FfiResult
{
    std::atomic<int>  status{-1};
    std::string       json_result;
    std::atomic<bool> done{false};
};

inline bool wait_for_ctx(FfiResult &ctx, uint32_t timeout_ms = 10000)
{
    for (uint32_t i = 0; i < timeout_ms / 10; ++i) {
        if (ctx.done.load(std::memory_order_acquire)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

// The caller and callback each retain the result independently. A bounded wait
// can return without invalidating user_data while the Rust operation completes.
inline void *retain_ffi_result(const std::shared_ptr<FfiResult> &result)
{
    return new std::shared_ptr<FfiResult>(result);
}

inline void complete_ffi_result(int status, const char *result_json, void *user_data)
{
    std::unique_ptr<std::shared_ptr<FfiResult>> retained(static_cast<std::shared_ptr<FfiResult> *>(user_data));
    auto                                       &result = **retained;
    result.status.store(status, std::memory_order_relaxed);
    if (result_json != nullptr) {
        result.json_result = result_json;
    }
    result.done.store(true, std::memory_order_release);
}

} // namespace crowdb::diskio
