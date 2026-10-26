// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "../../src/client/co_state.h"

namespace crowdb::rpc
{

inline CoTask test_co_wait(CoState *state)
{
    co_await CoAwait{state};
    ++state->total_ops;
    co_drop_response(state->resp_control, state->resp_data);
}

struct TestCoroutine
{
    std::atomic<uint32_t> finished{0};
    CoState               state;
    CoTask                task;

    TestCoroutine()
    {
        state.finished_count = &finished;
        state.co_state.store(CO_RUNNING);
        task = test_co_wait(&state);
    }

    ~TestCoroutine()
    {
        task.handle.destroy();
    }

    void complete()
    {
        co_on_complete(1, nullptr, nullptr, CROWDB_RPC_OK, &state);
    }
};

} // namespace crowdb::rpc
