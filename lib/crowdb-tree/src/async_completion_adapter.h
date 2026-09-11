// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/backend/async_page_store.h"

#include <memory>
#include <type_traits>
#include <utility>

namespace crowdb::tree::detail
{

// Transitional owner for existing tree continuations. The backend boundary
// receives only the fixed token; each completion destroys its callable before
// invoking it. New sender operation states pass their own address directly and
// do not use this adapter.
template <class Fn> AsyncCompletion own_async_completion(Fn &&fn)
{
    using StoredFn = std::decay_t<Fn>;

    struct State
    {
        StoredFn fn;

        static void complete(void *context, Status status)
        {
            auto state = std::unique_ptr<State>(static_cast<State *>(context));
            state->fn(std::move(status));
        }
    };

    auto *state = new State{std::forward<Fn>(fn)};
    return {.context = state, .complete_fn = &State::complete};
}

} // namespace crowdb::tree::detail
