// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#ifdef CROWDB_TREE_TEST_UTIL
namespace crowdb::tree
{
// The calling thread observes either acquiring or waiting for the prepare gate.
using FlushPrepareHook = void (*)(void *context, bool acquired) noexcept;
void set_flush_prepare_hook_for_tests(FlushPrepareHook hook, void *context) noexcept;
} // namespace crowdb::tree
#endif
