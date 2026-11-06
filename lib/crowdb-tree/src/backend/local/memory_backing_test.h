// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#ifdef CROWDB_TREE_TEST_UTIL
#    include <cstddef>
#    include <optional>

namespace crowdb::tree
{
// Synchronous backend allocation fault, scoped to the calling thread.
void set_memory_backing_failure_after_for_tests(std::optional<size_t> pages) noexcept;
} // namespace crowdb::tree
#endif
