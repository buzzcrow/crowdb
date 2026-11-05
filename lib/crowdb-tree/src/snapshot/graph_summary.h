// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/readable_page_catalog.h"

namespace crowdb::tree
{
// The caller visits each reachable PID exactly once. Recovery supplies its
// validated leaf counts; writeback uses the encoder's receipt. No I/O occurs.
[[nodiscard]] bool add_graph_summary(TreeSummary &summary, const ReadablePageFrame &frame,
                                     const FinalPageSummary *verified_leaf = nullptr) noexcept;
} // namespace crowdb::tree
