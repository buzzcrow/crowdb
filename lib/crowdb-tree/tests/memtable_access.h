// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/crowdb-tree.h"

#ifdef CROWDB_TREE_TEST_UTIL
namespace crowdb::tree
{
// Deterministic access to the production capture/publication boundaries.
struct MemTableAccess_for_tests
{
    static auto admit(Crowdbtree &tree)
    {
        return tree.admit_batch();
    }

    static auto capture(Crowdbtree &tree)
    {
        return tree.capture_flush_locked();
    }

    static auto sources(Crowdbtree &tree)
    {
        return tree.all_memtables();
    }

    static auto active(Crowdbtree &tree)
    {
        return tree.current_active();
    }

    static auto &counters(Crowdbtree &tree)
    {
        return tree.memtable_counters_;
    }

    static auto copy_latency(Crowdbtree &tree)
    {
        return tree.metrics_.mt_version_copy_l;
    }

    static auto &epoch(Crowdbtree &tree)
    {
        return tree.epoch_;
    }

    static bool replacing(Crowdbtree &tree)
    {
        return tree.generation_.closed_for_tests();
    }

    static void fail_publication(Crowdbtree &tree, void (*hook)())
    {
        tree.after_flush_group_for_tests_ = hook;
    }

    static void finish(Crowdbtree &tree, uint64_t slot)
    {
        tree.note_applied_slot(slot);
    }

    static void publish(Crowdbtree &tree, Crowdbtree::FlushBoundary &boundary)
    {
        tree.publish_flush_locked(boundary);
    }
};
} // namespace crowdb::tree
#endif
