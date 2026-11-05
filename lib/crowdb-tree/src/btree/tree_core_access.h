// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/tree_core.h"

namespace crowdb::tree
{
struct TreeCoreAccess
{
    [[nodiscard]] static TreeCore *get(Crowdbtree &tree) noexcept
    {
        return tree.core_.get();
    }

    static void record_compaction(Crowdbtree &tree, const MergeGcStats &stats, uint64_t elapsed_ns)
    {
        tree.record_compaction_metrics(stats, elapsed_ns);
    }

    static Status create_volatile(const Config &config, std::unique_ptr<Crowdbtree> *out)
    {
        std::shared_ptr<TreeRuntimeBinding> binding;
        auto                      status = TreeRuntimeBinding::create(config.runtime, config.runtime_binding, &binding);
        std::shared_ptr<TreeCore> core;
        if (status.ok()) {
            status = TreeCore::create(config, binding, {}, &core);
        }
        if (status.ok()) {
            out->reset(new Crowdbtree(config, std::move(core)));
        }
        return status;
    }
};
} // namespace crowdb::tree
