// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_core_internal.h"
#include "tree_generation_lease.h"

namespace crowdb::tree
{
Status TreeCore::install_generation(std::shared_ptr<const PublishedTreeState> candidate, const SplitGeneration *split)
{
    std::shared_ptr<TreeGenerationLease> prepared;
    auto status = TreeGenerationLease::prepare(*this, std::move(candidate), split, &prepared);
    if (!status.ok()) {
        return status;
    }
    status = prepared->acquire();
    if (!status.ok()) {
        return status;
    }
    return prepared->commit();
}

#ifdef CROWDB_TREE_TEST_UTIL
void TreeCore::set_before_generation_install_for_tests(std::function<void()> callback)
{
    state_->before_generation_install_for_tests = std::move(callback);
}
#endif

Status TreeCore::clear()
{
    return install_generation({});
}
} // namespace crowdb::tree
