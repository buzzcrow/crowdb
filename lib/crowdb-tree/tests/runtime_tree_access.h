// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/tree_core_access.h"
#ifdef CROWDB_TREE_TEST_UTIL
namespace crowdb::tree
{
struct RuntimeTreeAccess_for_tests
{
    static TreeCore &core(Crowdbtree &tree)
    {
        return *TreeCoreAccess::get(tree);
    }

    static auto capture(Crowdbtree &tree)
    {
        return core(tree).capture();
    }

    static auto root(Crowdbtree &tree)
    {
        auto view = capture(tree);
        return view->mapping.get(view->root_page_id);
    }

    static void reclaim(Crowdbtree &tree)
    {
        core(tree).publisher().reclaim();
        core(tree).binding()->drain();
    }
};
} // namespace crowdb::tree
#endif
