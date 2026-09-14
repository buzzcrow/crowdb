// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/c_api.h"

int main()
{
    ct_page_store *store = nullptr;
    if (ct_page_store_open_mem(1, &store) != 0) {
        return 1;
    }
    ct_options options{};
    options.page_store     = store;
    ct_tree        *tree   = nullptr;
    const ct_status status = ct_open(&options, &tree);
    ct_page_store_free(store);
    if (status != 0) {
        return 1;
    }
    ct_close(tree);
    return 0;
}
