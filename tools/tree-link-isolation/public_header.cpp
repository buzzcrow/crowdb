// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/crowdb-tree.h"

int main()
{
    crowdb::tree::Config config;
    return config.frame_bytes == 0 ? 1 : 0;
}
