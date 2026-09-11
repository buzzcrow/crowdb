// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

// Umbrella C++ interface. Subsystem headers remain available when a caller
// needs a narrower dependency surface.
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/btree/key_range.h"
#include "crowdb-tree/btree/range_rebuild.h"
#include "crowdb-tree/btree/tree.h"
