// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// B+tree key-range implementation.

#include "crowdb-tree/btree/key_range.h"

namespace crowdb::tree
{

Status KeyRange::validate() const
{
    if (bounded_ && start_.has_value() && end_.has_value() && Slice(*start_).compare(Slice(*end_)) > 0) {
        return Status::invalid_argument("key range start is greater than end");
    }
    return Status::Ok();
}

bool KeyRange::before(Slice key) const
{
    return bounded_ && start_.has_value() && key.compare(Slice(*start_)) < 0;
}

bool KeyRange::at_or_after_end(Slice key) const
{
    return bounded_ && end_.has_value() && key.compare(Slice(*end_)) >= 0;
}

bool KeyRange::contains(Slice key) const
{
    return !before(key) && !at_or_after_end(key);
}

} // namespace crowdb::tree
