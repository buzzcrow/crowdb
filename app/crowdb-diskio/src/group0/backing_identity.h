// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <folly/dynamic.h>

#include <string>

namespace crowdb::diskio
{

// Verify the opened descriptor and the current pathname against retained
// Group-0 identity. Observation never writes or interprets the medium's bytes.
bool matches_backing_identity(int fd, const std::string &path, const folly::dynamic &storage);

} // namespace crowdb::diskio
