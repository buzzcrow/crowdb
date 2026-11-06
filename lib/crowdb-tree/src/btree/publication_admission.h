// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "published_state.h"

namespace crowdb::tree
{
[[nodiscard]] Status validate_flush_admission(const PublishedTreeState     &current,
                                              const PublicationTransaction &transaction);
} // namespace crowdb::tree
