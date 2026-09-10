// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "stdexec_adapter.h"

namespace crowdb::tree::detail
{

// Keeps the private adapter in its own archive member. Production chunk code
// references this symbol; ordinary local tree users do not extract the member.
bool stdexec_adapter_available()
{
    return true;
}

} // namespace crowdb::tree::detail
