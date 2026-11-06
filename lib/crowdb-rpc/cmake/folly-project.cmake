# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

# Use the same pixi Boost lookup as the installed Folly consumer. Folly's
# project resets this policy before discovering its own dependencies.
cmake_policy(SET CMP0167 OLD)
