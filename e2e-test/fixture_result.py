# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Distinguish an undispatched client from a failed fixture operation."""


class CaseNotExecuted(RuntimeError):
    """An entered fixture whose client was blocked by another startup."""

    def __init__(self, blocked_by):
        super().__init__(f'browser client not executed: batch startup failed in {blocked_by}')
