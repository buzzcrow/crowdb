// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

export function ManagementSession() {
  return <div className="tw-px-4 tw-py-2 tw-bg-panel tw-border-b tw-border-border tw-flex tw-items-center tw-gap-3 tw-text-xs" data-testid="managed-preview">
    <span data-testid="managed-source">Source: Group 0</span>
    <span>Root administrator</span>
    <span data-testid="managed-topology-scope">Container topology: simplified view; service health is listed separately by Monitor</span>
    <span data-testid="managed-readonly">Hardware topology and disk management are read-only</span>
  </div>;
}
