// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { ReactNode } from 'react';
import { buttonClass } from './Workbench';
export function ResourceActions({ label, children }: { label: string; children: ReactNode }) {
  return <details className="tw-rounded tw-border tw-border-border tw-bg-panel" aria-label={label}>
    <summary className={`${buttonClass} tw-cursor-pointer tw-border-0 tw-text-muted`}>{label}</summary>
    <div className="tw-p-3 tw-border-t tw-border-border tw-space-y-3">{children}</div>
  </details>;
}
