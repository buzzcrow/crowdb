// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { lazy, Suspense, type ComponentProps } from 'react';
const CapacityPanel = lazy(() => import('../panels/CapacityPanel').then(m => ({ default: m.CapacityPanel })));

export function CapacityView(props: ComponentProps<typeof CapacityPanel>) {
  return <Suspense fallback={<p className="tw-p-4 tw-text-muted">Loading capacity…</p>}><CapacityPanel {...props} /></Suspense>;
}
