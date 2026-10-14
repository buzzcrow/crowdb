// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { CapacityObservationStatus } from '../data/capacityObservation';

export function CapacityObservations({ observations = [] }: { observations?: CapacityObservationStatus[] }) {
  return <details className="tw-shrink-0 tw-border-b tw-border-border tw-px-4 tw-py-1 tw-text-xs tw-text-muted">
    <summary aria-label="Capacity observation freshness">
      {observations.some(row => row.error) ? 'Capacity observations stale or unavailable' : 'Capacity observation times'}
    </summary>
    <ul className="tw-py-1" aria-label="Capacity observations">
      {observations.map(row => <li key={row.source}>
        {row.source}: {row.observedAt === null ? 'Unknown — not observed' : <>
          {row.error ? 'Stale' : row.cached ? 'Cached' : 'Observed'} at <time dateTime={new Date(row.observedAt).toISOString()}>{new Date(row.observedAt).toLocaleString()}</time>
        </>}{row.error && ` — ${row.error}`}
      </li>)}
    </ul>
  </details>;
}
