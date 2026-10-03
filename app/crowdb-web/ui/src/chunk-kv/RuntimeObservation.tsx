// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { buttonClass } from '../access/Workbench';
import type { Observation } from './useRuntimeObservation';

export function RuntimeObservation({ active, value, busy, error, refresh }: {
  active: boolean; value: Observation | null; busy: boolean; error: string; refresh: () => void;
}) {
  return <section aria-label="Partition runtime" className="tw-rounded tw-border tw-border-border tw-p-3 tw-space-y-3">
    <div className="tw-flex tw-items-center tw-justify-between"><h3 className="tw-font-semibold">Owner runtime observation</h3>
      <button className={buttonClass} disabled={busy || !active} onClick={refresh}>Refresh runtime</button></div>
    {busy && <p role="status">Reading partition state…</p>}
    {error && <p role="status" className="tw-text-degraded">Runtime unavailable: {error}</p>}
    {value && <>
      <dl className="tw-grid tw-grid-cols-2 tw-gap-3 tw-text-sm">
        {Object.entries({ 'Writer lifecycle': value.lifecycle, 'Server admission': value.admitting ? 'Open' : 'Draining',
          'Live serving grant': value.live_grant ? 'Present at observation' : 'Absent at observation',
          'Journal durable sequence': value.journal_durable_seq, 'Applied sequence': value.applied_seq,
          'Journal durable offset (bytes)': value.journal_durable_offset, 'Journal stream': value.stream_id,
        }).map(([label, value]) => <div key={label}><dt className="tw-text-muted">{label}</dt><dd className="tw-font-mono tw-break-all">{value}</dd></div>)}
      </dl>
      <p className="tw-text-xs tw-text-muted">Independently sampled counters · owner monotonic time {value.observed_at_monotonic_ms} ms. Serving lifecycle alone does not establish serving authority. No data pages read.</p>
    </>}
  </section>;
}
