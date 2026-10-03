// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { getApiBase, getManagementToken } from '../api';
import { readJson } from '../access/native';
import { buttonClass } from '../access/Workbench';
import type { Partition } from './catalog';

interface Observation {
  lifecycle: string; admitting: boolean; live_grant: boolean;
  journal_durable_seq: string; journal_durable_offset: string; applied_seq: string;
  stream_id: string; observed_at_monotonic_ms: string;
}

export function RuntimeObservation({ active, partition, generation, catalogPage, catalogOffset }: {
  active: boolean; partition: Partition; generation: string; catalogPage: number; catalogOffset: number;
}) {
  const [value, setValue] = useState<Observation | null>(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    if (!active) return;
    const abort = new AbortController();
    setValue(null); setError(''); setBusy(true);
    const token = getManagementToken();
    const query = new URLSearchParams({ id: partition.id, epoch: partition.epoch, generation, page: String(catalogPage), offset: String(catalogOffset) });
    fetch(`${getApiBase()}/chunk-kv/runtime?${query}`, { signal: abort.signal, headers: token ? { Authorization: `Bearer ${token}` } : {} })
      .then(response => readJson<Observation>(response))
      .then(value => { if (!abort.signal.aborted) setValue(value); })
      .catch(error => { if (!abort.signal.aborted) setError(String(error)); })
      .finally(() => { if (!abort.signal.aborted) setBusy(false); });
    return () => abort.abort();
  }, [active, partition.id, partition.epoch, generation, catalogPage, catalogOffset, refresh]);
  return <section aria-label="Partition runtime" className="tw-rounded tw-border tw-border-border tw-p-3 tw-space-y-3">
    <div className="tw-flex tw-items-center tw-justify-between"><h3 className="tw-font-semibold">Owner runtime observation</h3>
      <button className={buttonClass} disabled={busy || !active} onClick={() => setRefresh(value => value + 1)}>Refresh runtime</button></div>
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
