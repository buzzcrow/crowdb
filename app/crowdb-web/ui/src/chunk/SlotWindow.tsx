// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { getApiBase, getManagementToken } from '../api';
import { readJson } from '../access/native';
import { buttonClass } from '../access/Workbench';

interface SlotPage {
  generation: string; assigned: boolean; owned_count: number;
  slots: number[]; next: number | null;
}

/** Lazy 32-slot windows; gaps remain explicit instead of becoming fake ranges. */
export function SlotWindow({ scope }: { scope: string }) {
  const [page, setPage] = useState<SlotPage>();
  const [after, setAfter] = useState<number>();
  const [history, setHistory] = useState<(number | undefined)[]>([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [revision, setRevision] = useState(0);
  const [generation, setGeneration] = useState<string>();
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true); setError('');
    const token = getManagementToken();
    const params = new URLSearchParams(scope);
    params.set('limit', '32');
    if (after != null) params.set('after', String(after));
    if (generation) params.set('generation', generation);
    void fetch(`${getApiBase()}/chunk-slots?${params}`, {
      signal: controller.signal, headers: token ? { Authorization: `Bearer ${token}` } : {},
    }).then(readJson).then(result => {
      if (!controller.signal.aborted) setPage(result as SlotPage);
    }).catch(cause => {
      if (!controller.signal.aborted) setError(String(cause));
    }).finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [scope, after, generation, revision]);
  return <section aria-label="Slot ownership" className="tw-pr-2 tw-py-2 tw-text-xs tw-space-y-2">
    {error && <p role="alert">{error} · Previous observation retained.</p>}
    {page && <>
      <p className="tw-text-muted">{page.assigned ? `${page.owned_count} ${scope.startsWith('layer=service') ? 'service' : 'storage'} slots` : 'No slot assignment'} · generation {page.generation}</p>
      <div className="tw-flex tw-flex-wrap tw-gap-1" aria-label="Owned slots">
        {page.slots.map(slot => <span key={slot} className="tw-rounded tw-border tw-border-border tw-bg-panel tw-px-1.5 tw-py-0.5">{slot}</span>)}
      </div>
      <nav aria-label="Slot pages" className="tw-flex tw-gap-1">
        <button className={buttonClass} disabled={loading || !history.length || !!error} onClick={() => { setGeneration(page.generation); setAfter(history.at(-1)); setHistory(previous => previous.slice(0, -1)); }}>Previous</button>
        <button className={buttonClass} disabled={loading || page.next == null || !!error} onClick={() => { setGeneration(page.generation); setHistory(previous => [...previous, after].slice(-32)); setAfter(page.next!); }}>Next</button>
      </nav>
    </>}
    {loading && <p role="status">Loading slot ownership…</p>}
    <button className={buttonClass} disabled={loading} onClick={() => { setAfter(undefined); setHistory([]); setGeneration(undefined); setRevision(value => value + 1); }}>Refresh slots</button>
  </section>;
}
