// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useRef, useState } from 'react';
import { useDomain } from '../contexts/DomainContext';
import { iceberg } from '../access/native';
import type { Inspection, Selection, TableLoad, ParquetQuery } from './types';
export function useInspection(loaded: TableLoad | null, tablePath: string, token: string, origin: string | null) {
  const { checkpoint } = useDomain();
  const [selection, setSelection] = useState<Selection | null>(null);
  const [data, setData] = useState<Inspection | null>(null);
  type Page = Inspection & { previous?: string; trail: string[]; pageToken: string };
  const [cache, setCache] = useState<Record<string, Page>>({});
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [detail, setDetail] = useState<ParquetQuery>({ view: 'layout', columnPage: 0 });
  const controller = useRef<AbortController | null>(null);
  const scope = useRef(0);
  useEffect(() => { scope.current++; controller.current?.abort(); setSelection(null); setData(null); setCache({}); setError(''); setBusy(false); return () => { scope.current++; controller.current?.abort(); }; }, [loaded, token, origin, tablePath]);
  const key = (s: Selection) => JSON.stringify([String(s.snapshot['snapshot-id']), s.manifest?.location ?? '', s.file?.location ?? '']);
  async function select(next: Selection, offset?: string, preserveSelection = false, record = true) {
    if (record && !preserveSelection && (!selection || key(selection) !== key(next) || selection.kind !== next.kind
      || offset != null && offset !== (cache[key(next)]?.pageToken ?? '0'))) checkpoint();
    controller.current?.abort();
    const active = new AbortController(); controller.current = active;
    const generation = scope.current;
    if (!preserveSelection) { setSelection(next); setData(null); }
    setError('');
    if (next.kind === 'snapshot' && !next.snapshot['manifest-list']) { setBusy(false); return; }
    const cached = cache[key(next)];
    if (!preserveSelection && selection?.file?.location !== next.file?.location) setDetail({ view: 'layout', columnPage: 0 });
    offset ??= cached?.pageToken;
    if (!loaded) return;
    setBusy(true);
    try {
      const query = new URLSearchParams({ metadata: loaded['metadata-location'], snapshot: String(next.snapshot['snapshot-id']) });
      if (next.manifest) query.set('manifest', next.manifest.location);
      if (next.file) query.set('file', next.file.location);
      if (offset) query.set('offset', offset);
      const result: Inspection = await iceberg(`${tablePath}/inspect?${query}`, token, 'GET', undefined, active.signal);
      if (active.signal.aborted || generation !== scope.current) return;
      if (result.metadata_location !== loaded['metadata-location'] || result.snapshot_id !== String(next.snapshot['snapshot-id'])) throw new Error('Inspection response does not match selected table generation and snapshot');
      const location = next.file?.location ?? next.manifest?.location ?? next.snapshot['manifest-list'];
      const kind = next.file ? ['parquet', 'unsupported'] : next.manifest ? ['manifest'] : ['manifest-list'];
      if (result.location !== location || !kind.includes(result.kind) || (result.rows?.length ?? 0) > 100 || (result.groups?.length ?? 0) > 20) throw new Error('Inspection response does not match selected reference or window');
      const pageToken = offset ?? '0';
      const back = cached?.trail.indexOf(pageToken) ?? -1;
      const trail = pageToken === '0' ? [] : back >= 0 ? cached.trail.slice(0, back)
        : cached ? [...cached.trail, cached.pageToken].slice(-32) : ['0'];
      const value: Page = { ...result, pageToken, trail, previous: trail.at(-1) };
      if (!preserveSelection || selection && key(selection) === key(next)) setData(value);
      setCache(previous => {
        const entries = Object.entries(previous).filter(([k]) => k !== key(next));
        const target = preserveSelection && selection ? selection : next;
        const parents = new Set([key({ snapshot: target.snapshot, kind: 'list' }),
          key({ snapshot: target.snapshot, kind: 'manifest', manifest: target.manifest })]);
        const retained = [...entries.filter(([k]) => !parents.has(k)), ...entries.filter(([k]) => parents.has(k))];
        // Responses are capped at 4 MiB; retain at most four branch pages.
        return Object.fromEntries([...retained.slice(-3), [key(next), value]]);
      });
      return true;
    } catch (e) { if (!active.signal.aborted && generation === scope.current) { setError(String(e)); setData(null); setCache({}); } }
    finally { if (!active.signal.aborted && generation === scope.current) setBusy(false); }
  }
  return { selection, data, cache, error, busy, select, key, detail, setDetail, pageToken: selection ? cache[key(selection)]?.pageToken : undefined, clear: () => { if (selection) checkpoint(); controller.current?.abort(); setSelection(null); setData(null); setBusy(false); } };
}
export type Inspector = ReturnType<typeof useInspection>;
