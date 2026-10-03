// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useRef, useState } from 'react';
import { getApiBase } from '../api';
import { check } from '../access/native';

export interface StorageExtent {
  index: string; chunk_id: string | null; offset: string; length: string;
  logical_offset: string; logical_length: string;
}
interface LocationPage {
  bucket: string; key: string; generation: string; etag: string; logical_length: string;
  locations: StorageExtent[]; next_cursor: string | null;
}
export interface LocationQuery {
  bucket: string; key: string; cursor?: string; previous: Array<string | undefined>;
  generation?: string; selected?: string;
}
const exactInteger = (value: unknown): value is string => typeof value === 'string' && /^\d{1,20}$/.test(value) && BigInt(value) <= 18446744073709551615n;
const cacheKey = (query: LocationQuery) => JSON.stringify([query.bucket, query.key, query.cursor ?? '', query.generation ?? '']);

async function readPage(response: Response): Promise<LocationPage> {
  await check(response);
  const reader = response.body?.getReader();
  if (!reader) throw new Error('Object inspection returned no response');
  const decoder = new TextDecoder(); let length = 0; let text = '';
  try {
    for (;;) {
      const part = await reader.read(); if (part.done) break;
      length += part.value.byteLength;
      if (length > 1024 * 1024) throw new Error('Storage locations exceed the 1 MiB inspection limit');
      text += decoder.decode(part.value, { stream: true });
    }
    const page = JSON.parse(text + decoder.decode()) as LocationPage;
    if (!Array.isArray(page.locations) || page.locations.length > 20 || typeof page.generation !== 'string' || !/^[a-f0-9]{64}$/i.test(page.generation) || !exactInteger(page.logical_length) || typeof page.etag !== 'string' || typeof page.bucket !== 'string' || typeof page.key !== 'string' || (page.next_cursor !== null && (typeof page.next_cursor !== 'string' || page.next_cursor.length > 9216))) {
      throw new Error('Invalid or oversized storage location page');
    }
    for (const extent of page.locations) {
      for (const field of ['index', 'offset', 'length', 'logical_offset', 'logical_length'] as const) {
        if (!exactInteger(extent[field])) throw new Error('Storage extent has an invalid exact integer');
      }
      if (extent.chunk_id !== null && !/^[a-f0-9]{32}$/i.test(extent.chunk_id)) throw new Error('Invalid Chunk identity');
    }
    return page;
  } finally { await reader.cancel(); }
}

export function useObjectLocations(active: boolean, bucket: string, key?: string) {
  const [page, setPage] = useState<LocationPage | null>(null);
  const [query, setQuery] = useState<LocationQuery>({ bucket: '', key: '', previous: [] });
  const queryRef = useRef(query);
  const updateQuery = (state: LocationQuery) => { queryRef.current = state; setQuery(state); };
  const [selected, setSelected] = useState<StorageExtent | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [stale, setStale] = useState(false);
  const controller = useRef<AbortController | null>(null);
  const revision = useRef(0);
  const cache = useRef(new Map<string, LocationPage>());
  const queued = useRef<LocationQuery | null>(null);
  const [restoreRevision, setRestoreRevision] = useState(0);
  const load = async (target: LocationQuery) => {
    controller.current?.abort(); const request = new AbortController(); controller.current = request;
    const version = ++revision.current;
    setBusy(true); setError(''); setStale(false);
    const cached = cache.current.get(cacheKey(target));
    if (cached) { setPage(cached); setSelected(cached.locations.find(row => row.index === target.selected) ?? null); }
    try {
      const params = new URLSearchParams({ bucket: target.bucket, key: target.key, limit: '20' });
      if (target.cursor) params.set('cursor', target.cursor);
      const result = await readPage(await fetch(`${getApiBase()}/access/s3-inspect/locations?${params}`, { signal: request.signal }));
      if (request.signal.aborted || version !== revision.current) return;
      if (result.bucket !== target.bucket || result.key !== target.key) throw new Error('Inspection returned a different object');
      if (target.generation && result.generation !== target.generation) throw new Error('HTTP 409: Object generation changed; refresh storage locations');
      const state = { ...target, generation: result.generation };
      updateQuery(state); setPage(result); setSelected(result.locations.find(row => row.index === target.selected) ?? null);
      cache.current.delete(cacheKey(state)); cache.current.set(cacheKey(state), result);
      while (cache.current.size > 4) cache.current.delete(cache.current.keys().next().value!);
    } catch (error) {
      if (!request.signal.aborted && version === revision.current) {
        setError(String(error)); setStale(String(error).includes('HTTP 409'));
      }
    } finally { if (version === revision.current) setBusy(false); }
  };
  useEffect(() => {
    if (!active || !bucket || !key) return;
    const pending = queued.current;
    if (pending?.bucket === bucket && pending.key === key) {
      queued.current = null; updateQuery(pending); void load(pending);
    } else if (query.bucket !== bucket || query.key !== key) {
      setPage(null); setSelected(null); updateQuery({ bucket, key, previous: [] });
      void load({ bucket, key, previous: [] });
    } else if (!page) {
      void load(queryRef.current);
    }
    return () => { controller.current?.abort(); ++revision.current; setBusy(false); };
  }, [active, bucket, key, restoreRevision]);
  return {
    page, selected, busy, error, stale,
    select: (extent: StorageExtent) => { setSelected(extent); updateQuery({ ...queryRef.current, selected: extent.index }); },
    refresh: () => { if (bucket && key) void load({ bucket, key, previous: [] }); },
    next: () => { if (page?.next_cursor) void load({ ...query, cursor: page.next_cursor, selected: undefined, previous: [...query.previous, query.cursor].slice(-32) }); },
    previous: () => { if (query.previous.length) void load({ ...query, cursor: query.previous.at(-1), selected: undefined, previous: query.previous.slice(0, -1) }); },
    canPrevious: query.previous.length > 0,
    snapshot: (): LocationQuery | null => bucket && key && queryRef.current.bucket === bucket && queryRef.current.key === key ? { ...queryRef.current, previous: [...queryRef.current.previous] } : null,
    restore: (state: LocationQuery | null) => { if (state) { queued.current = state; setRestoreRevision(value => value + 1); } },
  };
}
