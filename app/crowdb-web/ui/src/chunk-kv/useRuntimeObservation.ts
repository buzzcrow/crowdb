// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { getApiBase, getManagementToken } from '../api';
import { readJson } from '../access/native';
import type { Partition } from './catalog';

export interface JournalObservation {
  generation: string; writer_epoch: string; metadata_group_id: string;
  trim_offset: string; sealed_tail: string; closed: boolean;
  active: { chunk_id: string; physical_start: string; logical_start: string; acknowledged_cursor: string; capacity: string } | null;
  extent_pages: { page_index: string; first_logical: string; end_logical: string }[];
  offset: number; next_offset: number | null;
}
export interface TreeObservation {
  error?: string; checkpoint_manifest: string; checkpoint_applied_seq: string;
  runtime: Record<string, string | boolean> | null; maintenance: Record<string, string>;
}
export interface Observation {
  lifecycle: string; admitting: boolean; live_grant: boolean;
  journal_durable_seq: string; journal_durable_offset: string; applied_seq: string;
  stream_id: string; observed_at_monotonic_ms: string;
  tree?: TreeObservation; journal?: JournalObservation | null;
}

export function useRuntimeObservation({ active, partition, generation, catalogPage, catalogOffset }: {
  active: boolean; partition: Partition; generation: string; catalogPage: number; catalogOffset: number;
}) {
  const [value, setValue] = useState<Observation | null>(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [revision, setRevision] = useState(0);
  const [cursor, setCursor] = useState<{ generation?: string; offset: number }>({ offset: 0 });
  useEffect(() => {
    if (!active) return;
    const abort = new AbortController();
    setValue(null); setError(''); setBusy(true);
    const token = getManagementToken();
    const query = new URLSearchParams({ id: partition.id, epoch: partition.epoch, generation, page: String(catalogPage), offset: String(catalogOffset), stream_offset: String(cursor.offset) });
    if (cursor.generation) query.set('stream_generation', cursor.generation);
    fetch(`${getApiBase()}/chunk-kv/runtime?${query}`, { signal: abort.signal, headers: token ? { Authorization: `Bearer ${token}` } : {} })
      .then(response => readJson<Observation>(response))
      .then(value => {
        if ((value.journal?.extent_pages.length ?? 0) > 100) throw new Error('Extent index exceeds 100 entries');
        if (cursor.generation && (value.journal?.generation !== cursor.generation || value.journal.offset !== cursor.offset)) throw new Error('Stream manifest changed; refresh runtime');
        if (!abort.signal.aborted) setValue(value);
      })
      .catch(error => { if (!abort.signal.aborted) setError(String(error)); })
      .finally(() => { if (!abort.signal.aborted) setBusy(false); });
    return () => abort.abort();
  }, [active, partition.id, partition.epoch, generation, catalogPage, catalogOffset, cursor, revision]);
  return { value, error, busy,
    refresh: () => { setCursor({ offset: 0 }); setRevision(value => value + 1); },
    pageStream: (offset: number) => { if (value?.journal) setCursor({ generation: value.journal.generation, offset }); },
  };
}
