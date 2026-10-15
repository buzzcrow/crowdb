// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { buttonClass } from '../access/Workbench';
import * as api from './api';
import { useQuery } from './query';
import type { DatasetSelection, SnapshotSummary } from './types';

/** Follow only this selected parent chain, never enumerate siblings or descendants. */
export function Ancestry({ dataset, snapshot, active, select }: {
  dataset: string; snapshot: string; active: boolean; select: (value: DatasetSelection) => void;
}) {
  const [start, setStart] = useState(snapshot);
  const result = useQuery(`${dataset}:${start}`, active, async signal => {
    const entries: SnapshotSummary[] = []; const seen = new Set<string>(); let next: string | null = start;
    while (next && entries.length < 8) {
      if (seen.has(next)) throw new Error('Snapshot ancestry contains a cycle.');
      seen.add(next);
      const value = await api.summary(dataset, next, signal);
      entries.push(value); next = value.parent;
    }
    return { entries: entries.reverse(), next };
  });
  return <section aria-label="Selected Snapshot ancestry" className="tw-border-t tw-border-border tw-pt-3 tw-space-y-2">
    <h2 className="tw-text-xs tw-font-semibold">Selected ancestry · up to 8 levels</h2>
    {result.error && <p role="alert">{result.error}</p>}
    {result.value?.next && <button className={buttonClass} onClick={() => setStart(result.value!.next!)}>Earlier ancestors</button>}
    <ol>{result.value?.entries.map((value, index) => <li key={value.id} style={{ paddingLeft: index * 8 }}>
      <button title={value.id} aria-current={value.id === snapshot ? 'page' : undefined} onClick={() => select({ kind: 'snapshot', dataset, snapshot: value.id })}>{value.id.slice(0, 10)}{value.parent === null && ' · Root'}</button>
    </li>)}</ol>
    {start !== snapshot && <button className={buttonClass} onClick={() => setStart(snapshot)}>Return to selected ancestry</button>}
  </section>;
}
