// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { buttonClass, inputClass } from '../access/Workbench';
import { useDomain } from '../contexts/DomainContext';
import { resourceMenuKey } from '../components/resourceMenuKey';
import * as api from './api';
import { bytes, sampleLabel } from './identity';
import { useQuery } from './query';
import type { DatasetSelection, DraftSplit } from './types';

type Scope = { dataset: string; snapshot: string; revision?: string; split?: string };
export interface MatrixWindow { rowCursor?: string; columnCursor?: string; exact: string; lookup: string; filter: string }
export const emptyWindow: MatrixWindow = { exact: '', lookup: '', filter: '' };
export function Matrix({ scope, active, refresh, select, window, updateWindow, onContextMenu, preview, selectedId }: { preview?: DraftSplit; selectedId?: string; scope: Scope; active: boolean; refresh: number; select: (value: DatasetSelection) => void; window: MatrixWindow; updateWindow: (value: MatrixWindow) => void; onContextMenu?: (event: import('react').MouseEvent, value: DatasetSelection) => void }) {
  const { rowCursor, columnCursor, exact, lookup, filter } = window;
  const setRowCursor = (value?: string) => updateWindow({ ...window, rowCursor: value });
  const setColumnCursor = (value?: string) => updateWindow({ ...window, columnCursor: value });
  const setExact = (value: string) => updateWindow({ ...window, exact: value });
  const setLookup = (value: string) => updateWindow({ ...window, lookup: value });
  const setFilter = (value: string) => updateWindow({ ...window, filter: value });
  const [error, setError] = useState('');
  const { checkpoint } = useDomain();
  const key = JSON.stringify([scope.dataset, scope.snapshot, scope.revision, scope.split, rowCursor, columnCursor, lookup, refresh, preview]);
  const data = useQuery(key, active, async signal => {
    const [schema, samples] = await Promise.all([
      api.schemaPage(scope.dataset, scope.snapshot, columnCursor, signal),
      preview ? api.splitPreview(scope.dataset, scope.snapshot, preview, rowCursor, signal) : api.samplePage(scope.dataset, scope.snapshot, rowCursor, scope.revision, scope.split, signal),
    ]);
    let ids = samples.entries;
    if (lookup && !preview) {
      const membership = await api.control<{ contains: boolean }>(`${api.snapshotPath(scope.dataset, scope.snapshot)}/contains?${new URLSearchParams({ sample: lookup })}`, 'GET', undefined, signal);
      if (!membership.contains) throw new Error('Sample is absent from this Snapshot.');
      // Split membership must not be inferred from Snapshot membership.
      if (scope.split !== undefined) throw new Error('Exact lookup is available in All Samples. Split pages preserve authoritative membership.');
      ids = [lookup];
    }
    const rows = await api.matrix(scope.dataset, scope.snapshot, ids, schema.entries.map(field => field.name), signal);
    return { schema, samples, rows };
  });
  const move = (action: () => void) => { checkpoint(); action(); };
  return <section aria-label="Samples and Schema" className="tw-space-y-3">
    <h2 className="tw-font-semibold">Samples {scope.split !== undefined && `· ${scope.split}`}</h2>
    {!preview && <form className="tw-flex tw-gap-2" onSubmit={event => { event.preventDefault(); try { bytes(exact); setError(''); move(() => setLookup(exact)); } catch (reason) { setError(String(reason)); } }}>
      <input className={inputClass} aria-label="Exact Sample ID (base64)" value={exact} onChange={event => setExact(event.target.value)} placeholder="Canonical base64 ID" />
      <button className={buttonClass} disabled={scope.split !== undefined}>Find Sample</button>
      <button type="button" className={buttonClass} onClick={() => move(() => setLookup(''))}>All Samples</button>
    </form>}
    <label className="tw-text-xs">Filter loaded rows <input className={inputClass} value={filter} onChange={event => setFilter(event.target.value)} /></label>
    {(error || data.error) && <p role="alert">{error || data.error} Refresh this window to reset its continuation.</p>}
    {!data.value && !data.error && <p role="status">Loading metadata…</p>}
    {data.value && <div className="tw-overflow-auto"><table className="tw-w-full tw-text-xs" aria-label="Sample field matrix">
      <thead className="tw-sticky tw-top-0 tw-bg-panel"><tr><th className="tw-sticky tw-left-0 tw-bg-panel tw-p-2">Sample ID</th>
        {data.value.schema.entries.map(field => <th key={field.name} className="tw-p-2" onKeyDown={resourceMenuKey} onContextMenu={event => onContextMenu?.(event, { ...scope, kind: 'schema', field: field.name, required: field.required })}><button onClick={() => select({ ...scope, kind: 'schema', field: field.name, required: field.required })}>{field.name}<small className="tw-block tw-text-muted">{field.required ? 'Required' : 'Optional'}</small></button></th>)}
      </tr></thead><tbody>
        {data.value.rows.filter(row => !filter || sampleLabel(row.id).includes(filter) || row.id.includes(filter)).map(row => <tr key={row.id} aria-selected={selectedId === row.id} className={`tw-border-t tw-border-border ${selectedId === row.id ? 'tw-bg-accent/10' : ''}`} onClick={event => { if (!(event.target as HTMLElement).closest('button')) select({ ...scope, kind: 'sample', sample: row.id }); }}>
          <th className="tw-sticky tw-left-0 tw-bg-panel tw-p-2 tw-text-left" onKeyDown={resourceMenuKey} onContextMenu={event => onContextMenu?.(event, { ...scope, kind: 'sample', sample: row.id })}><button title={row.id} onClick={() => select({ ...scope, kind: 'sample', sample: row.id })}>{sampleLabel(row.id)}</button></th>
          {row.fields.map((field, index) => <td key={data.value!.schema.entries[index].name} className="tw-p-2" onKeyDown={resourceMenuKey} onContextMenu={event => onContextMenu?.(event, { ...scope, kind: 'cell', sample: row.id, field: data.value!.schema.entries[index].name })}><button onClick={() => select({ ...scope, kind: 'cell', sample: row.id, field: data.value!.schema.entries[index].name })}>
            {field ? `${field.storage} · ${field.length} B` : 'Missing'}{field && field.source_snapshot !== scope.snapshot && <small className="tw-block">Inherited</small>}
          </button></td>)}
        </tr>)}
      </tbody></table>
      {data.value.rows.length === 0 && <p>{data.value.samples.after && preview ? 'No matches in this scan window. Next Samples continues the Snapshot scan.' : 'No Samples in this window.'}</p>}
      {data.value.schema.entries.length === 0 && <p>Schema has no fields.</p>}
    </div>}
    <div className="tw-flex tw-gap-2">
      <button className={buttonClass} disabled={!data.value?.samples.after || !!lookup} onClick={() => move(() => setRowCursor(data.value!.samples.after!))}>Next Samples</button>
      <button className={buttonClass} disabled={!data.value?.schema.after} onClick={() => move(() => setColumnCursor(data.value!.schema.after!))}>Next fields</button>
      <button className={buttonClass} onClick={() => move(() => { updateWindow({ ...window, rowCursor: undefined, columnCursor: undefined, lookup: '' }); })}>First window</button>
    </div>
  </section>;
}
