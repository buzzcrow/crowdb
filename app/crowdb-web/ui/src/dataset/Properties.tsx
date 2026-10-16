// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { buttonClass, inputClass, JsonView } from '../access/Workbench';
import * as api from './api';
import { useQuery } from './query';
import type { DatasetSelection, FieldMetadata } from './types';

export function Properties({ selected, active }: { selected: DatasetSelection; active: boolean }) {
  const result = useQuery(JSON.stringify(selected), active, async signal => {
    if (selected.kind === 'root') return { namespace: 'dataset-ns' };
    if (selected.kind === 'dataset') return api.control(api.datasetPath(selected.dataset), 'GET', undefined, signal);
    if (selected.kind === 'cell') return (await api.matrix(selected.dataset, selected.snapshot, [selected.sample], [selected.field], signal))[0].fields[0];
    if (selected.kind === 'sample') return { sample_id_base64: selected.sample, snapshot: selected.snapshot, revision: selected.revision, split: selected.split };
    if (selected.kind === 'schema') return { field: selected.field, required: selected.required, definition: 'The selected column shows required/optional. Schema fields have no type or MIME declaration.' };
    if (selected.revision) return api.revision(selected.dataset, selected.snapshot, selected.revision, signal);
    const [summary, lifecycle] = await Promise.allSettled([api.summary(selected.dataset, selected.snapshot, signal), api.control(`${api.snapshotPath(selected.dataset, selected.snapshot)}/reclaim`, 'GET', undefined, signal)]);
    if (summary.status === 'rejected') throw summary.reason;
    return { ...summary.value, lifecycle: lifecycle.status === 'fulfilled' ? lifecycle.value : { unavailable: String(lifecycle.reason) } };
  });
  return <section aria-label="Dataset properties" className="tw-space-y-3">
    <h2 className="tw-font-semibold">Properties · {selected.kind}</h2>
    {selected.kind !== 'root' && <p className="tw-text-xs tw-break-all">{selected.dataset}</p>}
    {(selected.kind === 'sample' || selected.kind === 'cell') && <label className="tw-block tw-text-xs">Canonical Sample ID (select to copy)<input aria-label="Canonical Sample ID" className={`${inputClass} tw-w-full tw-font-mono`} readOnly value={selected.sample} onFocus={event => event.target.select()} /></label>}
    {result.error && <p role="alert">{result.error}</p>}
    {result.value !== undefined && <JsonView value={result.value} />}
    {selected.kind === 'cell' && result.value !== undefined && <FieldPreview key={JSON.stringify(selected)} selected={selected} field={result.value as FieldMetadata | null} active={active} />}
  </section>;
}
function FieldPreview({ selected, field, active }: { selected: Extract<DatasetSelection, { kind: 'cell' }>; field: FieldMetadata | null; active: boolean }) {
  const [requested, setRequested] = useState(false); const [hex, setHex] = useState(false);
  useEffect(() => { if (!active) setRequested(false); }, [active]);
  const result = useQuery(JSON.stringify(selected), active && requested, async signal => {
    const value = await api.preview(selected.dataset, selected.snapshot, selected.sample, selected.field, signal);
    if (value.state !== 'value' || value.truncated || value.data === null) throw new Error('No complete verified value is available.');
    const decoded = Uint8Array.from(atob(value.data), character => character.charCodeAt(0));
    if (decoded.length > 65536 || value.length === null || BigInt(decoded.length) !== api.decimal(value.length)) throw new Error('Preview length is invalid.');
    let text: string | null = null;
    try { text = new TextDecoder('utf-8', { fatal: true }).decode(decoded); } catch { /* Binary data uses hex. */ }
    return { text, hex: Array.from(decoded, byte => byte.toString(16).padStart(2, '0')).join(' ') };
  });
  const allowed = field && field.storage !== 'tombstone' && api.decimal(field.length) <= 65536n;
  return <div className="tw-space-y-2">
    <p className="tw-text-xs tw-text-muted">Checksum above is declared. Preview verifies the complete value; maximum 64 KiB.</p>
    <button className={buttonClass} disabled={!allowed || requested} onClick={() => setRequested(true)}>Preview field</button>
    {!allowed && <p>Preview unavailable: value is missing, tombstoned or exceeds 64 KiB.</p>}
    {requested && !result.value && !result.error && <button className={buttonClass} onClick={() => setRequested(false)}>Cancel preview</button>}
    {requested && result.error && <p role="alert">{result.error}</p>}
    {requested && result.value && <><button className={buttonClass} onClick={() => setHex(!hex)}>{hex ? 'Text' : 'Hex'}</button><pre aria-label="Verified field preview" className="tw-whitespace-pre-wrap tw-break-all tw-text-xs">{hex || result.value.text === null ? result.value.hex : result.value.text}</pre></>}
  </div>;
}
