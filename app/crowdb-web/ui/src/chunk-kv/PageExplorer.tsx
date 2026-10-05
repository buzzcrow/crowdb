// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { getApiBase, getManagementToken } from '../api';
import { readJson } from '../access/native';
import { buttonClass } from '../access/Workbench';
import { useDomain } from '../contexts/DomainContext';
import type { Partition } from './catalog';

export interface PageCursor { path: string; version?: string; fingerprint?: number; offset: number; selected?: number; text?: boolean }
interface Bytes { hex: string; bytes: number; text: string | null; truncated: boolean }
interface Row { index: number; key: Bytes | null; child: string | null; inline_delta: boolean; cell: { sequence: string; tombstone: boolean; overflow_page: string | null; overflow_bytes: string | null; value: Bytes | null } | null }
interface TreePage { version: string; root: string; id: string; path: string; fingerprint: number; kind: 'leaf' | 'inner'; frame_bytes: number; pending_delta_pages: number; entries: number; offset: number; next: number | null; rows: Row[] }
export const initialPageCursor = (): PageCursor => ({ path: '', offset: 0 });

function ByteValue({ value, text }: { value: Bytes | null; text?: boolean }) {
  if (!value) return <span>Unbounded</span>;
  return <span className="tw-font-mono tw-break-all">{text && value.text !== null ? JSON.stringify(value.text) : value.hex || '(empty)'}
    {text && value.text === null && <span className="tw-text-muted"> · invalid UTF-8; hex</span>}
    {value.truncated && <span className="tw-text-degraded"> … preview of {value.bytes} bytes</span>}</span>;
}

export function PageExplorer({ active, partition, generation, catalogPage, catalogOffset, cursor, onCursor, propertyHost }: {
  active: boolean; partition: Partition; generation: string; catalogPage: number; catalogOffset: number;
  cursor: PageCursor; onCursor: (cursor: PageCursor) => void; propertyHost: HTMLDivElement | null;
}) {
  const { checkpoint } = useDomain();
  const [page, setPage] = useState<TreePage | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [revision, setRevision] = useState(0);
  const { path, offset, version, fingerprint } = cursor;
  useEffect(() => {
    if (!active) return;
    const abort = new AbortController();
    setBusy(true); setError(''); setPage(null);
    const query = new URLSearchParams({ id: partition.id, epoch: partition.epoch, generation, page: String(catalogPage), offset: String(catalogOffset), page_path: path, entry_offset: String(offset) });
    if (version) query.set('tree_version', version);
    if (fingerprint != null) query.set('page_fingerprint', String(fingerprint));
    const token = getManagementToken();
    fetch(`${getApiBase()}/chunk-kv/runtime?${query}`, { signal: abort.signal, headers: token ? { Authorization: `Bearer ${token}` } : {} })
      .then(response => readJson<{ page: TreePage }>(response)).then(({ page }) => {
        if (!page || page.path !== path || page.offset !== offset || page.rows.length > 20 || version && page.version !== version || fingerprint != null && page.fingerprint !== fingerprint) throw new Error('Page response does not match this observation');
        if (!abort.signal.aborted) setPage(page);
      }).catch(error => { if (!abort.signal.aborted) setError(String(error)); })
      .finally(() => { if (!abort.signal.aborted) setBusy(false); });
    return () => abort.abort();
  }, [active, partition.id, partition.epoch, generation, catalogPage, catalogOffset, path, offset, version, fingerprint, revision]);
  const navigate = (next: PageCursor) => {
    if (page) onCursor({ ...cursor, version: page.version, fingerprint: page.fingerprint });
    checkpoint(); onCursor({ ...next, text: cursor.text });
  };
  const steps = path ? path.split('.') : [];
  const selected = page?.rows.find(row => row.index === cursor.selected);
  const disabled = busy || !active || !!error;
  return <section aria-label="KV Page explorer" className="tw-space-y-3 tw-rounded tw-border tw-border-border tw-p-3">
    <div className="tw-flex tw-flex-wrap tw-items-center tw-gap-3"><h3 className="tw-font-semibold">KV Pages</h3>
      <button className={buttonClass} disabled={!active || busy} onClick={() => { onCursor(initialPageCursor()); setRevision(v => v + 1); }}>Refresh page root</button>
      <label className="tw-text-xs"><input type="checkbox" checked={!!cursor.text} onChange={event => onCursor({ ...cursor, text: event.target.checked })} /> UTF-8 text</label>
    </div>
    <p className="tw-text-xs tw-text-muted">Physical base pages · up to 20 records per window · 256-byte previews. Pending memory and external delta records are separate; this is not a live KV scan. Overflow values are not fetched.</p>
    {busy && <p role="status">Reading tree page…</p>}
    {error && <p role="alert" className="tw-text-degraded">Page unavailable: {error}. Refresh the page root; if ownership changed, refresh the catalog.</p>}
    {page && <>
      <nav aria-label="Page ancestry" className="tw-flex tw-flex-wrap tw-gap-2">
        <button className={buttonClass} disabled={disabled || !steps.length} onClick={() => navigate({ path: '', offset: 0, version: page.version })}>Root {page.root}</button>
        {steps.map((step, index) => <button key={index} className={buttonClass} disabled={disabled || index === steps.length - 1} onClick={() => navigate({ path: steps.slice(0, index + 1).join('.'), offset: 0, version: page.version })}>Child {step}</button>)}
      </nav>
      <div className="tw-flex tw-flex-wrap tw-gap-4 tw-text-xs"><strong>{page.kind === 'inner' ? 'Inner' : 'Leaf'} Page {page.id}</strong><span>{page.frame_bytes} B frame</span><span>{page.entries} structural entries</span><span>Version {page.version}</span><span>{page.pending_delta_pages} pending delta pages</span></div>
      <table aria-label="KV Page entries" className="tw-w-full tw-text-left tw-text-xs"><thead><tr>{['Entry', `${page.kind === 'inner' ? 'Lower separator' : 'Key'} (${cursor.text ? 'UTF-8 / hex' : 'hex'})`, page.kind === 'inner' ? 'Child Page' : 'Stored record'].map(label => <th className="tw-p-2 tw-border-b tw-border-border" key={label}>{label}</th>)}</tr></thead>
        <tbody>{page.rows.map(row => <tr key={row.index} className={selected?.index === row.index ? 'tw-bg-panel' : ''}>
          <td className="tw-p-2"><button className="tw-text-accent tw-underline" aria-pressed={selected?.index === row.index} onClick={() => onCursor({ ...cursor, version: page.version, fingerprint: page.fingerprint, selected: row.index })}>Entry {row.index}</button></td>
          <td className="tw-p-2"><ByteValue value={row.key} text={cursor.text} /></td>
          <td className="tw-p-2">{row.child ? <button className={buttonClass} disabled={disabled} onClick={() => navigate({ path: [...steps, String(row.index)].join('.'), offset: 0, version: page.version })}>Page {row.child}</button>
            : `${row.inline_delta ? 'Inline delta · ' : ''}${row.cell?.tombstone ? 'Tombstone' : row.cell?.overflow_page ? 'Overflow reference' : 'Inline value'} · sequence ${row.cell?.sequence}`}</td>
        </tr>)}</tbody></table>
      {!page.rows.length && <p className="tw-text-sm">This base page has no stored entries. Pending writes may still be in memory or delta pages.</p>}
      <div className="tw-flex tw-gap-2"><button className={buttonClass} disabled={disabled || page.offset === 0} onClick={() => navigate({ path, offset: Math.max(0, page.offset - 20), version: page.version, fingerprint: page.fingerprint })}>Previous page entries</button>
        <button className={buttonClass} disabled={disabled || page.next === null} onClick={() => navigate({ path, offset: page.next!, version: page.version, fingerprint: page.fingerprint })}>Next page entries</button></div>
      {selected && propertyHost && createPortal(<section aria-label="KV Page entry" className="tw-space-y-3 tw-border-t tw-border-border tw-pt-4 tw-text-xs">
        <h3 className="tw-font-semibold">Page {page.id} · entry {selected.index}</h3><div>Key · {selected.key?.bytes ?? 0} bytes</div><ByteValue value={selected.key} text={cursor.text} />
        {selected.child && <div>Child Page {selected.child}</div>}
        {selected.cell && <><div>Sequence {selected.cell.sequence}</div>{selected.cell.overflow_page ? <div>Overflow Page {selected.cell.overflow_page} · {selected.cell.overflow_bytes} bytes · not loaded</div>
          : <><div>{selected.cell.tombstone ? 'Tombstone' : `Value · ${selected.cell.value?.bytes ?? 0} bytes`}</div><ByteValue value={selected.cell.value} text={cursor.text} /></>}</>}
      </section>, propertyHost)}
    </>}
  </section>;
}
