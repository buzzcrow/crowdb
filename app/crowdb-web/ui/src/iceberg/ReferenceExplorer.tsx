// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { buttonClass } from '../access/Workbench';
import { Fields, Records, Structured, byteSize, scalar } from './Fields';
import { ParquetInspector } from './ParquetInspector';
import type { Inspector } from './useInspection';
import type { FileEntry, Manifest, Selection, TableLoad } from './types';
const filename = (path: string) => path.split('/').pop() || path;
export function ReferenceTree({ loaded, inspector }: { loaded: TableLoad; inspector: Inspector }) {
  const [snapshotPage, setSnapshotPage] = useState(0);
  const snapshots = [...(loaded.metadata.snapshots ?? [])].reverse();
  const visible = snapshots.slice(snapshotPage * 30, (snapshotPage + 1) * 30);
  const selectedSnapshot = inspector.selection?.snapshot;
  if (selectedSnapshot && !visible.some(s => s['snapshot-id'] === selectedSnapshot['snapshot-id'])) visible.push(selectedSnapshot);
  const refs = loaded.metadata.refs ?? {};
  const node = (selection: Selection, label: string) => <button className="tw-block tw-w-full tw-text-left tw-text-xs tw-py-1 tw-break-all hover:tw-text-accent" aria-pressed={inspector.selection?.kind === selection.kind && inspector.key(inspector.selection) === inspector.key(selection)} onClick={() => void inspector.select(selection)}>{label}</button>;
  return <nav aria-label="Iceberg references" className="tw-pl-2 tw-border-l tw-border-border">{visible.map(snapshot => {
    const listSelection: Selection = { snapshot, kind: 'list' };
    const list = inspector.cache[inspector.key(listSelection)];
    const selected = String(inspector.selection?.snapshot['snapshot-id']) === String(snapshot['snapshot-id']);
    const manifests = [...(list?.rows as Manifest[] ?? [])];
    const pinned = selected ? inspector.selection?.manifest : undefined;
    if (pinned && !manifests.some(m => m.location === pinned.location)) manifests.push(pinned);
    return <div key={String(snapshot['snapshot-id'])}>{node({ snapshot, kind: 'snapshot' }, `Snapshot ${snapshot['snapshot-id']} ${Object.entries(refs).filter(([, ref]) => String(ref['snapshot-id']) === String(snapshot['snapshot-id'])).map(([name]) => `· ${name}`).join(' ')}`)}
      {selected && <div className="tw-pl-2 tw-border-l tw-border-border">{snapshot['manifest-list'] ? node(listSelection, filename(snapshot['manifest-list'])) : <span className="tw-text-xs">Legacy snapshot: no manifest list</span>}
        {manifests.map(manifest => {
          const s: Selection = { snapshot, kind: 'manifest', manifest };
          const files = inspector.cache[inspector.key(s)];
          return <div key={manifest.location} className="tw-pl-2">{node(s, `${manifest.content} · ${filename(manifest.location)}`)}{inspector.selection?.manifest?.location === manifest.location && <div className="tw-pl-2 tw-border-l tw-border-border">{(files?.rows as FileEntry[] | undefined)?.map(file => <div key={file.location}>{node({ ...s, kind: 'file', file }, `${file.status} · ${filename(file.location)}`)}</div>)}{files && <TreePages data={files} selection={s} inspector={inspector} />}{inspector.selection?.file && !files?.rows?.some(f => f.location === inspector.selection?.file?.location) && <p className="tw-text-xs tw-text-muted tw-break-all">Selected outside this page: {filename(inspector.selection.file.location)}</p>}</div>}</div>;
        })}
        {list && <TreePages data={list} selection={listSelection} inspector={inspector} />}
      </div>}
    </div>;
  })}{snapshotPage > 0 && <button className={buttonClass} onClick={() => setSnapshotPage(n => n - 1)}>Previous snapshots</button>}{snapshots.length > (snapshotPage + 1) * 30 && <button className={buttonClass} onClick={() => setSnapshotPage(n => n + 1)}>Next snapshots</button>}</nav>;
}
export function InspectionView({ inspector }: { inspector: Inspector }) {
  const selection = inspector.selection;
  if (!selection) return null;
  const { data, error, busy } = inspector;
  const path = selection.file?.location ?? selection.manifest?.location ?? selection.snapshot['manifest-list'];
  return <div className="tw-space-y-4" aria-label="Iceberg file inspection">
    <p className="tw-text-xs tw-text-muted tw-break-all">Snapshot {selection.snapshot['snapshot-id']} / {path}</p>
    <h2 className="tw-text-lg tw-font-medium">{selection.kind === 'snapshot' ? `Snapshot ${selection.snapshot['snapshot-id']}` : filename(path ?? '')}</h2>
    {busy && <p role="status">Reading metadata…</p>}{error && <p role="alert" className="tw-text-failed tw-text-sm">{error}</p>}
    {selection.kind === 'snapshot' && <><Fields values={selection.snapshot} />{selection.snapshot['manifest-list'] && <button className={buttonClass} onClick={() => void inspector.select({ snapshot: selection.snapshot, kind: 'list' })}>Open manifest list</button>}</>}
    {selection.file && <details><summary className="tw-text-xs">Manifest entry and metrics</summary><Fields values={selection.file} /></details>}
    {data?.kind === 'unsupported' && <><Fields values={{ Format: data.format, Size: byteSize(data.size) } as Record<string, unknown>} /><p>{data.reason}</p></>}
    {data?.kind === 'parquet' && <ParquetInspector key={`${data.location}:${data.groups?.[0]?.index}`} data={data} />}
    {data?.kind === 'manifest-list' && <><Fields values={{ Format: 'Avro manifest list', Size: byteSize(data.size), 'Loaded records': data.rows?.length }} /><Records label="Manifest list records" headings={['Manifest', 'Content', 'Spec', 'Sequence', 'Added / Existing / Deleted', 'Size']} rows={(data.rows as Manifest[]).map(m => [<button className="tw-text-accent tw-underline" onClick={() => void inspector.select({ snapshot: selection.snapshot, kind: 'manifest', manifest: m })}>{filename(m.location)}</button>, m.content, m.partition_spec_id, m.sequence, m.file_counts.map(scalar).join(' / '), byteSize(m.size)])} /></>}
    {data?.kind === 'manifest' && <><Fields values={{ Codec: data.codec, ...data.descriptor }} /><Records label="Manifest file entries" headings={['File', 'Status', 'Content', 'Format', 'Records', 'Size']} rows={(data.rows as FileEntry[]).map(f => [<button className="tw-text-accent tw-underline" onClick={() => void inspector.select({ ...selection, kind: 'file', file: f })}>{filename(f.location)}</button>, f.status, f.content, f.format, f.records, byteSize(f.size)])} /><details><summary>Writer schema</summary><Structured value={data.schema} /></details></>}
    {data && data.kind !== 'parquet' && Number(data.offset) > 0 && <button className={buttonClass} disabled={busy} onClick={() => void inspector.select(selection, data.previous ?? '0')}>{data.previous ? 'Previous references' : 'First references'}</button>}
    {data?.next != null && <button className={buttonClass} disabled={busy} onClick={() => void inspector.select(selection, data.next!)}>{data.kind === 'parquet' ? 'Next row groups' : 'Next references'}</button>}
    {data?.kind === 'parquet' && data.groups?.[0]?.index !== '0' && <button className={buttonClass} disabled={busy} onClick={() => void inspector.select(selection, String(Math.max(0, Number(data.groups?.[0]?.index) - 20)))}>Previous row groups</button>}
  </div>;
}

function TreePages({ data, selection, inspector }: { data: import('./types').Inspection; selection: Selection; inspector: Inspector }) {
  return <div className="tw-flex tw-flex-wrap tw-gap-2 tw-text-xs"><span>From {Number(data.offset ?? 0) + 1} · {data.rows?.length ?? 0} loaded</span>{Number(data.offset) > 0 && <button disabled={inspector.busy} className="tw-text-accent" onClick={() => void inspector.select(selection, data.previous ?? '0', true)}>{data.previous ? 'Previous page' : 'First page'}</button>}{data.next != null && <button disabled={inspector.busy} className="tw-text-accent" onClick={() => void inspector.select(selection, data.next!, true)}>Next page</button>}</div>;
}
