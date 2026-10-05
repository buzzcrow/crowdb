// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { buttonClass } from '../access/Workbench';
import { Fields, Records, Structured, byteSize } from './Fields';
import { ManifestTable } from './ManifestTable';
import { ParquetInspector } from './ParquetInspector';
import type { Inspector } from './useInspection';
import type { FileEntry } from './types';
const filename = (path: string) => path.split('/').pop() || path;
export function InspectionView({ inspector, propertyHost }: { inspector: Inspector; propertyHost?: HTMLDivElement | null }) {
  const selection = inspector.selection;
  if (!selection) return null;
  const { data, error, busy } = inspector;
  return <div className="tw-space-y-4" aria-label="Iceberg file inspection">
    {busy && <p role="status">Reading metadata…</p>}{error && <p role="alert" className="tw-text-failed tw-text-sm">{error}</p>}
    {selection.kind === 'snapshot' && <Fields values={{ 'Snapshot ID': selection.snapshot['snapshot-id'], ...(selection.snapshot.summary ?? {}) as Record<string, unknown> }} />}
    {selection.file && <details><summary className="tw-text-xs">Manifest entry and metrics</summary><Fields values={selection.file} /></details>}
    {data?.kind === 'unsupported' && <><Fields values={{ Format: data.format, Size: byteSize(data.size) } as Record<string, unknown>} /><p>{data.reason}</p></>}
    {data?.kind === 'parquet' && <ParquetInspector key={`${data.location}:${data.groups?.[0]?.index}`} data={data} propertyHost={propertyHost} query={inspector.detail} onQuery={inspector.setDetail} />}
    {data?.kind === 'manifest-list' && <ManifestTable key={String(selection.snapshot['snapshot-id'])} inspector={inspector} selection={selection} />}
    {data?.kind === 'manifest' && <><Fields values={{ Codec: data.codec, ...data.descriptor }} /><Records label="Manifest file entries" headings={['File', 'Status', 'Content', 'Format', 'Records', 'Size']} rows={(data.rows as FileEntry[]).map(f => [<button className="tw-text-accent tw-underline" onClick={() => void inspector.select({ ...selection, kind: 'file', file: f })}>{filename(f.location)}</button>, f.status, f.content, f.format, f.records, byteSize(f.size)])} /><details><summary>Writer schema</summary><Structured value={data.schema} /></details></>}
    {data && data.kind !== 'parquet' && Number(data.offset) > 0 && <button className={buttonClass} disabled={busy} onClick={() => void inspector.select(selection, data.previous ?? '0')}>{data.previous ? 'Previous references' : 'First references'}</button>}
    {data?.next != null && <button className={buttonClass} disabled={busy} onClick={() => void inspector.select(selection, data.next!)}>{data.kind === 'parquet' ? 'Next row groups' : 'Next references'}</button>}
    {data?.kind === 'parquet' && data.groups?.[0]?.index !== '0' && <button className={buttonClass} disabled={busy} onClick={() => void inspector.select(selection, String(Math.max(0, Number(data.groups?.[0]?.index) - 20)))}>Previous row groups</button>}
  </div>;
}
