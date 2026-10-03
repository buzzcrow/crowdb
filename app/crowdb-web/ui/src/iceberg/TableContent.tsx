// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { Fields, Records, byteSize, scalar } from './Fields';
import { SchemaTable } from './SchemaTable';
import type { Inspector } from './useInspection';
import type { TableLoad } from './types';
export function TableContent({ loaded, section, inspector }: { loaded: TableLoad; section: string; inspector: Inspector }) {
  const metadata = loaded.metadata;
  const snapshots = metadata.snapshots ?? [];
  const current = snapshots.find(s => String(s['snapshot-id']) === String(metadata['current-snapshot-id']));
  const summary = (current?.summary ?? {}) as Record<string, unknown>;
  const snapshotRows = <Records label="Table snapshots" headings={['Snapshot', 'Current', 'Time', 'Operation', 'Records', 'Files']} rows={snapshots.map(snapshot => [<button className="tw-text-accent" onClick={() => void inspector.select({ snapshot, kind: 'snapshot' })}>{String(snapshot['snapshot-id'])}</button>, snapshot === current ? 'Current' : '', snapshot['timestamp-ms'] ? new Date(Number(snapshot['timestamp-ms'])).toLocaleString() : '—', scalar((snapshot.summary as Record<string, unknown> | undefined)?.operation), scalar((snapshot.summary as Record<string, unknown> | undefined)?.['total-records']), scalar((snapshot.summary as Record<string, unknown> | undefined)?.['total-data-files'])])} />;
  if (section === 'Overview') return <div className="tw-space-y-5">
    <div className="tw-grid tw-grid-cols-2 xl:tw-grid-cols-4 tw-gap-3">{Object.entries({ 'Format version': metadata['format-version'], Snapshots: snapshots.length, 'Physical records': summary['total-records'], 'Data files': summary['total-data-files'] }).map(([label, value]) => <div key={label} className="tw-rounded tw-border tw-border-border tw-bg-panel tw-p-4"><p className="tw-text-xs tw-text-muted">{label}</p><p className="tw-text-lg tw-mt-1">{scalar(value)}</p></div>)}</div>
    <h2 className="tw-font-medium">Current snapshot</h2><Fields values={{ ID: current?.['snapshot-id'], Operation: summary.operation, 'Data size': byteSize(summary['total-files-size'] as string | undefined), 'Schema ID': metadata['current-schema-id'] }} />
    {current && <button className="tw-text-sm tw-text-accent" onClick={() => void inspector.select({ snapshot: current, kind: 'snapshot' })}>Inspect current snapshot</button>}
    <h2 className="tw-font-medium">Snapshots</h2>{snapshotRows}
  </div>;
  if (section === 'Schema') {
    const schemas = (metadata.schemas ?? []) as Array<{ 'schema-id': number; fields: Array<{ id: number; name: string; type: unknown; required?: boolean }> }>;
    const schema = schemas.find(s => s['schema-id'] === metadata['current-schema-id']) ?? schemas.at(-1);
    return <div className="tw-space-y-4"><Fields values={{ 'schema-id': schema?.['schema-id'], 'Partition spec': metadata['default-spec-id'], 'Sort order': metadata['default-sort-order-id'] }} /><SchemaTable key={String(schema?.['schema-id'])} fields={schema?.fields ?? []} /></div>;
  }

  return <div className="tw-space-y-3"><h2 className="tw-font-medium">Metadata file</h2><p className="tw-text-sm tw-break-all">{loaded['metadata-location']}</p><p className="tw-text-sm tw-text-muted">Expand a snapshot to inspect its manifest list, manifests and data or delete files.</p></div>;
}
