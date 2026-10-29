// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { getApiBase } from '../../api';
import { iceberg, AccessError } from '../../access/native';
import { currentSchema, integer, supported, tablePath } from '../import/commit';
import { requestKey, snapshotId, exactJson } from '../import/request';
import type { ImportRequest } from '../import/storage';
import type { Selection, TableLoad } from '../types';
import { removeFile } from './removeFile';

export interface Maintenance extends ImportRequest {
  action: 'remove-file' | 'expire-snapshot'; target: Selection; timestamp: number;
}
const key = (namespace: string[], name: string) => `crowdb.iceberg.maintenance:${getApiBase()}:${JSON.stringify(namespace)}:${name}`;
export function load(namespace: string[], name: string): Maintenance | undefined {
  const text = localStorage.getItem(key(namespace, name)); if (!text) return;
  const value = JSON.parse(text) as Maintenance;
  if (value.version !== 1 || value.name !== name || JSON.stringify(namespace) !== JSON.stringify(value.namespace) || !['remove-file', 'expire-snapshot'].includes(value.action) || !value.table) throw new Error('Invalid saved Table maintenance request.');
  return value;
}
export function save(op: Maintenance) {
  const existing = load(op.namespace, op.name);
  if (existing && existing.identity !== op.identity) throw new Error('Resume or discard the saved Table maintenance request first.');
  const table = { ...op.table!, config: Object.fromEntries(Object.entries(op.table?.config ?? {}).filter(([key]) => key === 'client.refresh-credentials-endpoint')) };
  const text = JSON.stringify({ ...op, table });
  if (new TextEncoder().encode(text).length > 4 * 1024 * 1024) throw new Error('Saved maintenance request exceeds 4 MiB.');
  localStorage.setItem(key(op.namespace, op.name), text);
}
export function forget(op: Maintenance) { if (load(op.namespace, op.name)?.identity === op.identity) localStorage.removeItem(key(op.namespace, op.name)); }
export function protection(table: TableLoad, target: Selection, action: Maintenance['action']): string | undefined {
  const id = String(target.snapshot['snapshot-id']);
  const metadata = table.metadata;
  if (action === 'expire-snapshot') {
    const refs = Object.entries(metadata.refs ?? {}).filter(([, ref]) => String(ref['snapshot-id']) === id).map(([name]) => name);
    if (String(metadata['current-snapshot-id']) === id || refs.length) return `Snapshot is current or referenced by ${refs.join(', ') || 'main'}.`;
  } else {
    try { supported(table); } catch (reason) { return String(reason); }
    if (String(metadata['current-snapshot-id']) !== id || target.file?.status === 'Deleted') return 'Select a live file in the current Snapshot.';
    if (target.file?.content !== 'Data' || target.file.format.toUpperCase() !== 'PARQUET') return 'Only Parquet data files can be removed here.';
  }
}
export function begin(namespace: string[], name: string, table: TableLoad, target: Selection, action: Maintenance['action']): Maintenance {
  const problem = protection(table, target, action); if (problem) throw new Error(problem);
  return { version: 1, namespace, name, create: false, schema: currentSchema(table), files: [], identity: requestKey(), snapshot: snapshotId(), table: structuredClone(table), target: structuredClone(target), action, timestamp: Date.now() };
}
export function commit(op: Maintenance, updates: object[]): string {
  return exactJson({ 'x-crowdb-base-metadata-location': op.table!['metadata-location'], requirements: [{ type: 'assert-table-uuid', uuid: op.table!.metadata['table-uuid'] }], updates });
}
export async function submit(op: Maintenance, signal?: AbortSignal) {
  save(op);
  if (!op.commit) {
    const problem = protection(op.table!, op.target, op.action); if (problem) throw new Error(problem);
    op.commit = op.action === 'expire-snapshot'
      ? commit(op, [{ action: 'remove-snapshots', 'snapshot-ids': [integer(op.target.snapshot['snapshot-id'])] }])
      : await removeFile(op, signal);
    save(op);
  }
  const wasUncertain = !!op.uncertain;
  op.uncertain = true; save(op);
  try { await iceberg(tablePath(op), '', 'POST', op.commit, signal, op.identity); }
  catch (reason) {
    if (!wasUncertain && reason instanceof AccessError && !reason.unknownOutcome) { op.uncertain = false; save(op); }
    throw reason;
  }
  forget(op);
}
