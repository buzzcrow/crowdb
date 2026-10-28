// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { compatibleSchema, type IceSchema } from './schema';
import { iceberg } from '../../access/native';
import { currentSchema, tablePath } from './commit';
import type { FileEntry, Inspection, Manifest, TableLoad } from '../types';
import type { ImportRequest } from './storage';

export const mainSnapshot = (table: TableLoad): string | null => {
  const value = table.metadata.refs?.main?.['snapshot-id'] ?? table.metadata['current-snapshot-id'];
  return value === undefined || value === null || String(value) === '-1' ? null : String(value);
};
export function sourceSnapshot(op: ImportRequest) { return op.source === undefined ? mainSnapshot(op.table!) : op.source; }
export function validateSource(table: TableLoad, source: string | null) {
  if (!source) return;
  const snapshot = table.metadata.snapshots?.find(value => String(value['snapshot-id']) === source);
  if (!snapshot) throw new Error('The source Snapshot is no longer available.');
  if (snapshot['schema-id'] !== undefined) {
    const schema = (table.metadata.schemas as IceSchema[]).find(value => value['schema-id'] === snapshot['schema-id']);
    if (!schema) throw new Error('The source Snapshot Schema is unavailable.');
    compatibleSchema(schema, currentSchema(table));
  }
}
export async function inspectSource(op: ImportRequest, manifest?: string, offset?: string, signal?: AbortSignal): Promise<Inspection> {
  const snapshot = sourceSnapshot(op);
  if (!snapshot) throw new Error('The Table has no source Snapshot.');
  const query = new URLSearchParams({ metadata: op.table!['metadata-location'], snapshot });
  if (manifest) query.set('manifest', manifest);
  if (offset) query.set('offset', offset);
  const page: Inspection = await iceberg(`${tablePath(op)}/inspect?${query}`, '', 'GET', undefined, signal);
  const expected = manifest ?? op.table!.metadata.snapshots?.find(value => String(value['snapshot-id']) === snapshot)?.['manifest-list'];
  if (page.kind !== (manifest ? 'manifest' : 'manifest-list') || page.metadata_location !== op.table!['metadata-location'] || page.snapshot_id !== snapshot || page.location !== expected) throw new Error('Inspection does not match the fixed source Snapshot.');
  return page;
}
export interface FileCursor { list?: string; manifest: number; file?: string }
export interface SourceFile { file: FileEntry; manifest: string }
export async function filePage(op: ImportRequest, cursor: FileCursor = { manifest: 0 }, signal?: AbortSignal): Promise<{ rows: SourceFile[]; next?: FileCursor }> {
  if (!sourceSnapshot(op)) return { rows: [] };
  const list = await inspectSource(op, undefined, cursor.list, signal);
  const manifests = list.rows as Manifest[];
  const manifest = manifests[cursor.manifest];
  if (!manifest) return { rows: [], ...(list.next ? { next: { list: list.next, manifest: 0 } } : {}) };
  const page = await inspectSource(op, manifest.location, cursor.file, signal);
  const next = page.next ? { ...cursor, file: page.next } : cursor.manifest + 1 < manifests.length ? { list: cursor.list, manifest: cursor.manifest + 1 }
    : list.next ? { list: list.next, manifest: 0 } : undefined;
  if (next && JSON.stringify(next) === JSON.stringify(cursor)) throw new Error('File inspection did not advance.');
  return { rows: (page.rows as FileEntry[]).filter(file => file.status !== 'Deleted').map(file => ({ file, manifest: manifest.location })), next };
}
