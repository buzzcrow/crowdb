// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { AccessError } from '../../access/native';
import { digest } from '../../access/digest';
import { integer, oldManifests } from './commit';
import { inspectSource, sourceSnapshot } from './source';
import { fileRequest, location } from './files';
import { listSchema, manifestSchema, ocf } from './avro';
import { save, type ImportRequest } from './storage';
import type { FileEntry } from '../types';

async function write(op: ImportRequest, path: string, data: Blob, signal?: AbortSignal) {
  if (op.written?.includes(path)) return;
  // A lost PUT response must not overwrite a write-once file on retry.
  try {
    const found = await fileRequest(op, path, 'HEAD', undefined, {}, signal);
    if (found.headers.get('content-length') !== String(data.size) || found.headers.get('etag')?.replaceAll('"', '') !== await digest(data, signal)) throw new Error('Prepared Manifest identity has different bytes.');
  } catch (reason) {
    if (!(reason instanceof AccessError) || reason.status !== 404) throw reason;
    await fileRequest(op, path, 'PUT', data, {}, signal);
  }
  op.written = [...op.written ?? [], path]; save(op);
}
async function rewrite(op: ImportRequest, manifest: Record<string, unknown>, index: number, signal?: AbortSignal) {
  const removed = new Set(op.removed!.filter(value => value.manifest === manifest.manifest_path).map(value => value.location));
  const rows: FileEntry[] = []; const seen = new Set<string>(); let offset: string | undefined; let schema: unknown;
  do {
    const page = await inspectSource(op, String(manifest.manifest_path), offset, signal);
    if (!page.rewrite_safe || manifest.content !== 0) throw new Error('This Manifest cannot be safely edited by the Console. Use an Iceberg client.');
    schema = (op.table!.metadata.schemas as Array<Record<string, unknown>>).find(value => String(value['schema-id']) === String((page.schema as Record<string, unknown>)?.['schema-id']));
    if (!schema) throw new Error('The Manifest Schema is unavailable.');
    rows.push(...(page.rows as FileEntry[]).filter(row => row.status !== 'Deleted'));
    if (rows.length > 10000 || seen.size >= 1000) throw new Error('The edited Manifest exceeds the 10000-entry budget.');
    offset = page.next ?? undefined;
    if (offset && seen.has(offset)) throw new Error('Manifest inspection did not advance.');
    if (offset) seen.add(offset);
  } while (offset);
  if (rows.some(row => row.content !== 'Data') || [...removed].some(path => rows.filter(row => row.location === path).length !== 1)) throw new Error('Removed files must be uniquely live data files in the source.');
  const retained = rows.filter(row => !removed.has(row.location));
  const deleted = rows.filter(row => removed.has(row.location));
  const sequence = integer(op.table!.metadata['last-sequence-number'] ?? 0) + 1n;
  const data = ocf(manifestSchema, rows.map(row => ({ status: removed.has(row.location) ? 2 : 0,
    snapshot_id: removed.has(row.location) ? op.snapshot : row.snapshot_id,
    sequence_number: row.data_sequence, file_sequence_number: row.file_sequence,
    data_file: { content: 0, file_path: row.location, file_format: row.format.toUpperCase(), partition: {}, record_count: row.records, file_size_in_bytes: row.size },
  })), { schema: JSON.stringify(schema), 'partition-spec': '[]', 'partition-spec-id': String(manifest.partition_spec_id), 'format-version': '2', content: 'data' }, op.identity);
  const path = location(op, `metadata/${op.identity}-edit-${index}.avro`);
  await write(op, path, data, signal);
  return { removedBytes: deleted.reduce((sum, row) => sum + integer(row.size), 0n), manifest: { ...manifest,
    manifest_path: path, manifest_length: data.size, sequence_number: sequence, added_snapshot_id: op.snapshot,
    min_sequence_number: retained.reduce((min, row) => integer(row.data_sequence) < min ? integer(row.data_sequence) : min, sequence),
    added_files_count: 0, existing_files_count: retained.length, deleted_files_count: deleted.length,
    added_rows_count: 0, existing_rows_count: retained.reduce((sum, row) => sum + integer(row.records), 0n), deleted_rows_count: deleted.reduce((sum, row) => sum + integer(row.records), 0n),
  } };
}
export async function manifests(op: ImportRequest, signal?: AbortSignal) {
  if (op.list) return;
  const metadata = op.table!.metadata; const sequence = integer(metadata['last-sequence-number'] ?? 0) + 1n;
  const spec = Number(metadata['default-spec-id']); const old = await oldManifests(op, signal, true);
  if (op.removed?.some(value => !old.some(row => row.manifest_path === value.manifest))) throw new Error('A removed file is outside the fixed source Snapshot.');
  let removedBytes = 0n;
  for (let index = 0; index < old.length; index++) {
    if (!op.removed?.some(value => value.manifest === old[index].manifest_path)) continue;
    const result = await rewrite(op, old[index], index, signal); old[index] = result.manifest; removedBytes += result.removedBytes;
  }
  if (op.files.length) {
    const data = ocf(manifestSchema, op.files.map(input => ({ status: 1, snapshot_id: op.snapshot, sequence_number: sequence, file_sequence_number: sequence,
      data_file: { content: 0, file_path: input.location, file_format: 'PARQUET', partition: {}, record_count: input.rows, file_size_in_bytes: input.size } })),
    { schema: JSON.stringify(op.schema), 'partition-spec': '[]', 'partition-spec-id': String(spec), 'format-version': '2', content: 'data' }, op.identity);
    const path = location(op, `metadata/${op.identity}-add.avro`); await write(op, path, data, signal);
    old.push({ manifest_path: path, manifest_length: data.size, partition_spec_id: spec, added_snapshot_id: op.snapshot, content: 0,
      sequence_number: sequence, min_sequence_number: sequence, added_files_count: op.files.length, existing_files_count: 0, deleted_files_count: 0,
      added_rows_count: op.files.reduce((sum, file) => sum + BigInt(file.rows), 0n), existing_rows_count: 0, deleted_rows_count: 0 });
  }
  const previous = metadata.snapshots?.find(snapshot => String(snapshot['snapshot-id']) === sourceSnapshot(op));
  const size = (previous?.summary as Record<string, string> | undefined)?.['total-files-size'];
  const data = old.filter(row => row.content === 0);
  op.totals = { rows: data.reduce((sum, row) => sum + integer(row.added_rows_count) + integer(row.existing_rows_count), 0n).toString(),
    files: data.reduce((sum, row) => sum + integer(row.added_files_count) + integer(row.existing_files_count), 0n).toString(),
    ...(!previous || size !== undefined ? { bytes: (integer(size ?? 0) - removedBytes + op.files.reduce((sum, file) => sum + BigInt(file.size), 0n)).toString() } : {}) };
  const list = ocf(listSchema, old, {}, op.identity); const path = location(op, `metadata/${op.identity}-list.avro`);
  await write(op, path, list, signal); op.list = path; save(op);
}
