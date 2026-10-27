// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { iceberg, AccessError } from '../../access/native';
import type { Inspection, TableLoad } from '../types';
import { listSchema, manifestSchema, ocf } from './avro';
import { fileRequest, location, uploadFile } from './files';
import { exactJson } from './request';
import { schemaFromParquet, footer, nameMapping, type IceSchema } from './schema';
import { forget, save, type ImportRequest } from './storage';
import { digest } from '../../access/digest';
export const namespacePath = (namespace: string[]) => `/v1/namespaces/${encodeURIComponent(namespace.join('\x1f'))}`;
export const tablePath = (op: Pick<ImportRequest, 'namespace' | 'name'>) => `${namespacePath(op.namespace)}/tables/${encodeURIComponent(op.name)}`;
export const currentSchema = (table: TableLoad): IceSchema => (table.metadata.schemas as IceSchema[]).find(value => value['schema-id'] === table.metadata['current-schema-id'])!;
const integer = (value: unknown): bigint => {
  if (!/^-?(0|[1-9][0-9]*)$/.test(String(value))) throw new Error('Table contains an invalid exact integer.');
  const number = BigInt(String(value)); if (number < -(1n << 63n) || number >= 1n << 63n) throw new Error('Table integer exceeds i64.'); return number;
};

export function supported(table: TableLoad) {
  if (table.metadata['format-version'] !== 2 || !(table.metadata['partition-specs'] as Array<{ fields: unknown[] }>).every(spec => spec.fields.length === 0)) throw new Error('Console Parquet import currently supports unpartitioned format-v2 Tables. This Table remains available for browsing and metadata operations.');
  if (!currentSchema(table)) throw new Error('Table current Schema is unavailable.');
}

async function oldManifests(op: ImportRequest, signal?: AbortSignal): Promise<Record<string, unknown>[]> {
  const table = op.table!; const id = table.metadata.refs?.main?.['snapshot-id'] ?? table.metadata['current-snapshot-id'];
  const snapshot = table.metadata.snapshots?.find(snapshot => String(snapshot['snapshot-id']) === String(id));
  if (!snapshot) return [];
  const result: Record<string, unknown>[] = []; const seen = new Set<string>(); let offset: string | undefined;
  do {
    const query = new URLSearchParams({ metadata: table['metadata-location'], snapshot: String(id) }); if (offset) query.set('offset', offset);
    const page: Inspection = await iceberg(`${tablePath(op)}/inspect?${query}`, '', 'GET', undefined, signal);
    if (page.kind !== 'manifest-list' || page.metadata_location !== table['metadata-location'] || page.snapshot_id !== String(id) || page.location !== snapshot['manifest-list']) throw new Error('Manifest inspection does not match the frozen Table head.');
    for (const row of page.rows ?? []) {
      const value = row as Record<string, any>;
      if (value.partitions?.length || !['Data', 'Deletes'].includes(value.content) || !Array.isArray(value.row_counts) || [...value.file_counts, ...value.row_counts].some(value => value === null)) throw new Error('Historical manifest summaries cannot be preserved by this import.');
      result.push({ manifest_path: value.location, manifest_length: value.size, partition_spec_id: value.partition_spec_id, added_snapshot_id: value.snapshot_id, content: value.content === 'Data' ? 0 : 1,
        sequence_number: value.sequence, min_sequence_number: value.min_sequence, added_files_count: value.file_counts[0], existing_files_count: value.file_counts[1], deleted_files_count: value.file_counts[2], added_rows_count: value.row_counts[0], existing_rows_count: value.row_counts[1], deleted_rows_count: value.row_counts[2] });
    }
    if (result.length > 10000 || seen.size >= 1000) throw new Error('Table exceeds the 10000-manifest import budget.');
    offset = page.next ?? undefined;
    if (offset && seen.has(offset)) throw new Error('Manifest inspection did not advance.');
    if (offset) seen.add(offset);
  } while (offset);
  return result;
}

async function manifests(op: ImportRequest, signal?: AbortSignal) {
  if (op.list) return;
  const metadata = op.table!.metadata; const sequence = integer(metadata['last-sequence-number'] ?? 0) + 1n;
  const spec = Number(metadata['default-spec-id']);
  const old = await oldManifests(op, signal);
  const data = ocf(manifestSchema, op.files.map(input => ({ status: 1, snapshot_id: op.snapshot, sequence_number: sequence, file_sequence_number: sequence,
    data_file: { content: 0, file_path: input.location, file_format: 'PARQUET', partition: {}, record_count: input.rows, file_size_in_bytes: input.size } })),
  { schema: JSON.stringify(op.schema), 'partition-spec': '[]', 'partition-spec-id': String(spec), 'format-version': '2', content: 'data' }, op.identity);
  const path = location(op, `metadata/${op.identity}-m0.avro`);
  if (!op.manifest) { await fileRequest(op, path, 'PUT', data, {}, signal); op.manifest = path; save(op); }
  const rows = op.files.reduce((sum, file) => sum + BigInt(file.rows), 0n);
  const previous = metadata.snapshots?.find(snapshot => String(snapshot['snapshot-id']) === String(metadata['current-snapshot-id']));
  const size = (previous?.summary as Record<string, string> | undefined)?.['total-files-size'];
  const existingData = old.filter(row => row.content === 0);
  op.totals = { rows: (existingData.reduce((sum, row) => sum + integer(row.added_rows_count) + integer(row.existing_rows_count), 0n) + rows).toString(),
    files: (existingData.reduce((sum, row) => sum + integer(row.added_files_count) + integer(row.existing_files_count), 0n) + BigInt(op.files.length)).toString(),
    ...(!previous || size !== undefined ? { bytes: (integer(size ?? 0) + op.files.reduce((sum, file) => sum + BigInt(file.size), 0n)).toString() } : {}) };
  const list = ocf(listSchema, [...old, { manifest_path: path, manifest_length: data.size, partition_spec_id: spec, added_snapshot_id: op.snapshot, content: 0,
    sequence_number: sequence, min_sequence_number: sequence, added_files_count: op.files.length, existing_files_count: 0, deleted_files_count: 0, added_rows_count: rows, existing_rows_count: 0, deleted_rows_count: 0 }], {}, op.identity);
  const listPath = location(op, `metadata/${op.identity}-list.avro`);
  await fileRequest(op, listPath, 'PUT', list, {}, signal); op.list = listPath; save(op);
}

export function commitBody(op: ImportRequest): string {
  const metadata = op.table!.metadata; const updates: object[] = [];
  const main = metadata.refs?.main?.['snapshot-id'] ?? (integer(metadata['current-snapshot-id'] ?? -1) > 0n ? metadata['current-snapshot-id'] : null);
  const requirements = op.create ? [{ type: 'assert-create' }] : [{ type: 'assert-table-uuid', uuid: metadata['table-uuid'] },
    { type: 'assert-current-schema-id', 'current-schema-id': metadata['current-schema-id'] }, { type: 'assert-default-spec-id', 'default-spec-id': metadata['default-spec-id'] },
    { type: 'assert-ref-snapshot-id', ref: 'main', 'snapshot-id': main === null ? null : integer(main) }];
  if (op.create) updates.push({ action: 'assign-uuid', uuid: metadata['table-uuid'] }, { action: 'upgrade-format-version', 'format-version': 2 },
    { action: 'add-schema', schema: op.schema }, { action: 'set-current-schema', 'schema-id': -1 },
    { action: 'add-spec', spec: (metadata['partition-specs'] as object[])[0] }, { action: 'set-default-spec', 'spec-id': -1 },
    { action: 'add-sort-order', 'sort-order': (metadata['sort-orders'] as object[])[0] }, { action: 'set-default-sort-order', 'sort-order-id': -1 }, { action: 'set-location', location: metadata.location });
  if (!(metadata.properties as Record<string, string>)?.['schema.name-mapping.default']) updates.push({ action: 'set-properties', updates: { 'schema.name-mapping.default': nameMapping(op.schema) } });
  if (op.files.length) {
    const snapshot = { 'snapshot-id': integer(op.snapshot), ...(main === null ? {} : { 'parent-snapshot-id': integer(main) }), 'sequence-number': integer(metadata['last-sequence-number'] ?? 0) + 1n,
      'timestamp-ms': Date.now(), 'schema-id': op.schema['schema-id'], 'manifest-list': op.list,
      summary: { operation: 'append', 'added-data-files': String(op.files.length), 'added-records': op.files.reduce((sum, file) => sum + BigInt(file.rows), 0n).toString(), 'added-files-size': op.files.reduce((sum, file) => sum + BigInt(file.size), 0n).toString(),
        ...(op.totals ? { 'total-records': op.totals.rows, 'total-data-files': op.totals.files, ...(op.totals.bytes === undefined ? {} : { 'total-files-size': op.totals.bytes }) } : {}) } };
    updates.push({ action: 'add-snapshot', snapshot }, { action: 'set-snapshot-ref', 'ref-name': 'main', type: 'branch', 'snapshot-id': integer(op.snapshot) });
  }
  return exactJson({ requirements, updates });
}

export async function submitImport(op: ImportRequest, files: File[], progress: (value: string) => void, signal?: AbortSignal) {
  save(op);
  if (!op.table) {
    progress('Preparing Table…');
    op.table = await iceberg(`${namespacePath(op.namespace)}/tables`, '', 'POST', { name: op.name, schema: op.schema, 'stage-create': true, properties: { 'format-version': '2' } }, signal);
    op.schema = currentSchema(op.table!); supported(op.table!); save(op);
  }
  if (!op.commit) {
    supported(op.table!);
    for (const input of op.files) {
      if (input.location) continue;
      const file = files.find(file => file.name === input.name);
      if (!file || file.size !== input.size || await digest(file, signal) !== input.md5) throw new Error(`Reselect the original bytes for ${input.name}.`);
      schemaFromParquet(await footer(file), op.schema);
      progress(`Uploading ${input.name}…`); await uploadFile(op, input, file, progress, signal);
    }
    if (op.files.length) { progress('Writing Manifest and Manifest List…'); await manifests(op, signal); }
    op.commit = commitBody(op); save(op);
  }
  if (op.uncertain && op.create) {
    try {
      const current: TableLoad = await iceberg(tablePath(op), '', 'GET', undefined, signal);
      if (current.metadata['table-uuid'] === op.table!.metadata['table-uuid'] && (!op.files.length || current.metadata.snapshots?.some(snapshot => String(snapshot['snapshot-id']) === op.snapshot && snapshot['manifest-list'] === op.list))) { forget(op); return; }
    } catch (reason) { if (!(reason instanceof AccessError) || reason.status !== 404) throw reason; }
  }
  progress('Publishing Table metadata…'); op.uncertain = true; save(op);
  try { await iceberg(tablePath(op), '', 'POST', op.commit, signal, op.identity); }
  catch (reason) {
    if (reason instanceof AccessError && !reason.unknownOutcome) { op.uncertain = false; save(op); }
    throw reason;
  }
  forget(op);
}
