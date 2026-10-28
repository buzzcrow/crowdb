// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { iceberg } from '../../access/native';
import { listSchema, manifestSchema, ocf } from '../import/avro';
import { integer, oldManifests, tablePath } from '../import/commit';
import { fileRequest, location } from '../import/files';
import type { FileEntry, Inspection } from '../types';
import { commit, save, type Maintenance } from './operation';

async function entries(op: Maintenance, signal?: AbortSignal) {
  const result: FileEntry[] = []; const seen = new Set<string>(); let offset: string | undefined; let schema: unknown;
  do {
    const query = new URLSearchParams({ metadata: op.table!['metadata-location'], snapshot: String(op.target.snapshot['snapshot-id']), manifest: op.target.manifest!.location });
    if (offset) query.set('offset', offset);
    const page: Inspection = await iceberg(`${tablePath(op)}/inspect?${query}`, '', 'GET', undefined, signal);
    if (page.kind !== 'manifest' || page.metadata_location !== op.table!['metadata-location'] || page.snapshot_id !== String(op.target.snapshot['snapshot-id']) || page.location !== op.target.manifest!.location) throw new Error('Manifest inspection does not match the selected Table and file.');
    if (!page.rewrite_safe) throw new Error('This Manifest contains fields the Console cannot preserve. Use an Iceberg client to remove this file.');
    // Inspection renders integral metadata as strings. Avro's schema property
    // needs the original typed Schema retained in the frozen Table metadata.
    schema = (op.table!.metadata.schemas as Array<Record<string, unknown>>).find(value =>
      String(value['schema-id']) === String((page.schema as Record<string, unknown>)?.['schema-id']));
    if (!schema) throw new Error('Manifest Schema is not retained in the selected Table.');
    result.push(...page.rows as FileEntry[]);
    if (result.length > 10000 || seen.size >= 1000) throw new Error('Manifest exceeds the 10000-entry maintenance budget.');
    offset = page.next ?? undefined;
    if (offset && seen.has(offset)) throw new Error('Manifest inspection did not advance.');
    if (offset) seen.add(offset);
  } while (offset);
  return { rows: result.filter(row => row.status !== 'Deleted'), schema };
}

export async function removeFile(op: Maintenance, signal?: AbortSignal): Promise<string> {
  const old = await oldManifests(op, signal, true);
  const index = old.findIndex(row => row.manifest_path === op.target.manifest?.location);
  if (index < 0 || old[index].content !== 0) throw new Error('The selected file Manifest is no longer in current main.');
  const { rows, schema } = await entries(op, signal);
  const matches = rows.filter(row => row.location === op.target.file!.location);
  if (matches.length !== 1) throw new Error('The selected file is not uniquely live in the Manifest.');
  const removed = matches[0]; const retained = rows.filter(row => row !== removed);
  if (rows.some(row => row.content !== 'Data')) throw new Error('A data Manifest contains non-data entries.');
  const metadata = op.table!.metadata; const sequence = integer(metadata['last-sequence-number']) + 1n;
  const records = rows.map(row => ({ status: row === removed ? 2 : 0, snapshot_id: row === removed ? op.snapshot : row.snapshot_id,
    sequence_number: row.data_sequence, file_sequence_number: row.file_sequence,
    data_file: { content: 0, file_path: row.location, file_format: row.format.toUpperCase(), partition: {}, record_count: row.records, file_size_in_bytes: row.size } }));
  const manifest = ocf(manifestSchema, records, { schema: JSON.stringify(schema), 'partition-spec': '[]', 'partition-spec-id': String(old[index].partition_spec_id), 'format-version': '2', content: 'data' }, op.identity);
  const manifestPath = location(op, `metadata/${op.identity}-remove.avro`);
  if (!op.manifest) { await fileRequest(op, manifestPath, 'PUT', manifest, {}, signal); op.manifest = manifestPath; save(op); }
  const remainingRows = retained.reduce((sum, row) => sum + integer(row.records), 0n);
  const minSequence = retained.reduce((min, row) => integer(row.data_sequence) < min ? integer(row.data_sequence) : min, sequence);
  const replacement = { ...old[index], manifest_path: manifestPath, manifest_length: manifest.size, added_snapshot_id: op.snapshot, sequence_number: sequence, min_sequence_number: minSequence,
    added_files_count: 0, existing_files_count: retained.length, deleted_files_count: 1, added_rows_count: 0, existing_rows_count: remainingRows, deleted_rows_count: removed.records };
  const list = ocf(listSchema, old.map((row, position) => position === index ? replacement : row), {}, op.identity);
  const listPath = location(op, `metadata/${op.identity}-list.avro`);
  if (!op.list) { await fileRequest(op, listPath, 'PUT', list, {}, signal); op.list = listPath; save(op); }
  const data = old.filter(row => row.content === 0);
  const totalRecords = data.reduce((sum, row) => sum + integer(row.added_rows_count) + integer(row.existing_rows_count), 0n) - integer(removed.records);
  const totalFiles = data.reduce((sum, row) => sum + integer(row.added_files_count) + integer(row.existing_files_count), 0n) - 1n;
  const priorSize = (op.target.snapshot.summary as Record<string, string> | undefined)?.['total-files-size'];
  return commit(op, [{ action: 'add-snapshot', snapshot: { 'snapshot-id': integer(op.snapshot), 'parent-snapshot-id': integer(op.target.snapshot['snapshot-id']),
    'sequence-number': sequence, 'timestamp-ms': op.timestamp, 'schema-id': metadata['current-schema-id'], 'manifest-list': listPath,
    summary: { operation: 'delete', 'deleted-data-files': '1', 'deleted-records': removed.records, 'removed-files-size': removed.size,
      'total-records': totalRecords.toString(), 'total-data-files': totalFiles.toString(), ...(priorSize === undefined ? {} : { 'total-files-size': (integer(priorSize) - integer(removed.size)).toString() }) } } },
  { ...metadata.refs?.main, action: 'set-snapshot-ref', 'ref-name': 'main', type: 'branch', 'snapshot-id': integer(op.snapshot) }]);
}
