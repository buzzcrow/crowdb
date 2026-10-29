// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { afterEach, expect, it, vi } from 'vitest';
import { AccessError, iceberg } from '../../access/native';
import { commitBody, submitImport } from './commit';
import { manifests } from './manifests';
import { fileRequest } from './files';
import { filePage, validateSource } from './source';
import { ocf } from './avro';
import { loadDraft, saveDraft, type TableDraft } from './draft';
import { load, type ImportRequest } from './storage';
vi.mock('../../access/native', async original => ({ ...await original<typeof import('../../access/native')>(), iceberg: vi.fn() }));
vi.mock('./files', async original => ({ ...await original<typeof import('./files')>(), fileRequest: vi.fn() }));
vi.mock('./avro', async original => { const actual = await original<typeof import('./avro')>(); return { ...actual, ocf: vi.fn(actual.ocf) }; });
afterEach(() => { localStorage.clear(); vi.clearAllMocks(); });
const schema = { type: 'struct' as const, 'schema-id': 0, fields: [{ id: 1, name: 'id', required: true, type: 'long' }] };
const operation = (): ImportRequest => ({ version: 1, namespace: ['ns'], name: 'table', create: false, identity: '019a0000-0000-7000-8000-000000000001', snapshot: '30', source: '10', schema,
  files: [{ name: 'c.parquet', identity: 'file-c', size: 33, md5: 'md5', rows: '3', location: 's3://table/c.parquet' }], removed: [{ manifest: 's3://table/m1.avro', location: 's3://table/a.parquet' }], guarded: true,
  table: { 'metadata-location': 's3://table/v2.json', metadata: { 'table-uuid': 'uuid', location: 's3://table', 'format-version': 2,
    'current-schema-id': 0, schemas: [schema], 'partition-specs': [{ 'spec-id': 0, fields: [] }], 'default-spec-id': 0, 'last-sequence-number': 2,
    'current-snapshot-id': '20', snapshots: [{ 'snapshot-id': '10', 'schema-id': 0, 'manifest-list': 's3://table/l1.avro', summary: { 'total-files-size': '33' } }, { 'snapshot-id': '20', 'schema-id': 0, 'manifest-list': 's3://table/l2.avro' }] } } });
const file = (name: string, size: string, records: string) => ({ location: `s3://table/${name}.parquet`, status: 'Added', content: 'Data', format: 'PARQUET', size, records, snapshot_id: '10', data_sequence: '1', file_sequence: '1' });
function inspections() {
  vi.mocked(iceberg).mockImplementation(async path => {
    const query = new URLSearchParams(path.split('?')[1]); expect(query.get('snapshot')).toBe('10');
    return query.has('manifest') ? { kind: 'manifest', rewrite_safe: true, metadata_location: 's3://table/v2.json', snapshot_id: '10', location: 's3://table/m1.avro', next: null, schema,
      rows: [file('a', '11', '1'), file('b', '22', '2')] }
      : { kind: 'manifest-list', rewrite_safe: true, metadata_location: 's3://table/v2.json', snapshot_id: '10', location: 's3://table/l1.avro', next: null,
        rows: [{ location: 's3://table/m1.avro', size: '200', partition_spec_id: '0', snapshot_id: '10', content: 'Data', sequence: '1', min_sequence: '1', file_counts: ['2', '0', '0'], row_counts: ['3', '0', '0'] }] };
  });
  vi.mocked(fileRequest).mockImplementation(async (_op, _path, method) => { if (method === 'HEAD') throw new AccessError(404, 'missing'); return new Response(''); });
}
it('publishes one add/remove Snapshot from a historical source while fencing and parenting current main', async () => {
  inspections(); const op = operation(); await manifests(op);
  expect(op.totals).toEqual({ files: '2', rows: '5', bytes: '55' });
  const rewrite = vi.mocked(ocf).mock.calls[0][1];
  expect(rewrite.map(row => [row.status, row.snapshot_id, row.sequence_number, row.file_sequence_number])).toEqual([[2, '30', '1', '1'], [0, '10', '1', '1']]);
  const body = JSON.parse(commitBody(op));
  expect(body['x-crowdb-base-metadata-location']).toBe('s3://table/v2.json');
  expect(body.requirements).toContainEqual({ type: 'assert-ref-snapshot-id', ref: 'main', 'snapshot-id': 20 });
  expect(body.updates.find((update: any) => update.action === 'add-snapshot').snapshot).toMatchObject({ 'snapshot-id': 30, 'parent-snapshot-id': 20, 'sequence-number': 3, summary: { operation: 'overwrite', 'crowdb.source-snapshot-id': '10', 'total-records': '5' } });
});
it('reads only the requested manifest window and retains continuation without counting all files', async () => {
  inspections(); const original = vi.mocked(iceberg).getMockImplementation()!;
  vi.mocked(iceberg).mockImplementation(async (...args) => { const page = await original(...args); return page.kind === 'manifest' ? { ...page, next: 'more' } : page; });
  const page = await filePage(operation()); expect(page.rows).toHaveLength(2); expect(page.next).toEqual({ manifest: 0, file: 'more' });
  expect(iceberg).toHaveBeenCalledTimes(2);
});
it('rejects concurrent main changes before producing new metadata files', async () => {
  const op = operation(); vi.mocked(iceberg).mockResolvedValueOnce({ ...op.table!, 'metadata-location': 's3://table/v3.json' });
  await expect(submitImport(op, [], vi.fn())).rejects.toThrow('Table changed'); expect(fileRequest).not.toHaveBeenCalled();
});
it('rejects incompatible stored-file Schema edits before uploading or publishing metadata', async () => {
  const op = operation(); op.files = []; op.removed = [];
  op.schema = { ...schema, fields: [{ ...schema.fields[0], type: 'string' }] };
  await expect(submitImport(op, [], vi.fn())).rejects.toThrow('incompatible');
  expect(iceberg).not.toHaveBeenCalled(); expect(fileRequest).not.toHaveBeenCalled();
});
it('reconciles a lost update response by immutable Snapshot identity without publishing twice', async () => {
  const op = operation(); op.commit = 'frozen'; op.list = 's3://table/new-list.avro'; op.uncertain = true;
  vi.mocked(iceberg).mockResolvedValueOnce({ metadata: { 'table-uuid': 'uuid', snapshots: [{ 'snapshot-id': '30', 'manifest-list': op.list }] } });
  await submitImport(op, [], vi.fn()); expect(iceberg).toHaveBeenCalledTimes(1); expect(load(op.namespace, op.name)).toBeUndefined();
});
it('rejects a stale browser Draft version and keeps different source Drafts separate', () => {
  const op = operation(); const draft: TableDraft = { version: 1, identity: op.identity, namespace: op.namespace, table: op.name, name: op.name, schema, mode: 'parquet', files: op.files };
  saveDraft(draft); const stale = structuredClone(draft); saveDraft(draft);
  expect(() => saveDraft(stale)).toThrow('changed in another tab');
  const historical = { ...draft, revision: 0, requestedSource: '10' }; saveDraft(historical);
  expect(loadDraft(op.namespace, op.name)?.revision).toBe(2); expect(loadDraft(op.namespace, op.name, '10')?.revision).toBe(1);
});

it('continues editing current files after compatible metadata-only Schema evolution', () => {
  const op = operation(); const table = op.table!;
  table.metadata['current-schema-id'] = 1;
  table.metadata.schemas = [schema, { ...schema, 'schema-id': 1, fields: [{ ...schema.fields[0], name: 'renamed' }, { id: 2, name: 'note', required: false, type: 'string' }] }];
  expect(() => validateSource(table, '20')).not.toThrow();
  (table.metadata.schemas as any[])[1].fields[0].type = 'string';
  expect(() => validateSource(table, '20')).toThrow('incompatible');
});
