// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { beforeEach, expect, it, vi } from 'vitest';
import { iceberg } from '../../access/native';
import { begin, load, protection, submit } from './operation';
import { fileRequest } from '../import/files';
import type { Selection, TableLoad } from '../types';
vi.mock('../../access/native', async original => ({ ...await original<typeof import('../../access/native')>(), iceberg: vi.fn() }));
vi.mock('../import/files', async original => ({ ...await original<typeof import('../import/files')>(), fileRequest: vi.fn() }));
const id = '9007199254740993';
const table: TableLoad = { 'metadata-location': 'metadata-1', metadata: { 'table-uuid': 'table-1', 'current-snapshot-id': '20', 'format-version': 2, 'partition-specs': [{ 'spec-id': 0, fields: [] }], 'current-schema-id': 0,
  schemas: [{ 'schema-id': 0, type: 'struct', fields: [] }], snapshots: [{ 'snapshot-id': id }, { 'snapshot-id': '20', 'manifest-list': 'list' }], refs: { main: { type: 'branch', 'snapshot-id': '20' } } } };
const target: Selection = { kind: 'snapshot', snapshot: { 'snapshot-id': id } };
beforeEach(() => { vi.clearAllMocks(); localStorage.clear(); });
it('protects current and tag references and keeps a frozen exact expiry body on recovery', async () => {
  expect(protection(table, { kind: 'snapshot', snapshot: { 'snapshot-id': '20' } }, 'expire-snapshot')).toContain('main');
  expect(protection({ ...table, metadata: { ...table.metadata, refs: { keep: { type: 'tag', 'snapshot-id': id } } } }, target, 'expire-snapshot')).toContain('keep');
  const op = begin(['ns'], 'events', table, target, 'expire-snapshot');
  vi.mocked(iceberg).mockRejectedValueOnce(new Error('connection lost'));
  await expect(submit(op)).rejects.toThrow('connection lost');
  const saved = load(['ns'], 'events')!;
  expect(saved.uncertain).toBe(true);
  expect(saved.commit).toContain(`"snapshot-ids":[${id}]`);
  expect(saved.commit).toContain('"x-crowdb-base-metadata-location":"metadata-1"');
  vi.mocked(iceberg).mockResolvedValueOnce({});
  await submit(saved);
  expect(vi.mocked(iceberg).mock.calls[1]).toEqual(vi.mocked(iceberg).mock.calls[0]);
  expect(load(['ns'], 'events')).toBeUndefined();
});
it('rejects foreign manifest layouts before writing files', async () => {
  const op = begin(['ns'], 'events', table, { kind: 'file', snapshot: table.metadata.snapshots![1], manifest: { location: 'm', content: 'Data', size: '1', partition_spec_id: '0', sequence: '1', file_counts: ['1','0','0'] },
    file: { location: 'file', size: '10', format: 'Parquet', status: 'Added', content: 'Data', records: '1' } }, 'remove-file');
  vi.mocked(iceberg).mockResolvedValueOnce({ kind: 'manifest-list', metadata_location: 'metadata-1', snapshot_id: '20', location: 'list', rows: [], next: null, rewrite_safe: false });
  await expect(submit(op)).rejects.toThrow('cannot preserve');
  expect(fileRequest).not.toHaveBeenCalled();
  expect(load(['ns'], 'events')!.commit).toBeUndefined();
});

it('writes numeric Schema IDs from Table metadata while inspection IDs stay exact strings', async () => {
  const base = { ...table, metadata: { ...table.metadata, location: 's3://iceberg/table', 'last-sequence-number': 2 } };
  const manifest = { location: 'm', content: 'Data', size: '100', partition_spec_id: '0', sequence: '1', min_sequence: '1', snapshot_id: id, file_counts: ['2','0','0'], row_counts: ['4','0','0'] };
  const file = { location: 'a.parquet', size: '10', format: 'Parquet', status: 'Added', content: 'Data', records: '2', snapshot_id: id, data_sequence: '1', file_sequence: '1' };
  const op = begin(['ns'], 'events', base, { kind: 'file', snapshot: base.metadata.snapshots![1], manifest, file }, 'remove-file');
  vi.mocked(iceberg).mockResolvedValueOnce({ kind: 'manifest-list', metadata_location: 'metadata-1', snapshot_id: '20', location: 'list', rows: [manifest], next: null, rewrite_safe: true })
    .mockResolvedValueOnce({ kind: 'manifest', metadata_location: 'metadata-1', snapshot_id: '20', location: 'm', schema: { 'schema-id': '0' }, rows: [file, { ...file, location: 'b.parquet' }], next: null, rewrite_safe: true })
    .mockResolvedValueOnce({});
  vi.mocked(fileRequest).mockResolvedValue(new Response());
  await submit(op);
  const bytes = await new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.onerror = () => reject(reader.error);
    reader.readAsText(vi.mocked(fileRequest).mock.calls[0][3] as Blob);
  });
  expect(bytes).toContain('"schema-id":0');
  expect(bytes).not.toContain('"schema-id":"0"');
  expect(op.commit).toContain('"operation":"delete"');
  expect(op.commit).toContain('"total-data-files":"1"');
});
