// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { afterEach, expect, it, vi } from 'vitest';
import { AccessError, iceberg } from '../../access/native';
import { submitImport } from './commit';
import { load, type ImportRequest } from './storage';
import { loadDraft, saveDraft, type TableDraft } from './draft';
vi.mock('../../access/native', async original => ({ ...await original<typeof import('../../access/native')>(), iceberg: vi.fn() }));
afterEach(() => { localStorage.clear(); vi.clearAllMocks(); });
const input = (): ImportRequest => ({ version: 1, namespace: ['ns'], name: 'events', create: false, identity: 'key', snapshot: '1',
  files: [], schema: { type: 'struct', 'schema-id': 0, fields: [] }, table: { 'metadata-location': 's3://table/metadata.json', metadata: {} }, commit: '{"requirements":[],"updates":[]}' });
it('definite conflicts unlock discard while unknown replies retain the identical commit', async () => {
  const op = input();
  vi.mocked(iceberg).mockRejectedValueOnce(new AccessError(409, 'main changed', 'CommitFailedException'));
  await expect(submitImport(op, [], vi.fn())).rejects.toThrow('main changed');
  expect(op.uncertain).toBe(false);
  const saved = load(op.namespace, op.name)!;
  expect(saved.commit).toBe(op.commit); expect(saved.identity).toBe('key');
  vi.mocked(iceberg).mockRejectedValueOnce(new AccessError(503, 'reply unavailable', 'CommitStateUnknownException'));
  await expect(submitImport(saved, [], vi.fn())).rejects.toThrow('reply unavailable');
  expect(saved.uncertain).toBe(true);
  expect(vi.mocked(iceberg).mock.calls[0].slice(0, 4)).toEqual(vi.mocked(iceberg).mock.calls[1].slice(0, 4));
  expect(vi.mocked(iceberg).mock.calls.map(call => call[5])).toEqual(['key', 'key']);
});
it('saves editing input and fixed Table base without credentials and guards another edit', () => {
  const draft: TableDraft = { version: 1, identity: 'edit-a', namespace: ['ns'], table: 'events', name: 'events', mode: 'parquet', schema: input().schema, files: [],
    base: { 'metadata-location': 's3://table/metadata.json', metadata: { 'table-uuid': 'fixed' }, config: { 's3.secret-access-key': 'secret', 'client.refresh-credentials-endpoint': '/v1/credentials' } } };
  saveDraft(draft);
  expect(loadDraft(['ns'], 'events')!.base).toMatchObject({ metadata: { 'table-uuid': 'fixed' }, config: { 'client.refresh-credentials-endpoint': '/v1/credentials' } });
  expect(JSON.stringify(loadDraft(['ns'], 'events'))).not.toContain('secret');
  expect(() => saveDraft({ ...draft, identity: 'edit-b' })).toThrow('Another Table edit');
});
