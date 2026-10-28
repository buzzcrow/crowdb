// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { expect, it, vi } from 'vitest';
import * as api from '../api';
import { independentRoot, openDraft, patchSample, validateDraft } from './draft';
vi.mock('../operation/storage', () => ({ readRequest: vi.fn(), writeRequest: vi.fn(), removeRequest: vi.fn() }));
vi.mock('../api', async original => ({ ...await original<typeof import('../api')>(), capabilities: vi.fn(async () => ['dataset-identity-fence']),
  control: vi.fn(async (path: string) => path.endsWith('/head') ? { latest: '2'.repeat(32), stable: null, generation: '3'.repeat(64) } : { id: '1'.repeat(32) }),
  summary: vi.fn(async () => ({ schema_version: 2 })), schemaPage: vi.fn(async () => ({ entries: [{ name: 'content', required: true }], after: null })),
  splitHead: vi.fn(async () => ({ revision: '4'.repeat(32) })), revision: vi.fn(async () => ({ policy: { overlap: 'allow_overlap', coverage: 'require_complete' }, splits: [{ name: 'train', definition: { kind: 'selection', predicate: { kind: 'all' } } }] })),
}));
it('freezes the default Latest, Dataset identity, head, schema and Split policy at opening', async () => {
  const draft = await openDraft('training');
  expect(draft.source?.splits).toEqual([]);
  expect(draft.datasetId).toBe('1'.repeat(32)); expect(draft.expectedHead).toBe('3'.repeat(64));
  expect(draft.source?.parent).toBe('2'.repeat(32));
  expect(draft.parentSplits?.policy).toEqual({ overlap: 'allow_overlap', coverage: 'require_complete' });
  expect(draft.parentSplits?.splits[0].origin).toEqual({ kind: 'inherited', source: { snapshot: '2'.repeat(32), revision: '4'.repeat(32), name: 'train' } });
  expect(vi.mocked(api.control).mock.calls[1][4]).toBe(draft.datasetId);
});
it('accumulates independent fields on the same Sample and keeps clear distinct from removal', async () => {
  const draft = await openDraft('training');
  const id = btoa('sample');
  let mapping = patchSample(draft.mapping!, { kind: 'upsert', id, fields: [{ name: 'label', value: { kind: 'inline', data: btoa('keep') } }] });
  mapping = patchSample(mapping, { kind: 'upsert', id, fields: [{ name: 'content', value: { kind: 'file', file: 'new.txt' } }] });
  expect(mapping.samples[0]).toMatchObject({ kind: 'upsert', fields: [{ name: 'label' }, { name: 'content' }] });
  mapping = patchSample(mapping, { kind: 'upsert', id, fields: [{ name: 'content', value: { kind: 'tombstone' } }] });
  expect(mapping.samples[0]).toMatchObject({ kind: 'upsert', fields: [{ name: 'label' }, { name: 'content', value: { kind: 'tombstone' } }] });
  expect(patchSample(mapping, { kind: 'remove', id }).samples).toEqual([{ kind: 'remove', id }]);
});
it('keeps Sample order while merging edits into an existing row', () => {
  const first = btoa('first'); const second = btoa('second');
  const mapping = { version: 1 as const, schema: { version: 1, fields: [{ name: 'content', required: false }] }, samples: [
    { kind: 'upsert' as const, id: first, fields: [] }, { kind: 'upsert' as const, id: second, fields: [] },
  ] };
  const next = patchSample(mapping, { kind: 'upsert', id: first, fields: [{ name: 'content', value: { kind: 'inline', data: btoa('edited') } }] });
  expect(next.samples.map(sample => sample.id)).toEqual([first, second]);
  expect(next.samples[0]).toMatchObject({ id: first, fields: [{ name: 'content' }] });
});
it('rejects removal or downgrade of required parent fields and empty independent Snapshots', async () => {
  const draft = await openDraft('training');
  draft.mapping!.schema = { version: 2, fields: [{ name: 'content', required: false }] };
  expect(() => validateDraft(draft)).toThrow('must remain required');
  draft.source!.parent = null;
  expect(() => validateDraft(draft)).toThrow('at least one Sample');
});
it('restores the frozen Split source when independent root is turned off', async () => {
  const draft = await openDraft('training', undefined, { snapshot: '2'.repeat(32), revision: '4'.repeat(32), name: 'train' });
  const root = independentRoot(draft, true);
  expect(root.source?.parent).toBeNull(); expect(root.source?.splits).toEqual([]);
  expect(independentRoot(root, false).source).toEqual(draft.source);
});
