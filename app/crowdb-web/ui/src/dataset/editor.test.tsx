// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, fireEvent, render, screen, within } from '@testing-library/react';
import { useState } from 'react';
import { afterEach, expect, it, vi } from 'vitest';
import { Definitions, validateEditor } from './splits/Definitions';
import { defaultPolicy, type DraftSplit } from './types';
import { SelectionProvider } from '../contexts/SelectionContext';
import { DomainProvider } from '../contexts/DomainContext';
import { DatasetView } from '../views/DatasetView';
import * as api from './api';
vi.mock('./operation/storage', () => ({ listRequests: vi.fn(async () => []), readRequest: vi.fn(async () => undefined), writeRequest: vi.fn(async () => {}), removeRequest: vi.fn(async () => {}) }));
vi.mock('./api', async original => ({ ...await original<typeof import('./api')>(),
  listDatasets: vi.fn(async () => ({ entries: [], after: null })),
  capabilities: vi.fn(async () => ['dataset-management', 'composition-preparation', 'snapshot-head-update', 'dataset-identity-fence']),
  mutate: vi.fn(async () => ({})),
  page: vi.fn(async () => ({ entries: [], after: null })),
  splitHead: vi.fn(async () => ({ revision: null, generation: null })),
  head: vi.fn(async () => ({ generation: null, latest: null, stable: null })),
  control: vi.fn(async (path: string) => path.endsWith('/head') ? { latest: null, stable: null, generation: null } : { id: '1'.repeat(32) }),
}));
afterEach(() => { vi.clearAllMocks(); localStorage.clear(); });
it('creates through the parent menu and keeps resource operations out of the center toolbar', async () => {
  render(<DomainProvider><SelectionProvider><DatasetView active readonly={false} showActivity={false} /></SelectionProvider></DomainProvider>);
  await act(async () => {});
  fireEvent.contextMenu(screen.getByRole('button', { name: 'Datasets' }));
  fireEvent.click(screen.getByRole('menuitem', { name: 'Create Dataset…' }));
  expect((screen.getByLabelText('New Dataset name') as HTMLInputElement).value).toMatch(/^dataset-[0-9a-f]{8}$/);
  await act(async () => fireEvent.click(screen.getByRole('button', { name: /^Create Dataset$/ })));
  for (let index = 0; index < 3; index++) {
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Refresh Dataset' })));
    expect(screen.queryByRole('region', { name: 'Dataset actions' })).toBeNull();
    fireEvent.contextMenu(screen.getByTestId('dataset-scope'));
    expect(screen.getAllByRole('menuitem', { name: 'New Snapshot…' })).toHaveLength(1);
    expect(screen.getByRole('menuitem', { name: 'New Snapshot…' })).toBeEnabled();
    fireEvent.keyDown(document, { key: 'Escape' });
  }
  expect(api.mutate).toHaveBeenCalledTimes(1);
  expect(vi.mocked(api.mutate).mock.calls[0][1]).toMatch(/\/datasets\/dataset-[0-9a-f]{8}$/);
  fireEvent.contextMenu(screen.getByTestId('dataset-scope'));
  await act(async () => fireEvent.click(screen.getByRole('menuitem', { name: 'New Snapshot…' })));
  expect(screen.getByRole('region', { name: 'Import Snapshot' })).toBeInTheDocument();
  expect(within(screen.getByRole('region', { name: 'Dataset help' })).getByRole('link', { name: 'Help' })).toHaveAttribute('href', 'https://crowdb.dev/docs/manual/');
  expect(screen.getByLabelText('Local payload files')).toBeVisible();
  expect(screen.queryByRole('textbox', { name: 'Sample ID (UTF-8)' })).toBeNull();
  expect(screen.getByText('Advanced').parentElement).not.toHaveAttribute('open');
  expect(screen.queryByRole('dialog', { name: 'Import Snapshot' })).toBeNull();
  expect(screen.queryByRole('navigation', { name: 'Dataset tree pages' })).toBeNull();
});
it('invalid rule text blocks saving and invalidates the previously reviewed rule', () => {
  const change = vi.fn();
  const { container } = render(<Definitions splits={[{ name: 'train', origin: { kind: 'evaluated' }, definition: { kind: 'selection', predicate: { kind: 'all' } } }]} policy={defaultPolicy} change={change} />);
  fireEvent.click(screen.getByText('Advanced selection JSON'));
  fireEvent.change(screen.getByRole('textbox', { name: 'Selection rule' }), { target: { value: '{broken' } });
  expect(change).toHaveBeenCalled();
  expect(() => validateEditor(container)).toThrow('Correct the invalid editor input');
  fireEvent.change(screen.getByRole('textbox', { name: 'Selection rule' }), { target: { value: '{"kind":"all"}' } });
  expect(() => validateEditor(container)).not.toThrow();
});
it('renaming a weighted member keeps every partition definition on the same weight group', () => {
  let observed: DraftSplit[] = [];
  function Editor() {
    const [splits, setSplits] = useState<DraftSplit[]>([]);
    return <Definitions splits={splits} policy={defaultPolicy} change={values => { observed = values; setSplits(values); }} />;
  }
  render(<Editor />);
  fireEvent.click(screen.getByRole('button', { name: 'Add weighted partitions' }));
  fireEvent.change(screen.getByLabelText('Split 1 name'), { target: { value: 'training' } });
  expect(observed.map(value => value.name)).toEqual(['training', 'test']);
  for (const value of observed) {
    expect(value.definition.kind).toBe('weighted_partition');
    if (value.definition.kind === 'weighted_partition') expect(value.definition.weights.map(weight => weight.name)).toEqual(['training', 'test']);
  }
});
it('readonly exposes inspection without creation, import or lifecycle mutations', async () => {
  vi.mocked(api.listDatasets).mockResolvedValueOnce({ entries: [{ id: '1'.repeat(32), name: 'readonly-demo', namespace: 'dataset-ns', latest: null, stable: null }], after: null });
  render(<DomainProvider><SelectionProvider><DatasetView active readonly showActivity={false} /></SelectionProvider></DomainProvider>);
  await act(async () => {});
  expect(screen.queryByRole('button', { name: 'Create Dataset' })).toBeNull();
  fireEvent.click(within(screen.getByRole('navigation', { name: 'Dataset tree' })).getByRole('button', { name: 'readonly-demo' }));
  await act(async () => {});
  expect(screen.queryByRole('button', { name: 'New Snapshot' })).toBeNull();
  expect(screen.queryByRole('button', { name: 'Reclaim Snapshot' })).toBeNull();
  expect(api.mutate).not.toHaveBeenCalled();
  expect(screen.queryByText('Session activity')).toBeNull();
});
it('identical retry preserves the candidate and uses the returned head without status queries', async () => {
  const { publish, retryIdentical, useObservedHead } = await import('./operation');
  const operation: import('./operation').Operation = { version: 2, dataset: 'retry', kind: 'snapshot', resource: '3'.repeat(32), expectedHead: null,
    prepared: { snapshot: '3'.repeat(32), initial_split: null, sample_count: '1' } };
  const observed = { latest: '5'.repeat(32), stable: null, generation: '4'.repeat(64) };
  vi.mocked(api.mutate).mockRejectedValueOnce(new api.DatasetError(502, 'outcome unknown', true))
    .mockRejectedValueOnce(new api.DatasetError(409, 'head conflict', false, observed))
    .mockResolvedValueOnce({ ...observed, latest: operation.resource });
  await expect(publish(operation)).rejects.toThrow('outcome unknown');
  expect(operation.pending).toEqual({ kind: 'publish', expectedHead: null, expectedRevision: undefined });
  await expect(retryIdentical(operation)).rejects.toThrow('head conflict');
  expect(operation.conflict).toEqual(observed); expect(operation.pending).toBeUndefined();
  await useObservedHead(operation);
  expect(operation.expectedHead).toBe(observed.generation); expect(operation.resource).toBe('3'.repeat(32));
  expect((await publish(operation)).target).toEqual({ kind: 'snapshot', snapshot: operation.resource });
  const first = vi.mocked(api.mutate).mock.calls;
  expect(first[0].slice(0, 4)).toEqual(first[1].slice(0, 4));
  expect(first[2][3]).toEqual({ snapshot: operation.resource, expected_head: observed.generation });
  expect(api.control).not.toHaveBeenCalled(); expect(api.head).not.toHaveBeenCalled();
});
it('storage failure stops a preparation before its network mutation', async () => {
  const { prepare } = await import('./operation'); const { writeRequest } = await import('./operation/storage');
  vi.mocked(writeRequest).mockRejectedValueOnce(new Error('browser storage full'));
  const operation: import('./operation').Operation = { version: 2, dataset: 'bounded', kind: 'split', resource: '3'.repeat(32), snapshot: '2'.repeat(32), expectedHead: null, expectedRevision: null,
    input: { splits: [], policy: defaultPolicy } };
  await expect(prepare(operation)).rejects.toThrow('browser storage full');
  expect(api.mutate).not.toHaveBeenCalled();
});
it('reclamation confirmation keeps its exact Snapshot target across navigation', async () => {
  const { Actions } = await import('./Actions');
  const complete = vi.fn();
  const props = { action: 'reclaim' as const, readonly: false, capabilities: ['snapshot-reclaim'], complete };
  const { rerender } = render(<Actions {...props} selected={{ kind: 'snapshot', dataset: 'demo', snapshot: '1'.repeat(32) }} />);
  rerender(<Actions {...props} selected={{ kind: 'snapshot', dataset: 'demo', snapshot: '2'.repeat(32) }} />);
  await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Confirm reclamation' })));
  expect(api.mutate).toHaveBeenCalledWith('snapshot-reclaim', expect.stringContaining(`/snapshots/${'1'.repeat(32)}/reclaim`), 'POST');
  expect(complete).toHaveBeenCalledOnce();
});

it('waits for the selected Split base before enabling edits so loading cannot erase input', async () => {
  const { SplitEditor } = await import('./splits/Editor');
  let loaded!: (value: import('./types').SplitHead) => void;
  vi.mocked(api.control).mockResolvedValueOnce({ id: '1'.repeat(32) }).mockReturnValueOnce(new Promise(resolve => { loaded = resolve; }));
  render(<SplitEditor dataset="demo" snapshot={'1'.repeat(32)} close={vi.fn()} complete={vi.fn()} />);
  expect(screen.getByRole('button', { name: 'Add selection Split' })).toBeDisabled();
  expect(screen.getByRole('button', { name: 'Commit' })).toBeDisabled();
  expect(screen.queryByLabelText('Split 1 name')).toBeNull();
  await act(async () => {});
  await act(async () => loaded({ revision: null, generation: null }));
  fireEvent.click(screen.getByRole('button', { name: 'Add selection Split' }));
  fireEvent.change(screen.getByLabelText('Split 1 name'), { target: { value: 'train' } });
  await act(async () => {});
  expect(screen.getByLabelText('Split 1 name')).toHaveValue('train');
});

it('a rejected preparation is corrected under a new identity while unknown replies stay frozen', async () => {
  const { correctedDraft, prepare } = await import('./operation');
  const op: import('./operation').Operation = { version: 2, dataset: 'demo', kind: 'split', snapshot: '1'.repeat(32), resource: '2'.repeat(32), expectedHead: null,
    input: { splits: [], policy: defaultPolicy } };
  vi.mocked(api.mutate).mockRejectedValueOnce(new api.DatasetError(422, 'invalid policy'));
  await expect(prepare(op)).rejects.toThrow('invalid policy');
  expect(op.rejectedPreparation).toBe(true);
  const next = await correctedDraft(op);
  expect(next.resource).not.toBe(op.resource); expect(next.input).toEqual(op.input); expect(next.editing).toBe(true);
  op.pending = { kind: 'prepare' };
  await expect(correctedDraft(op)).rejects.toThrow('Resolve');
});
