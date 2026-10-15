// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, fireEvent, render, screen, within } from '@testing-library/react';
import { useState } from 'react';
import { afterEach, expect, it, vi } from 'vitest';
import { Definitions, validateEditor } from './splits/Definitions';
import { defaultPolicy, type DraftSplit } from './types';
import { DomainProvider } from '../contexts/DomainContext';
import { DatasetView } from '../views/DatasetView';
import * as api from './api';
vi.mock('./api', async original => ({ ...await original<typeof import('./api')>(),
  listDatasets: vi.fn(async () => ({ entries: [], after: null })),
  capabilities: vi.fn(async () => ['dataset-management', 'composition']),
  mutate: vi.fn(async () => ({})),
  page: vi.fn(async () => ({ entries: [], after: null })),
  head: vi.fn(async () => ({ generation: null, latest: null, stable: null })),
  control: vi.fn(async () => ({})),
}));
afterEach(() => { vi.clearAllMocks(); localStorage.clear(); });
it('keeps one action region across creation, capability refresh and inventory reconciliation', async () => {
  render(<DomainProvider><DatasetView active readonly={false} showActivity={false} /></DomainProvider>);
  await act(async () => {});
  fireEvent.change(screen.getByLabelText('New Dataset name'), { target: { value: 'demo' } });
  await act(async () => fireEvent.click(screen.getByRole('button', { name: /^Create Dataset$/ })));
  for (let index = 0; index < 3; index++) {
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Refresh Dataset' })));
    expect(screen.getAllByRole('region', { name: 'Dataset actions' })).toHaveLength(1);
    expect(screen.getAllByRole('button', { name: 'Import / New Snapshot' })).toHaveLength(1);
  }
  expect(api.mutate).toHaveBeenCalledTimes(1);
});
it('invalid rule text blocks saving and invalidates the previously reviewed rule', () => {
  const change = vi.fn();
  const { container } = render(<Definitions splits={[{ name: 'train', origin: { kind: 'evaluated' }, definition: { kind: 'selection', predicate: { kind: 'all' } } }]} policy={defaultPolicy} change={change} />);
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
  render(<DomainProvider><DatasetView active readonly showActivity={false} /></DomainProvider>);
  await act(async () => {});
  expect(screen.queryByRole('button', { name: 'Create Dataset' })).toBeNull();
  fireEvent.click(within(screen.getByRole('navigation', { name: 'Dataset tree' })).getByRole('button', { name: 'readonly-demo' }));
  await act(async () => {});
  expect(screen.queryByRole('button', { name: 'Import / New Snapshot' })).toBeNull();
  expect(screen.queryByRole('button', { name: 'Reclaim Snapshot' })).toBeNull();
  expect(api.mutate).not.toHaveBeenCalled();
  expect(screen.queryByText('Session activity')).toBeNull();
});
